package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/auth"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/MatsuoTakuro/proglog/internal/server"
	"github.com/MatsuoTakuro/proglog/internal/server/discovery"
)

// Agent runs on every service instance and has all settings for all componets (replicator, membership, log, and server)
type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	isShutdown   bool
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config // a tls config for runing a peer local server to save replications of records from other servers
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(cfg Config) (*Agent, error) {
	a := &Agent{
		Config: cfg,
	}
	setup := []func() error{
		// set up each component in the reverse order of shutdown.
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.DataDir,
		log.Config{},
	)

	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.ACLModelFile,
		a.ACLPolicyFile,
	)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(
		serverConfig,
		opts...,
	)
	if err != nil {
		return err
	}

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	var opts []grpc.DialOption
	if a.PeerTLSConfig != nil {
		opts = append(opts,
			grpc.WithTransportCredentials(
				credentials.NewTLS(a.PeerTLSConfig),
			),
		)
	}

	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	cli := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		LocalServer: cli,
		DialOpts:    opts,
	}
	a.membership, err = discovery.New(
		a.replicator,
		discovery.Config{
			NodeName: a.NodeName,
			BindAddr: a.BindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.StartJoinAddrs,
		},
	)

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.isShutdown {
		return nil
	}
	a.isShutdown = true

	allShutdowns := []func() error{
		// shut down each component in the reverse order of setup.
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range allShutdowns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
