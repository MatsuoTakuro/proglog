package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/MatsuoTakuro/proglog/internal/auth"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/MatsuoTakuro/proglog/internal/log/distributed"
	"github.com/MatsuoTakuro/proglog/internal/server"
	"github.com/MatsuoTakuro/proglog/internal/server/discovery"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
)

// Agent runs on every service instance and has all settings for all componets (replicator, membership, log, and server)
type Agent struct {
	Config

	mux        cmux.CMux
	log        *distributed.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

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
	Bootstrap       bool
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
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()

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

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		a.Config.RPCPort,
	)

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupLog() error {
	// prepare a matcher for raft connection
	raftLn := a.mux.Match(func(r io.Reader) bool {
		b := make([]byte, 1)
		// read the fist byte in the socket of every connection
		if _, err := r.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})

	logCfg := log.Config{}
	logCfg.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logCfg.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logCfg.Raft.Bootstrap = a.Config.Bootstrap
	logCfg.Raft.CommitTimeout = 1000 * time.Millisecond

	var err error
	a.log, err = distributed.NewDistributedLog(
		a.Config.DataDir,
		logCfg,
	)
	if err != nil {
		return err
	}

	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
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

	// if a connection does not match a matcher for raft,
	// assign a listener for grpc (grpcLn) to it (and otherwise raftLn).
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
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

	a.membership, err = discovery.New(a.log,
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

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}

	return nil
}
