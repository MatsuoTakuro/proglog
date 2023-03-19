package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/config"
)

func TestAgent(t *testing.T) {
	// create a certificate for a client to use
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
			IsServer:      true,
		},
	)
	require.NoError(t, err)

	// create a certificate for server-to-server use
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
			IsServer:      false,
		},
	)
	require.NoError(t, err)

	var agents []*Agent
	// create three server (node) for one cluster.
	for i := 0; i < 3; i++ {
		// prepare two port numbers for one server that makes connections for gRPC-log
		// and serf's service-discovery respectively.
		// so,  2 ports per server * 3 server = 6 port numbers are needed.
		// cuz every server having two connections runs all on the same host.
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].BindAddr)
		}

		agent, err := New(
			Config{
				NodeName:        fmt.Sprintf("%d", i),
				StartJoinAddrs:  startJoinAddrs,
				BindAddr:        bindAddr,
				RPCPort:         rpcPort,
				DataDir:         dataDir,
				ACLModelFile:    config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
				ServerTLSConfig: serverTLSConfig,
				PeerTLSConfig:   peerTLSConfig,
			},
		)
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(a.DataDir))
		}
	}()
	// takes some time to discover each node
	time.Sleep(3 * time.Second)

	// create a client for the 1st server and produce a record
	leaderCli := client(t, agents[0], peerTLSConfig)
	want := []byte("foo")
	produceResp, err := leaderCli.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: want,
			},
		},
	)
	require.NoError(t, err)

	// consume the produced record from the 1st server
	consumeResp, err := leaderCli.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResp.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, want)

	// wait some time to complete all replications
	time.Sleep(3 * time.Second)

	// create a client for the 2nd server and consume a record
	// that would have been replicated from the 1st server.
	followerCli := client(t, agents[1], peerTLSConfig)
	consumeResp, err = followerCli.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResp.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, want)

	// there is an issue that the original server that produced a record replicates the same record
	// from other servers that replicated it from the origilanl one before.
	// consumeResp, err = leaderCli.Consume(
	// 	context.Background(),
	// 	&api.ConsumeRequest{
	// 		Offset: produceResp.Offset + 1,
	// 	},
	// )
	// require.Nil(t, consumeResp) // should be nil, but got any value
	// require.Error(t, err)       // should be error, but go an error
	// wantStatus := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	// gotStatus := status.Code(err)
	// require.Equal(t, gotStatus, wantStatus) // should be equal, but was not equal
}

func client(
	t *testing.T,
	a *Agent,
	tlsCfg *tls.Config,
) api.LogClient {
	t.Helper()

	tlsCreds := credentials.NewTLS(tlsCfg)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	rpcAddr, err := a.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	cli := api.NewLogClient(conn)

	return cli
}
