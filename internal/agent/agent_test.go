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
	"google.golang.org/grpc/status"

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
	for id := 0; id < 3; id++ {
		// prepare two port numbers for one server that makes connections for gRPC-log
		// and serf's service-discovery respectively.
		// so,  2 ports per server * 3 server = 6 port numbers are needed.
		// cuz every server having two connections runs all on the same host.
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		// NOTE: switch between MkdirTemp to Mkdir if you want to leave logs in your loca dataDir.
		// dataDir, err := os.MkdirTemp("", "agent-test-log")
		dataDir := fmt.Sprintf("agent-test-log-%d", id)
		err = os.Mkdir(dataDir, 0755)

		require.NoError(t, err)

		var startJoinAddrs []string
		if id != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].BindAddr)
		}

		agent, err := New(
			Config{
				NodeName:        fmt.Sprintf("%d", id),
				StartJoinAddrs:  startJoinAddrs,
				BindAddr:        bindAddr,
				RPCPort:         rpcPort,
				DataDir:         dataDir,
				ACLModelFile:    config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
				ServerTLSConfig: serverTLSConfig,
				PeerTLSConfig:   peerTLSConfig,
				Bootstrap:       id == 0,
			},
		)
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {

		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)
			// NOTE: comment out this line if you want to leave logs in your local DataDir.
			// require.NoError(t, os.RemoveAll(a.DataDir))
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

	// test that the leader node do not duplicate a record from other nodes.
	// try to consume a record that should not exist in the leader node.
	consumeResp, err = leaderCli.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResp.Offset + 1,
		},
	)
	require.Nil(t, consumeResp)
	require.Error(t, err)
	wantStatus := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	gotStatus := status.Code(err)
	require.Equal(t, gotStatus, wantStatus)
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
