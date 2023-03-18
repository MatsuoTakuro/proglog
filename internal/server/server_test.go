package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/auth"
	"github.com/MatsuoTakuro/proglog/internal/config"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootCli api.LogClient,
		nobodyCli api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootCli, nobodyCli, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootCli, nobodyCli, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	// setupTest func's return values
	rootCli api.LogClient,
	nobodyCli api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// you need to set a host among ones in server csr's settings
	// port 0 can automatically allocate a free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// prepare a newClient func that creates a client with connection over TLS
	newClient := func(certPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: certPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			IsServer: false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		// opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)

		// finally, the client is set up!
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	// create a root client, using newClient func
	var rootConn *grpc.ClientConn
	rootConn, rootCli, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	// create a nobody client, using newClient func
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyCli, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// build a server with connection over TLS
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		IsServer:      true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "grpc-server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	// finally, the server is built up!
	srv, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// start the server by using goroutine cuz it is a blocking call
	go func() {
		srv.Serve(l)
	}()

	return rootCli, nobodyCli, cfg, func() {
		rootConn.Close()
		nobodyConn.Close()
		srv.Stop()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, cli, _ api.LogClient, cfg *Config) { // use rootClient
	ctx := context.Background()

	want := &api.Record{
		Value:  []byte("hello world"),
		Offset: 0,
	}

	produce, err := cli.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	want.Offset = produce.Offset

	consume, err := cli.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, cli, _ api.LogClient, cfg *Config) { // use rootClient
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// whats just {} (curly brackets) means?
	// -> that answer is whatever happens in that scope, stays in that scope. Variable declarations, calculations, etc.
	{
		strmCli, err := cli.ProduceStream(ctx)
		require.NoError(t, err)

		for _, rcd := range records {
			err = strmCli.Send(&api.ProduceRequest{
				Record: rcd,
			})
			require.NoError(t, err)

			resp, err := strmCli.Recv()
			require.NoError(t, err)

			if resp.Offset != rcd.Offset {
				t.Fatalf("got offset: %d, want: %d",
					resp.Offset,
					rcd.Offset,
				)
			}
		}
	}

	{
		strmCli, err := cli.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for _, rcd := range records {
			resp, err := strmCli.Recv()
			require.NoError(t, err)
			require.Equal(t, resp.Record, &api.Record{
				Value:  rcd.Value,
				Offset: rcd.Offset,
			})
		}
	}
}

func testConsumePastBoundary(t *testing.T, cli, _ api.LogClient, cfg *Config) { // use rootClient
	ctx := context.Background()

	produce, err := cli.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value:  []byte("hello world"),
				Offset: 0,
			},
		},
	)
	require.NoError(t, err)

	consume, err := cli.Consume(
		ctx,
		&api.ConsumeRequest{
			// set off which will be outside the log's range
			Offset: produce.Offset + 1,
		},
	)
	if consume != nil {
		t.Fatal("consume resp is not nil")
	}

	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	got := status.Code(err)
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testUnauthorized(t *testing.T, _, cli api.LogClient, cfg *Config) { // use nobodyClient

	wantCode := codes.PermissionDenied
	ctx := context.Background()

	// an invalid client makes a produce request
	produce, err := cli.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode := status.Code(err)
	if gotCode != wantCode {
		t.Fatalf("got code: %v, but want: %d", gotCode, wantCode)
	}

	// an invalid client makes a consume request
	consume, err := cli.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode = status.Code(err)
	if gotCode != wantCode {
		t.Fatalf("got code: %v, but want: %d", gotCode, wantCode)
	}
}
