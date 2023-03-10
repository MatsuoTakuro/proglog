package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestGRPCServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			cli, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, cli, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	cli api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// port 0 can automatically allocate a free port
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial(l.Addr().String(), opts...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "grpc-server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}

	srv, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	// use goroutine for *grpc.Server.Serve cuz it is a blocking call
	go func() {
		srv.Serve(l)
	}()

	cli = api.NewLogClient(conn)

	return cli, cfg, func() {
		conn.Close()
		srv.Stop()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, cli api.LogClient, cfg *Config) {
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

func testProduceConsumeStream(t *testing.T, cli api.LogClient, cfg *Config) {
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

	// TODO: whats just {} (curly brackets) means
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

func testConsumePastBoundary(t *testing.T, cli api.LogClient, cfg *Config) {
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
