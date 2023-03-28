package loadbalance

import (
	"net"
	"testing"

	api "github.com/MatsuoTakuro/proglog/api/v1"

	"github.com/MatsuoTakuro/proglog/internal/config"
	"github.com/MatsuoTakuro/proglog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		IsServer:      true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	srvCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServersMock{},
	}, grpc.Creds(srvCreds))
	require.NoError(t, err)

	go srv.Serve(l)

	conn := &clinetConnMock{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		IsServer:      false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	cliCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: cliCreds,
	}
	r := &Resolver{}
	require.NoError(t, err)
	_, err = r.Build(
		resolver.Target{
			// TODO: use url.URL instead of Endpoint
			// URL: url.URL,
			Endpoint: l.Addr().String(),
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr:       "127.0.0.1:9001",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr:       "127.0.0.1:9002",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

type getServersMock struct{}

func (s *getServersMock) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "0",
			RpcAddr:  "127.0.0.1:9001",
			IsLeader: true,
		},
		{
			Id:      "1",
			RpcAddr: "127.0.0.1:9002",
		},
	}, nil
}

type clinetConnMock struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clinetConnMock) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clinetConnMock) ReportError(error) {}

func (c *clinetConnMock) NewAddress(addresses []resolver.Address) {}

func (c *clinetConnMock) NewServiceConfig(serviceConfig string) {}

func (c *clinetConnMock) ParseServiceConfig(
	serviceConfigJSON string,
) *serviceconfig.ParseResult {
	return nil
}
