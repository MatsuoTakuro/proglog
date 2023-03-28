package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/MatsuoTakuro/proglog/api/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const Name = "proglog"

// Resolver calls the GetServers endpoint and pass info about servers to Picker.
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn        // user's client connection that is passed to resolver.
	resolverConn  *grpc.ClientConn           // resolver's client connection with servers in order to find one that can be updated.
	serviceConfig *serviceconfig.ParseResult // client's configuration defined for how calls to services are to be balanced.
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)

func init() {
	// register a resolver to grpc.
	resolver.Register(&Resolver{})
}

// Build returns a resolver that has a client connection to able to call the GetServers endpoint.
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")

	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	// TODO: use URL.Path instead of Endpoint
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

// Schema returns a schema identifier by which grpc can tell a resolver from others.
// The grpc parse a schema from given target address and find one that match the identifier.
func (r *Resolver) Scheme() string {
	return Name
}

// ResolveNow resolves a target, discover servers in it, and update a state of a client connection for servers.
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// get a cluster of servers
	cli := api.NewLogClient(r.resolverConn)
	ctx := context.Background()
	resp, err := cli.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}

	var addrs []resolver.Address
	for _, srv := range resp.Servers {
		addrs = append(addrs,
			resolver.Address{
				Addr: srv.RpcAddr,
				Attributes: attributes.New(
					"is_leader", srv.IsLeader,
				),
			},
		)
	}

	// update a state of a client connection with service configuration
	r.clientConn.UpdateState(
		resolver.State{
			Addresses:     addrs,
			ServiceConfig: r.serviceConfig,
		},
	)
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
