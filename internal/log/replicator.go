package log

import (
	"context"
	"sync"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/server/discovery"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Replicator reads all records from a new server to join and save them to a local server.
// it also implements discovery.Handler interface.
type Replicator struct {
	LocalServer api.LogClient     // a gRPC client to connect to other servers
	DialOpts    []grpc.DialOption // options for a gRPC client

	logger *zap.Logger

	mu                   sync.Mutex
	SrvsUnderReplication map[string]chan struct{} // a list of servers that are gonna to be or being replicated already. it is also used for stopping replications from servers that fails or leaves a cluster
	closed               bool
	close                chan struct{}
}

var _ discovery.Handler = (*Replicator)(nil)

func (r *Replicator) Join(srvName, srvAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.SrvsUnderReplication[srvName]; ok {
		// skip replication for the server cuz it is gonna to be or being replicated already.
		return nil
	}
	r.SrvsUnderReplication[srvName] = make(chan struct{})

	// replicate records of the target server
	go r.replicate(srvAddr, r.SrvsUnderReplication[srvName])

	return nil
}

func (r *Replicator) Leave(srvName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.SrvsUnderReplication[srvName]; !ok {
		return nil
	}
	close(r.SrvsUnderReplication[srvName]) // notify a goroutine replicating the server to stop that.
	delete(r.SrvsUnderReplication, srvName)

	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	close(r.close) // notify all goroutines replicating some servers to stop all that.

	return nil
}

func (r *Replicator) replicate(srvAddr string, targetSrvLeave chan struct{}) {
	cc, err := grpc.Dial(srvAddr, r.DialOpts...)
	if err != nil {
		r.logError(err, "failed to dial", srvAddr)
		return
	}
	defer cc.Close()

	cli := api.NewLogClient(cc)

	ctx := context.Background()
	// create a streaming client to fetch all records from the target server.
	streamCli, err := cli.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", srvAddr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			// actually receive a record by streaming
			recv, err := streamCli.Recv()
			if err != nil {
				r.logError(err, "failed to receive", srvAddr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		// if it (the replicator) closes the replication for the server.
		case <-r.close:
			return
		// if the target server fails or leaves the cluster
		case <-targetSrvLeave:
			return

		// if it (the replicator) receives a record by streaming above
		case record := <-records:
			// save every record in a local server
			_, err := r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", srvAddr)
				return
			}
		}
	}
}

func (r *Replicator) init() {
	// take advantage of zero values for each type in golang as our book tells you.
	// you can also refer to https://www.scaler.com/topics/golang/golang-zero-values/ .
	// each zero value here means that each process has not yet been initialized.
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.SrvsUnderReplication == nil {
		r.SrvsUnderReplication = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
