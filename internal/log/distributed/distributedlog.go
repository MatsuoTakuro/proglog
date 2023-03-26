package distributed

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/MatsuoTakuro/proglog/internal/server"
	"github.com/MatsuoTakuro/proglog/internal/server/discovery"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

// DistributedLog has two logs; log and raftLog.
// it implements server.CommitLog and discovery.Handler interfaces.
type DistributedLog struct {
	cfg          log.Config
	log          *log.Log
	raftLogStore *raftLogStore
	raft         *raft.Raft
}

var _ server.CommitLog = (*DistributedLog)(nil)
var _ discovery.Handler = (*DistributedLog)(nil)

func NewDistributedLog(dataDir string, cfg log.Config) (
	*DistributedLog,
	error,
) {
	dl := &DistributedLog{
		cfg: cfg,
	}

	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return dl, nil
}

func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	dl.log, err = log.NewLog(logDir, dl.cfg)

	return err
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	var err error

	fsm := &fsm{log: dl.log}

	raftLogDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(raftLogDir, 0755); err != nil {
		return err
	}

	cfg := dl.cfg
	cfg.Segment.InitialOffset = 1
	dl.raftLogStore, err = newRaftLogStore(raftLogDir, cfg)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		dl.cfg.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	conf := raft.DefaultConfig()
	conf.LocalID = dl.cfg.Raft.LocalID
	if dl.cfg.Raft.HeartbeatTimeout != 0 {
		conf.HeartbeatTimeout = dl.cfg.Raft.HeartbeatTimeout
	}
	if dl.cfg.Raft.ElectionTimeout != 0 {
		conf.ElectionTimeout = dl.cfg.Raft.ElectionTimeout
	}
	if dl.cfg.Raft.LeaderLeaseTimeout != 0 {
		conf.LeaderLeaseTimeout = dl.cfg.Raft.LeaderLeaseTimeout
	}
	if dl.cfg.Raft.CommitTimeout != 0 {
		conf.CommitTimeout = dl.cfg.Raft.CommitTimeout
	}

	dl.raft, err = raft.NewRaft(
		conf,
		fsm,
		dl.raftLogStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		dl.raftLogStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	if dl.cfg.Raft.Bootstrap && !hasState {
		raftCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      conf.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = dl.raft.BootstrapCluster(raftCfg).Error()
	}

	return err
}

func (dl *DistributedLog) Append(record *api.Record) (uint64, error) {
	resp, err := dl.apply(
		log.AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, nil
	}

	return resp.(*api.ProduceResponse).Offset, nil
}

func (dl *DistributedLog) apply(reqType log.RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	// write a reqest type in buf so that it can tell which request type by buf[0].
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := dl.raft.Apply(buf.Bytes(), timeout) // acutually, raft delegates this business logic to fsm.
	if future.Error() != nil {
		return nil, future.Error()
	}

	resp := future.Response()
	if err, ok := resp.(error); ok {
		return nil, err
	}

	return resp, nil // TODO: check why it does not return nil instead of err.
}

func (dl *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return dl.log.Read(offset)
}

// Join adds a new server to its cluster.
func (dl *DistributedLog) Join(id, addr string) error {
	cfgFuture := dl.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// this server has already join its cluster
				return nil
			}
			// remove exisiting servers
			removeFuture := dl.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := dl.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

func (dl *DistributedLog) Leave(id string) error {
	removeFuture := dl.raft.RemoveServer(raft.ServerID(id), 0, 0)

	return removeFuture.Error()
}

func (dl *DistributedLog) WaitForLeader(timeout time.Duration) error {
	// wait for timeout
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		// timeout comes
		case <-timeoutChan:
			return fmt.Errorf("timed out")
		// every ticker's channel comes
		case <-ticker.C:
			// if a new leader is seleced
			if laddr := dl.raft.Leader(); laddr != "" {
				return nil
			}
		}
	}
}

func (dl *DistributedLog) Close() error {
	future := dl.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	return dl.log.Close()
}
