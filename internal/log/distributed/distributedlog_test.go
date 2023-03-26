package distributed

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	var dlogs []*DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	// create nodes and form a cluster of them.
	for id := 0; id < nodeCount; id++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dataDir)
		}(dataDir)

		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[id]),
		)
		require.NoError(t, err)

		cfg := log.Config{}
		cfg.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		cfg.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", id))
		cfg.Raft.HeartbeatTimeout = 50 * time.Millisecond
		cfg.Raft.ElectionTimeout = 50 * time.Millisecond
		cfg.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		cfg.Raft.CommitTimeout = 5 * time.Millisecond

		// if it is an initial node, allow only it to bootstrap.
		if id == 0 {
			cfg.Raft.Bootstrap = true
		}

		dl, err := NewDistributedLog(dataDir, cfg)
		require.NoError(t, err)

		if id != 0 {
			// if it not is an initial node, add it to the cluster.
			err = dlogs[0].Join(fmt.Sprintf("%d", id), ln.Addr().String())
			require.NoError(t, err)
		} else {
			// if it is an initial node (the first possible leader), wait for a leader to be elected.
			err = dl.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		dlogs = append(dlogs, dl)
	}

	rcds := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, rcd := range rcds {
		// add a record to the leader node.
		off, err := dlogs[0].Append(rcd)
		require.NoError(t, err)

		// make sure other nodes duplicate the record from the leader node
		require.Eventually(t, func() bool {
			for id := 0; id < nodeCount; id++ {
				got, err := dlogs[id].Read(off)
				if err != nil {
					return false
				}
				rcd.Offset = off
				if !reflect.DeepEqual(got.Value, rcd.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// make the seconde node leave the cluster.
	leaveIDStr := "1"
	err := dlogs[0].Leave(leaveIDStr)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// add a record to the leader node.
	off, err := dlogs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	leaveID, err := strconv.ParseInt(leaveIDStr, 10, 8)
	require.NoError(t, err)
	got, err := dlogs[leaveID].Read(off)
	// make sure the record is not duplicated by and to the node that left the cluster.
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, got)

	notLeaveID := 2
	got, err = dlogs[notLeaveID].Read(off)
	// make sure the record is duplicated by and to the node that is still in the cluster.
	require.NoError(t, err)
	require.Equal(t, []byte("third"), got.Value)
	require.Equal(t, off, got.Offset)

}
