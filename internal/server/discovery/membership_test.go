package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	// add three members (nodes) to a cluster
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	// assert this by each 250 mili seconds in three seconds.
	require.Eventually(t, func() bool {
		return len(handler.joinEvents) == 2 &&
			len(m[0].Members()) == 3 &&
			len(handler.leaveEvents) == 0
	}, 3*time.Second, 250*time.Millisecond)

	// the third (last) member leaves the cluster.
	require.NoError(t, m[2].Leave())

	// assert this by each 250 mili seconds in three seconds.
	require.Eventually(t, func() bool {
		return len(handler.joinEvents) == 2 &&
			// a return value of Members() remains 3, not 2.
			len(m[0].Members()) == 3 &&
			m[0].Members()[2].Status == serf.StatusLeft &&
			len(handler.leaveEvents) == 1
	}, 3*time.Second, 250*time.Millisecond)

	// assert whether "2" is the id of the last member to leave the cluster.
	require.Equal(t, "2", <-handler.leaveEvents)
}

// handler implements a Handler interface.
type handler struct {
	joinEvents  chan map[string]string
	leaveEvents chan string
}

var _ Handler = (*handler)(nil)

func (h *handler) Join(id, addr string) error {
	if h.joinEvents != nil {
		// if it is not initializing a cluster (= the first node's joining), add a join event to a counter
		h.joinEvents <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}

	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaveEvents != nil {
		h.leaveEvents <- id
	}

	return nil
}

func setupMember(t *testing.T, members []*Membership) (
	[]*Membership, *handler,
) {
	id := len(members)

	ports := dynaport.Get(1) // get ports that are free to use.
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_name": addr,
	}

	cfg := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &handler{}
	if len(members) == 0 {
		h.joinEvents = make(chan map[string]string, 3)
		h.leaveEvents = make(chan string, 3)
	} else {
		// add an address of the first member that joined to StartJoinAddrs.
		// this member will connect to this address when it starts to join.
		cfg.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}

	m, err := New(h, cfg)
	require.NoError(t, err)

	members = append(members, m)

	return members, h
}
