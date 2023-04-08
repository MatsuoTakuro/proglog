package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}

// Picker selects a server from those discovered by the Resolver to handle each PRC.
// By doing this, it distributes the load of PRC processing across servers.
type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64 // NOTE: it is initialized as zero value (0).
}

var _ base.PickerBuilder = (*Picker)(nil)
var _ balancer.Picker = (*Picker)(nil)

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers

	return p
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result balancer.PickResult
	if strings.Contains(info.FullMethodName, "Produce") || len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}

	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	// add 1 to the current number in a thread-safe way and return the updated one
	cur := atomic.AddUint64(&p.current, uint64(1))
	// calculate the index of the next follower in the round-robin sequence
	len := uint64(len(p.followers))
	idx := int(cur % len)

	return p.followers[idx]
}
