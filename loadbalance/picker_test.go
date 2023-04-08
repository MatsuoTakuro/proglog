package loadbalance

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestPickerNoSubConnAvaliable(t *testing.T) {
	p := &Picker{}
	for _, method := range []string{
		"/log.vX.Log/Produce",
		"/log.vX.Log/Consume",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}
		result, err := p.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

// let's say 0th sub conn is the leader.
var leaderIdx = 0

func TestPickerProducesToLeader(t *testing.T) {
	p, sc := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	}
	for i := 0; i < 5; i++ {
		result, err := p.Pick(info)
		require.NoError(t, err)
		require.Equal(t, sc[leaderIdx], result.SubConn)
	}
}

func TestPickerConsumesFromFollowers(t *testing.T) {
	p, scs := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	}
	for i := 0; i < 5; i++ {
		result, err := p.Pick(info)
		require.NoError(t, err)
		require.Equal(t, scs[i%2+1], result.SubConn)
	}
}

// subConn implements balancer.SubConn.
type subConn struct {
	addrs []resolver.Address
}

var _ balancer.SubConn = (*subConn)(nil)

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}

func (s *subConn) GetOrBuildProducer(balancer.ProducerBuilder) (
	p balancer.Producer, close func(),
) {
	return nil, nil
}

func setupTest() (*Picker, []*subConn) {
	// set up sub conns.
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == leaderIdx),
		}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}

	// set up a picker.
	p := &Picker{}
	p.Build(buildInfo)

	return p, subConns
}
