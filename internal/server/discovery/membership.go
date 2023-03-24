package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership represents a serf's decentralized cluster membership for every member.
type Membership struct {
	Config                  // a settings of serf cluster membership
	handler Handler         // a process to call once for receiving events always
	serf    *serf.Serf      // an embedded struct for using serf libraries
	events  chan serf.Event // events always received about that nodes (members) join or leave a cluster membership
	logger  *zap.Logger
}

type Config struct {
	NodeName       string            // an unique node's (member's) name in serf's entire cluster
	BindAddr       string            // an address(ip and port) used for gssip protocol
	Tags           map[string]string // tags(for example, a rpc address) shared with other nodes
	StartJoinAddrs []string          // other existing nodes' addresses connected by itself if its new node joins an exsiting cluster membership
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

func New(handler Handler, cfg Config) (*Membership, error) {
	m := &Membership{
		Config:  cfg,
		handler: handler,
		serf:    &serf.Serf{},
		events:  make(chan serf.Event),
		logger:  zap.L().Named("membership"),
	}

	if err := m.setupSerf(); err != nil {
		return nil, err
	}

	return m, nil
}

// setupSerf sets a settings of a cluster membership and creates an serf instance based on it.
func (m *Membership) setupSerf() (err error) {
	// resolve a literal ip address to an address of TCP end point.
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	cfg := serf.DefaultConfig()
	cfg.Init()
	cfg.MemberlistConfig.BindAddr = addr.IP.String()
	cfg.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	cfg.EventCh = m.events
	cfg.Tags = m.Tags
	cfg.NodeName = m.NodeName

	m.serf, err = serf.Create(cfg)
	if err != nil {
		return err
	}

	// start a events handler in a goroutine
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			// use a loop cuz updates from multiple	members can be a single event.
			for _, member := range e.(serf.MemberEvent).Members {
				// do nothing if the member of the received event is itself.
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				// do nothing if the member of the received event is itself.
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether a specified member's name is its own name or not.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns members in the cluster at the time of the call.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave leaves itself out of the cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	// set output of logs as error level
	logger := m.logger.Error
	if err == raft.ErrNotLeader {
		// set output of logs as debug level
		// to avoid logging this error as error level
		logger = m.logger.Debug
	}

	logger(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
