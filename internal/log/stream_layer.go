package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type ConnectionType uint8

const (
	RaftRPC ConnectionType = iota + 1
)

// StreamLayer provides an abstruction of low layer stream to connect with other raft servers.
// it implements raft.StreamLayer interface.
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config // for connections over TLS to receive messages
	peerTLSConfig   *tls.Config // for connections over TLS to send messages
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig *tls.Config,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// Dial makes a connection to send message to other raft servers.
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// write a reqest type in buf so that it can tell which request type by buf[0].
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

// Accept makes a connection to receive messages from other raft servers.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
