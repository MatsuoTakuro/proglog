package distributed

/*
	Raft consists of fsm, log store, stable store, snapshot, and transport(stream layer).
	here describes implementations for the three elements of them; fsm, snapshot, log store, and stream layer.
*/

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type RequestType uint8

const (
	AppendRequestType RequestType = iota
)

type ConnectionType uint8

const (
	RaftRPC ConnectionType = iota + 1
)

// fsm executes or applies commands given from Raft.
// 'fsm' stands for finite state machine.
// it implements raft.FSM interface.
type fsm struct {
	log *log.Log
}

var _ raft.FSM = (*fsm)(nil)

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, log.RecordSizeWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		// read each record bytes from a snapshot with snapshot.reader
		_, err := io.ReadFull(r, b)
		// when it goes to end of last file for snapshot, get out of this loop
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(log.Enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			// reset an exisiting log first
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		// write the record of snapshot in it own log
		if _, err = f.log.Append(record); err != nil {
			return err
		}

		// get buf empty for going to next loop
		buf.Reset()
	}

	return nil
}

// snapshot represents a point-in-time snapshot of fsm.
// it implements raft.FSMSnapshot interface.
type snapshot struct {
	reader io.Reader
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// raftLogStore represents a log store for raft.
// it implements raft.LogStore interface.
type raftLogStore struct {
	*log.Log
}

var _ raft.LogStore = (*raftLogStore)(nil)

func newRaftLogStore(dir string, cfg log.Config) (*raftLogStore, error) {
	raftLog, err := log.NewLog(dir, cfg)
	if err != nil {
		return nil, err
	}

	return &raftLogStore{Log: raftLog}, nil
}

func (rls *raftLogStore) FirstIndex() (uint64, error) {
	return rls.LowestOffset()
}

func (rls *raftLogStore) LastIndex() (uint64, error) {
	offset, err := rls.HighestOffset()

	return offset, err
}

func (rls *raftLogStore) GetLog(index uint64, outRecord *raft.Log) error {
	inRecord, err := rls.Read(index)
	if err != nil {
		return err
	}

	outRecord.Data = inRecord.Value
	outRecord.Index = inRecord.Offset
	outRecord.Term = inRecord.Term
	outRecord.Type = raft.LogType(inRecord.Type)

	return nil
}

func (rls *raftLogStore) StoreLog(record *raft.Log) error {
	return rls.StoreLogs([]*raft.Log{record})
}

func (rls *raftLogStore) StoreLogs(records []*raft.Log) error {
	for _, rcd := range records {
		if _, err := rls.Append(
			&api.Record{
				Value: rcd.Data,
				Term:  rcd.Term,
				Type:  uint32(rcd.Type),
			},
		); err != nil {
			return err
		}
	}

	return nil
}

func (rls *raftLogStore) DeleteRange(min, max uint64) error {
	return rls.Truncate(max)
}

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
