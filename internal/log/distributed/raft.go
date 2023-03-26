package distributed

/*
	Raft consists of fsm, log store, stable store, snapshot, and transport.
	here describes implementations for the three elements of them; fsm, snapshot, and log store.
*/

import (
	"bytes"
	"io"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/MatsuoTakuro/proglog/internal/log"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
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
	reqType := log.RequestType(buf[0])
	switch reqType {
	case log.AppendRequestType:
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
