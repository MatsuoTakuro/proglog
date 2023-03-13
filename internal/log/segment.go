package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store *store
	index *index
	// for calculate a relative offset for an index entry
	baseOffset uint64
	// for adding new record
	nextOffset uint64
	config     Config
}

func newSegment(
	dir string, baseOffset uint64, cfg Config,
) (*segment, error) {

	s := &segment{
		baseOffset: baseOffset,
		config:     cfg,
	}

	// create a store
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// create an index
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, cfg); err != nil {
		return nil, err
	}

	// initialize a nextOffset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {

	// marshal record struct to bytes
	cur := s.nextOffset
	record.Offset = cur
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append record to a store
	_, position, err := s.store.Append(recordBytes)
	if err != nil {
		return 0, err
	}

	// write an offset for the appended record (new entry) in index
	if err = s.index.Write(
		// offset for index is a relative one from baseOffset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		position,
	); err != nil {
		return 0, err
	}

	// update a nextOffset for a new entry
	s.nextOffset++

	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {

	// locate a record in a store from an index
	_, position, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// read a record from store
	recordBytes, err := s.store.Read(position)
	if err != nil {
		return nil, err
	}

	// unmarshal record bytes to struct
	record := &api.Record{}
	err = proto.Unmarshal(recordBytes, record)

	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return nil
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return nil
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return nil
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return nil
	}

	return nil
}

// originalReader is a io.Reader for a store (per a segment).
type originalReader struct {
	*store
	off int64
}

// Read reads an entire store file from a given position (size).
// it also can be called by an io.MultiReader a log has
func (o *originalReader) Read(size []byte) (int, error) {
	n, err := o.ReadAt(size, o.off)
	o.off += int64(n)

	return n, err
}
