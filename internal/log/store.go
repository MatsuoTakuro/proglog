package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// order of encoding to binary (binary-serialization)
	Enc = binary.BigEndian
)

const (
	// fixed width to store size of record
	RecordSizeWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// fs.FileInfo can be known from https://pkg.go.dev/io/fs#FileInfo
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

func (s *store) Append(record []byte) (n uint64, position uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	position = s.size
	// write size of record
	// you can specify size (bytes) of record when you read the record
	if err := binary.Write(s.buf, Enc, uint64(len(record))); err != nil {
		return 0, 0, err
	}

	// write record
	nw, err := s.buf.Write(record)
	if err != nil {
		return 0, 0, err
	}

	nw += RecordSizeWidth
	s.size += uint64(nw)
	return uint64(nw), position, nil
}

func (s *store) Read(position uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read size of record
	size := make([]byte, RecordSizeWidth)
	if _, err := s.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}

	// read record
	record := make([]byte, Enc.Uint64(size))
	if _, err := s.File.ReadAt(record, int64(position+RecordSizeWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

// ReadAt returns number of bytes read
// if there is any data in len(size of record or width of it stored) from offset
func (s *store) ReadAt(size []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(size, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
