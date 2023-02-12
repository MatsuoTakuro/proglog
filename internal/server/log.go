package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{
		mu:      sync.Mutex{},
		records: []Record{},
	}
}

func (l *Log) Append(new Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	new.Offset = uint64(len(l.records))
	l.records = append(l.records, new)
	return new.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return l.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
