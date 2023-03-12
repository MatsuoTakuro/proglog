package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/MatsuoTakuro/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex
	// a dir to save segments
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, cfg Config) (*Log, error) {
	if cfg.Segment.MaxStoreBytes == 0 {
		cfg.Segment.MaxStoreBytes = 1024 // 1 kilo bytes
	}
	if cfg.Segment.MaxIndexBytes == 0 {
		cfg.Segment.MaxIndexBytes = 1024 // 1 kilo bytes
	}

	l := &Log{
		Dir:    dir,
		Config: cfg,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	// read a list of possible segments that may exist under dir
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// sort a list of possible segments
	// in order of decreasing value of offset (iow, oldest first)
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// recreate a list of possible segments in the sorted order
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains duplicates for index and store
		// so we skip the other one
		i++
	}

	// in case of no exisiting segments, create the first segment
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, nil
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	// in case of a new segment with no entry
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

// Truncate removes all segments whose max offset (= nextOffset) is lower than a given lowest.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var ss []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		ss = append(ss, s)
	}
	l.segments = ss

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

// Readers returns an io.MultiReader that can read an entire log by consolidating stores segments have.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originalReader{s.store, 0}
	}

	return io.MultiReader(readers...)
}
