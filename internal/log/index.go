package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	// offset value stored in offWidth
	offWidth uint64 = 4
	// position value stored in offWidth
	positionWidth uint64 = 8
	// entire index stored in endWidth
	endWidth = offWidth + positionWidth
)

// index represents an index file to search for records in stores
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, cfg Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// truncate index file to max bytes
	if err = os.Truncate(
		f.Name(), int64(cfg.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// call mmap for index file, instead of open, read, and write system calls
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	// sync with file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	// sync with storage(disk)
	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(input int64) (off uint32, position uint64, err error) {
	// in case of no index
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if input == -1 {
		// read the latest index
		off = uint32((i.size / endWidth) - 1)
	} else {
		off = uint32(input)
	}

	position = uint64(off) * endWidth
	// in case that input is out of range
	if i.size < position+endWidth {
		return 0, 0, io.EOF
	}

	// read offset value as output
	off = Enc.Uint32(i.mmap[position : position+offWidth])
	// read position value as output
	position = Enc.Uint64(i.mmap[position+offWidth : position+endWidth])

	return off, position, nil
}

func (i *index) Write(off uint32, position uint64) error {
	if i.isMaxed() {
		return io.EOF
	}

	Enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	Enc.PutUint64(i.mmap[i.size+offWidth:i.size+endWidth], position)
	i.size += uint64(endWidth)

	return nil
}

// isMaxed returns if new entry can be written into an index file or not
func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+endWidth
}

// Name returns a file path of an index file
func (i *index) Name() string {
	return i.file.Name()
}
