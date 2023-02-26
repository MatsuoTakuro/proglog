package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	record     = []byte("hello world")
	totalWidth = uint64(len(record) + widthRecordSizeStored)
)

func TestStoreAppendAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	// f, err := os.Create("store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		nw, position, err := s.Append(record)
		require.NoError(t, err)
		require.Equal(t, position+nw, totalWidth*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var position uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(position)
		require.NoError(t, err)
		require.Equal(t, record, read)
		position += totalWidth
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		sizeb := make([]byte, widthRecordSizeStored)
		nr, err := s.ReadAt(sizeb, off)
		require.NoError(t, err)
		require.Equal(t, widthRecordSizeStored, nr)
		off += int64(nr)

		size := enc.Uint64(sizeb)
		read := make([]byte, size)
		nr, err = s.ReadAt(read, off)
		require.NoError(t, err)
		require.Equal(t, record, read)
		require.Equal(t, int(size), nr)
		off += int64(nr)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	// f, err := os.Create("store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(record)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
