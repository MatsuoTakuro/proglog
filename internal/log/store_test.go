package log

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	record     = []byte("hello world")
	totalWidth = uint64(len(record) + recordSizeWidth)
)

func TestStoreAppendAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	// f, err := os.Create("store_append_read_test") // can see binary file for debugging, instead
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
	printFile(t, s.File.Name()) // print no content because store.Append does not flush
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
	printFile(t, s.File.Name())
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		sizeb := make([]byte, recordSizeWidth)
		nr, err := s.ReadAt(sizeb, off)
		require.NoError(t, err)
		require.Equal(t, recordSizeWidth, nr)
		off += int64(nr)

		size := enc.Uint64(sizeb)
		read := make([]byte, size)
		nr, err = s.ReadAt(read, off)
		require.NoError(t, err)
		require.Equal(t, record, read)
		require.Equal(t, int(size), nr)
		off += int64(nr)
	}
	printFile(t, s.File.Name())
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	// f, err := os.Create("store_close_test") // can see binary file for debugging, instead
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(record)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	printFile(t, f.Name())

	err = s.Close()
	require.NoError(t, err)

	f, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	printFile(t, f.Name())

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

// print content of file for debugging
func printFile(t *testing.T, name string) {
	t.Helper()

	b, err := os.ReadFile(name)
	if err != nil {
		t.Error(err)
	}
	log.Println(string(b))
}
