package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	cfg := Config{}
	cfg.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, cfg)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := map[string]struct {
		off      uint32
		position uint64
	}{
		"entry 1": {off: 0, position: 0},
		"entry 2": {off: 1, position: 10},
	}

	for _, sub := range entries {

		err = idx.Write(sub.off, sub.position)
		require.NoError(t, err)

		_, position, err := idx.Read(int64(sub.off))
		require.NoError(t, err)
		require.Equal(t, sub.position, position)
	}

	// return err when an input index is out of range
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// reboot an existing index file and recreate index from it
	_ = idx.Close()
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, cfg)
	require.NoError(t, err)

	// read the latest index
	off, position, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32((len(entries) - 1)), off)
	require.Equal(t, entries["entry 2"].position, position)

}
