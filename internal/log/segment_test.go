package log

import (
	"io"
	"os"
	"testing"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	// dir := "segment-test"
	// err := os.Mkdir(dir, 0750)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	cfg := Config{}
	cfg.Segment.MaxStoreBytes = 1024         // 1 kilo byte
	cfg.Segment.MaxIndexBytes = endWidth * 3 // record up to 3 entries
	var baseOffset uint64 = 16

	s, err := newSegment(dir, baseOffset, cfg)
	require.NoError(t, err)
	require.Equal(t, baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	// returns an error, but the record is added and remains as garbage
	require.Equal(t, io.EOF, err)

	// an index is maxed
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	recordBytes, _ := proto.Marshal(want)
	cfg.Segment.MaxStoreBytes = uint64(len(recordBytes)+RecordSizeWidth) * 4
	cfg.Segment.MaxIndexBytes = 1024
	// reboot the exisiting segment
	s, err = newSegment(dir, baseOffset, cfg)
	require.NoError(t, err)
	// a store is maxed
	require.True(t, s.IsMaxed())

	require.NoError(t, s.Remove())

	// recreate a new segment with the same dir
	s, err = newSegment(dir, baseOffset, cfg)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

	require.NoError(t, s.Close())
}
