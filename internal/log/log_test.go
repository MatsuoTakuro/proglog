package log

import (
	"io"
	"os"
	"testing"

	api "github.com/MatsuoTakuro/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		// initialize a map of scenario and test func
		// every test func needs to have *testing.T and *Log arguments
		"apped and read a reocrd succeeds": testAppendRead,
		"offset out of range error":        testOutOfRangeErr,
		"init with exsiting segments":      testInitExisting,
		"reader":                           testReader,
		"truncate":                         testTruncate,
	} {
		// iterates the followig process with every scenario and test func
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log-test")
			// dir := "log-test"
			// err := os.Mkdir(dir, 0750)
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			cfg := Config{}
			cfg.Segment.MaxStoreBytes = 32

			log, err := NewLog(dir, cfg)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
	require.NoError(t, log.Close())
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	existing, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	off, err = existing.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = existing.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
	require.NoError(t, err)

	require.NoError(t, existing.Close())
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(readBytes[RecordSizeWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
	require.NoError(t, log.Close())
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
	require.NoError(t, log.Close())
}
