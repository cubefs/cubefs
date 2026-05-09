package stream

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/require"
)

func TestOpenStreamReuseReadToWriteTriggersForbiddenMigration(t *testing.T) {
	client := &ExtentClient{
		streamers:   make(map[uint64]*Streamer),
		multiVerMgr: &MultiVerMgr{},
		renewalForbiddenMigration: func(inode uint64) error {
			return nil
		},
	}

	var callCount int32
	client.forbiddenMigration = func(inode uint64) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	const inode = uint64(7788)
	require.NoError(t, client.OpenStream(inode, false, false, "/test-file"))
	require.NoError(t, client.OpenStream(inode, true, false, "/test-file"))
	require.Equal(t, int32(1), atomic.LoadInt32(&callCount))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReuseReadToWriteForbiddenMigrationErrorSetsStreamerError(t *testing.T) {
	client := &ExtentClient{
		streamers:   make(map[uint64]*Streamer),
		multiVerMgr: &MultiVerMgr{},
		renewalForbiddenMigration: func(inode uint64) error {
			return nil
		},
	}

	client.forbiddenMigration = func(inode uint64) error {
		return errors.New("mock forbiddenMigration error")
	}

	const inode = uint64(8899)
	require.NoError(t, client.OpenStream(inode, false, false, "/test-error-file"))
	require.NoError(t, client.OpenStream(inode, true, false, "/test-error-file"))

	s, ok := client.streamers[inode]
	require.True(t, ok)
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&s.status))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

// Exercises OpenStream read->write reuse with log.Debug enabled (EnableDebug branch) and forbiddenMigration error (LogWarnf + setError).
func TestOpenStreamReadThenWriteForbiddenMigrationErrWithDebugEnabled(t *testing.T) {
	_, err := log.InitLog("", "ec_openstream_cov", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	require.NoError(t, err)

	client := &ExtentClient{
		streamers:   make(map[uint64]*Streamer),
		multiVerMgr: &MultiVerMgr{},
		renewalForbiddenMigration: func(inode uint64) error {
			return nil
		},
	}
	client.forbiddenMigration = func(inode uint64) error {
		return errors.New("mock forbiddenMigration error")
	}

	const inode = uint64(9901)
	require.NoError(t, client.OpenStream(inode, false, false, "/cov-file"))
	require.NoError(t, client.OpenStream(inode, true, false, "/cov-file"))

	s, ok := client.streamers[inode]
	require.True(t, ok)
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&s.status))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}
