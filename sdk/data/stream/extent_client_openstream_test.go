package stream

import (
	"sync/atomic"
	"testing"

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
