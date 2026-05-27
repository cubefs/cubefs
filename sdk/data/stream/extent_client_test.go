package stream

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/require"
)

func TestForceRefreshExtentsCacheUsesForceRefresh(t *testing.T) {
	const inode = uint64(6601)
	client := &ExtentClient{
		streamers: make(map[uint64]*Streamer),
		getExtents: func(ino uint64, isCache, openForWrite, isMigration bool) (uint64, uint64, []proto.ExtentKey, error) {
			return 2, 100, nil, nil
		},
	}
	cache := NewExtentCache(inode)
	cache.gen = 5
	s := &Streamer{
		inode:     inode,
		client:    client,
		extents:   cache,
		dirtylist: NewDirtyExtentList(),
		isOpen:    true, // avoid GetStreamer starting server() on a partial streamer
	}
	client.streamers[inode] = s

	require.NoError(t, client.ForceRefreshExtentsCache(inode))
	require.Equal(t, uint64(2), s.extents.gen,
		"force refresh must update cache even when remote gen is lower than local")
}

func noopLoadInodeInfo(uint64) (*proto.InodeInfo, error) {
	return nil, nil
}

func TestOpenStreamReuseReadToWriteTriggersForbiddenMigration(t *testing.T) {
	client := &ExtentClient{
		streamers:   make(map[uint64]*Streamer),
		multiVerMgr: &MultiVerMgr{},
		renewalForbiddenMigration: func(inode uint64) error {
			return nil
		},
		loadInodeInfo: noopLoadInodeInfo,
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

func TestOpenStreamReadToWriteDoesNotCallLoadInodeInfo(t *testing.T) {
	var loadCount int32
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
		loadInodeInfo: func(uint64) (*proto.InodeInfo, error) {
			atomic.AddInt32(&loadCount, 1)
			return nil, nil
		},
	}

	const inode = uint64(7789)
	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(0), atomic.LoadInt32(&loadCount))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteKeepsLockDuringForbiddenMigration(t *testing.T) {
	var sawStreamerLockHeld bool
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		loadInodeInfo:             noopLoadInodeInfo,
	}
	client.forbiddenMigration = func(uint64) error {
		if client.streamerLock.TryLock() {
			client.streamerLock.Unlock()
		} else {
			sawStreamerLockHeld = true
		}
		return nil
	}

	const inode = uint64(77891)
	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.True(t, sawStreamerLockHeld)

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteDoesNotSynchronouslyReloadInode(t *testing.T) {
	var loadCount int32
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
	}
	client.loadInodeInfo = func(uint64) (*proto.InodeInfo, error) {
		atomic.AddInt32(&loadCount, 1)
		return nil, nil
	}

	const inode = uint64(77892)
	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(0), atomic.LoadInt32(&loadCount))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteDoesNotFailWhenLoadInodeInfoWouldFail(t *testing.T) {
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
		loadInodeInfo: func(uint64) (*proto.InodeInfo, error) {
			return nil, errors.New("load inode failed")
		},
	}

	const inode = uint64(7790)
	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	err := client.OpenStream(inode, true, false, "/rw")
	require.NoError(t, err)

	s, ok := client.streamers[inode]
	require.True(t, ok)
	require.Equal(t, int32(StreamerNormal), atomic.LoadInt32(&s.status))
	require.Equal(t, int32(2), atomic.LoadInt32(&s.refcnt))

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteDoesNotRunLoadInodeInfoAfterOpenRequest(t *testing.T) {
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
	}

	const inode = uint64(7791)
	var loadCount int32
	client.loadInodeInfo = func(uint64) (*proto.InodeInfo, error) {
		atomic.AddInt32(&loadCount, 1)
		return nil, nil
	}

	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(0), atomic.LoadInt32(&loadCount))

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
		loadInodeInfo: noopLoadInodeInfo,
	}

	client.forbiddenMigration = func(inode uint64) error {
		return errors.New("mock forbiddenMigration error")
	}

	const inode = uint64(8899)
	client.streamers[inode] = &Streamer{
		inode:        inode,
		client:       client,
		openForWrite: false,
		extents:      NewExtentCache(inode),
		dirtylist:    NewDirtyExtentList(),
	}
	err := client.OpenStream(inode, true, false, "/test-error-file")
	require.Error(t, err)

	s, ok := client.streamers[inode]
	require.True(t, ok)
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&s.status))
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
		loadInodeInfo: noopLoadInodeInfo,
	}
	client.forbiddenMigration = func(inode uint64) error {
		return errors.New("mock forbiddenMigration error")
	}

	const inode = uint64(9901)
	client.streamers[inode] = &Streamer{
		inode:        inode,
		client:       client,
		openForWrite: false,
		extents:      NewExtentCache(inode),
		dirtylist:    NewDirtyExtentList(),
	}
	err = client.OpenStream(inode, true, false, "/cov-file")
	require.Error(t, err)

	s, ok := client.streamers[inode]
	require.True(t, ok)
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&s.status))
}

func TestOpenStreamReadToWriteDoesNotSynchronouslyRefreshExtentsCache(t *testing.T) {
	const inode = uint64(7792)
	var getExtentsCount int32
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
		getExtents: func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
			atomic.AddInt32(&getExtentsCount, 1)
			return 3, 50, nil, nil
		},
	}
	client.loadInodeInfo = func(ino uint64) (*proto.InodeInfo, error) {
		return nil, client.ForceRefreshExtentsCache(ino)
	}

	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(0), atomic.LoadInt32(&getExtentsCount))
	require.Equal(t, uint64(0), client.streamers[inode].extents.gen)

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteDoesNotLeakStreamerLockWhenLoadInodeInfoWouldFail(t *testing.T) {
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration:        func(uint64) error { return nil },
		loadInodeInfo: func(uint64) (*proto.InodeInfo, error) {
			return nil, errors.New("load inode failed")
		},
	}

	const inode = uint64(7793)
	require.NoError(t, client.OpenStream(inode, false, false, "/ro"))
	require.NoError(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(StreamerNormal), atomic.LoadInt32(&client.streamers[inode].status))
	requireCompletes(t, func() {
		_ = client.GetStreamer(inode)
	})

	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.CloseStream(inode))
	require.NoError(t, client.EvictStream(inode))
}

func TestOpenStreamReadToWriteForbiddenMigrationErrorDoesNotLeakStreamerLock(t *testing.T) {
	client := &ExtentClient{
		streamers:                 make(map[uint64]*Streamer),
		multiVerMgr:               &MultiVerMgr{},
		renewalForbiddenMigration: func(uint64) error { return nil },
		forbiddenMigration: func(uint64) error {
			return errors.New("forbidden migration failed")
		},
		loadInodeInfo: noopLoadInodeInfo,
	}

	const inode = uint64(7794)
	client.streamers[inode] = &Streamer{
		inode:        inode,
		client:       client,
		openForWrite: false,
		extents:      NewExtentCache(inode),
		dirtylist:    NewDirtyExtentList(),
	}

	require.Error(t, client.OpenStream(inode, true, false, "/rw"))
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&client.streamers[inode].status))
	requireCompletes(t, func() {
		_ = client.GetStreamer(inode)
	})
}

func requireCompletes(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("operation blocked, streamerLock may be leaked")
	}
}
