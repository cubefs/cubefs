package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

func tempPath() (string, func()) {
	tmp := path.Join(os.TempDir(), fmt.Sprintf("shardserver_disk_%d", rand.Int31n(10000)+10000))
	return tmp, func() { os.RemoveAll(tmp) }
}

type mockDisk struct {
	d  *Disk
	sh *MockShardHandler
}

func newMockDisk(tb testing.TB) (*mockDisk, func()) {
	diskPath, pathClean := tempPath()
	var cfg DiskConfig
	cfg.DiskPath = diskPath
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true
	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, lockCF, dataCF, writeCF)
	cfg.StoreConfig.RaftOption.ColumnFamily = append(cfg.StoreConfig.RaftOption.ColumnFamily, raftWalCF)

	cfg.RaftConfig.NodeID = 1
	mockResolver := raft.NewMockAddressResolver(C(tb))
	cfg.RaftConfig.Resolver = mockResolver
	mockResolver.EXPECT().Resolve(A, A).Return(raft.NewMockAddr(C(tb)).EXPECT().String().Return("127.0.0.1:8080").AnyTimes()).AnyTimes()
	sh := NewMockShardHandler(C(tb))
	cfg.ShardHandler = sh

	disk := OpenDisk(ctx, cfg)
	disk.DiskID = 1
	require.NoError(tb, disk.Load(ctx))
	return &mockDisk{d: disk, sh: sh}, func() {
		time.Sleep(time.Second)
		disk.raftManager.Close()
		disk.store.KVStore().Close()
		disk.store.RaftStore().Close()
		pathClean()
	}
}

func TestServerDisk_Open(t *testing.T) {
	c := newMockCatalog(t)
	defer c.Close()
	diskPath, pathClean := tempPath()
	defer pathClean()

	var cfg DiskConfig
	cfg.DiskPath = diskPath
	_panic := func() { require.Panics(t, func() { OpenDisk(ctx, cfg) }) }

	cfg.CheckMountPoint = true
	_panic()
	cfg.CheckMountPoint = false
	_panic()
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true

	cfg.Transport = c.c.transport
	disk := OpenDisk(ctx, cfg)

	c.m.EXPECT().DiskSetBroken(A, A).Return(nil, errors.New("set Disk broken"))
	disk.cfg.StoreConfig.HandleEIO(nil)

	require.NoError(t, disk.SaveDiskInfo(disk.GetDiskInfo()))
	t.Logf("Disk info: %+v", disk.GetDiskInfo())
	disk.store.KVStore().Close()
	disk.store.RaftStore().Close()

	disk = OpenDisk(ctx, cfg)
	disk.store.KVStore().Close()
	disk.store.RaftStore().Close()

	f, err := disk.store.NewRawFS(sysRawFSPath).CreateRawFile(diskMetaFile)
	require.NoError(t, err)
	f.Write([]byte{'0'})
	_panic()
}

func TestServerDisk_Shard(t *testing.T) {
	c := newMockCatalog(t)
	defer c.Close()
	d, diskClean := newMockDisk(t)
	defer diskClean()

	d.d.cfg.Transport = c.c.transport
	require.Equal(t, 0, d.d.GetShardCnt())

	d.sh.EXPECT().GetSpace(A, A).Return(nil, errors.New("get space"))
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	require.Error(t, d.d.AddShard(ctx, 1, 1, *rg, []proto.ShardNode{{}}))

	shardID := proto.ShardID(1)
	_, err := d.d.GetShard(shardID)
	require.Error(t, err)

	d.sh.EXPECT().GetSpace(A, A).Return(&Space{}, nil).AnyTimes()
	d.sh.EXPECT().GetShardBaseConfig().Return(&ShardBaseConfig{}).AnyTimes()
	d.sh.EXPECT().GetNodeInfo().Return(&proto.Node{}).AnyTimes()

	require.NoError(t, d.d.AddShard(ctx, shardID, 1, *rg, []proto.ShardNode{{DiskID: 1}}))
	require.NoError(t, d.d.AddShard(ctx, shardID, 1, *rg, []proto.ShardNode{{DiskID: 1}}))

	s, err := d.d.GetShard(shardID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.GetEpoch())

	require.NoError(t, d.d.AddShard(ctx, shardID+1, 1, *rg, []proto.ShardNode{{DiskID: 1}}))

	d.d.RangeShard(func(s *shard) bool { s.Checkpoint(ctx); return true })
	require.NoError(t, d.d.DeleteShard(ctx, shardID+1))
	require.NoError(t, d.d.DeleteShard(ctx, shardID+1))

	require.NoError(t, d.d.Load(ctx))
}
