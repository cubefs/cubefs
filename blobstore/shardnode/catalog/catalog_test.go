package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cubefs/inodedb/common/sharding"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/inodedb/proto"
	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -source=transport.go -destination=./mock_master_test.go -package=catalog -mock_names masterClient=MockMaster
//go:generate mockgen -source=disk.go -destination=./mock_disk_test.go -package=catalog -mock_names shardHandler=MockShardHandler

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")
)

func mockMaster(tb testing.TB) *MockMaster {
	return NewMockMaster(C(tb))
}

func newMockCatalog(tb testing.TB) mockCatalog {
	cfg := &Config{}
	cfg.NodeConfig.GrpcPort = 7777
	cfg.NodeConfig.RaftPort = 7778
	cfg.RaftConfig.NodeID = 1
	m := mockMaster(tb)
	m.EXPECT().Cluster(A, A, A).Return(&proto.ClusterResponse{}, nil)
	m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{}, nil)
	m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{}, nil)
	return mockCatalog{
		c: newCatalog(ctx, cfg, m),
		m: m,
	}
}

type mockCatalog struct {
	c *Catalog
	m *MockMaster
}

func (mc mockCatalog) Close() {
	mc.c.Close()
	mc.m.ctrl.Finish()
}

func TestServerCatalog_New(t *testing.T) {
	cfg := &Config{}
	require.Panics(t, func() { NewCatalog(ctx, cfg) })

	mm := mockMaster(t)
	_panic := func() { require.Panics(t, func() { newCatalog(ctx, cfg, mm) }) }

	_panic()
	cfg.NodeConfig.GrpcPort = 1
	_panic()
	cfg.NodeConfig.RaftPort = 2

	mm.EXPECT().Cluster(A, A, A).Return(nil, errors.New("cluster"))
	_panic()
	mm.EXPECT().Cluster(A, A, A).Return(&proto.ClusterResponse{}, nil).AnyTimes()

	mm.EXPECT().GetCatalogChanges(A, A, A).Return(nil, errors.New("get catalog changes"))
	_panic()
	mm.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{}, nil).AnyTimes()

	mm.EXPECT().ListDisks(A, A, A).Return(nil, errors.New("list disks"))
	_panic()
	mm.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{}, nil)
	newCatalog(ctx, cfg, mm).Close()
}

func TestServerCatalog_InitRoute(t *testing.T) {
	c := newMockCatalog(t)
	defer c.Close()
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(nil, errors.New("get catalog changes"))
	require.Error(t, c.c.initRoute(ctx))
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{RouteVersion: 1}, nil)
	require.NoError(t, c.c.initRoute(ctx))
	require.Equal(t, uint64(1), c.c.routeVersion.Load())
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{
		RouteVersion: 2,
		Items: []proto.CatalogChangeItem{
			{RouteVersion: 11, Type: 1024},
			{RouteVersion: 3, Type: 1024},
		},
	}, nil)
	require.NoError(t, c.c.initRoute(ctx))
	require.Equal(t, uint64(11), c.c.routeVersion.Load())

	item, _ := types.MarshalAny(&proto.CatalogChangeSpaceDelete{})
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{
		Items: []proto.CatalogChangeItem{
			{Type: proto.CatalogChangeItem_AddSpace, Item: item},
		},
	}, nil)
	require.Error(t, c.c.initRoute(ctx))
	item, _ = types.MarshalAny(&proto.CatalogChangeSpaceAdd{})
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{
		Items: []proto.CatalogChangeItem{
			{Type: proto.CatalogChangeItem_DeleteSpace, Item: item},
		},
	}, nil)
	require.Error(t, c.c.initRoute(ctx))

	item1, _ := types.MarshalAny(&proto.CatalogChangeSpaceAdd{Sid: 1})
	item2, _ := types.MarshalAny(&proto.CatalogChangeSpaceAdd{Sid: 2, FixedFields: []proto.FieldMeta{{}}})
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{
		Items: []proto.CatalogChangeItem{
			{Type: proto.CatalogChangeItem_AddSpace, Item: item1},
			{Type: proto.CatalogChangeItem_AddSpace, Item: item2},
			{Type: proto.CatalogChangeItem_AddSpace, Item: item2},
		},
	}, nil)
	require.NoError(t, c.c.initRoute(ctx))
	length := 0
	c.c.spaces.Range(func(_, _ interface{}) bool { length++; return true })
	require.Equal(t, 2, length)

	item1, _ = types.MarshalAny(&proto.CatalogChangeSpaceDelete{Sid: 1})
	c.m.EXPECT().GetCatalogChanges(A, A, A).Return(&proto.GetCatalogChangesResponse{
		Items: []proto.CatalogChangeItem{
			{Type: proto.CatalogChangeItem_DeleteSpace, Item: item1},
		},
	}, nil)
	require.NoError(t, c.c.initRoute(ctx))
	length = 0
	c.c.spaces.Range(func(_, _ interface{}) bool { length++; return true })
	require.Equal(t, 1, length)
}

func TestServerCatalog_InitDisks(t *testing.T) {
	{
		c := newMockCatalog(t)
		defer c.Close()
		path, pathClean := tempPath()
		defer pathClean()
		c.c.cfg.Disks = append(c.c.cfg.Disks, path)

		c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{}, errors.New("list disks"))
		require.Error(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))

		c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{Disks: []proto.Disk{
			{DiskID: 0, Path: path},
			{DiskID: 0, Path: path},
		}}, nil)
		require.Error(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))
	}
	{
		c := newMockCatalog(t)
		defer c.Close()
		path, pathClean := tempPath()
		defer pathClean()
		c.c.cfg.Disks = append(c.c.cfg.Disks, path)

		c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{Disks: []proto.Disk{
			{DiskID: 0, Path: path, Status: proto.DiskStatus_DiskStatusRepaired},
		}}, nil)
		c.m.EXPECT().AllocDiskID(A, A).Return(nil, errors.New("alloc disk id"))
		require.Error(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))
	}
	{
		c := newMockCatalog(t)
		defer c.Close()
		path, pathClean := tempPath()
		defer pathClean()
		c.c.cfg.Disks = append(c.c.cfg.Disks, path)

		c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{Disks: []proto.Disk{
			{DiskID: 0, Path: path, Status: proto.DiskStatus_DiskStatusRepaired},
		}}, nil)
		c.m.EXPECT().AllocDiskID(A, A).Return(&proto.AllocDiskIDResponse{DiskID: 11}, nil)
		c.m.EXPECT().AddDisk(A, A).Return(nil, errors.New("add disk"))
		require.Error(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))
	}
	{
		c := newMockCatalog(t)
		defer c.Close()
		path, pathClean := tempPath()
		defer pathClean()
		c.c.cfg.Disks = append(c.c.cfg.Disks, path)

		c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{Disks: []proto.Disk{
			{DiskID: 0, Path: path, Status: proto.DiskStatus_DiskStatusRepaired},
		}}, nil)
		c.m.EXPECT().AllocDiskID(A, A).Return(&proto.AllocDiskIDResponse{DiskID: 11}, nil)
		c.m.EXPECT().AddDisk(A, A).Return(nil, nil)
		require.NoError(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))
	}
}

func TestServerCatalog_Shard(t *testing.T) {
	c := newMockCatalog(t)
	defer c.Close()
	path, pathClean := tempPath()
	defer pathClean()
	c.c.cfg.Disks = append(c.c.cfg.Disks, path)

	diskID := proto.DiskID(11)

	c.m.EXPECT().ListDisks(A, A, A).Return(&proto.ListDiskResponse{Disks: []proto.Disk{
		{DiskID: 0, Path: path, Status: proto.DiskStatus_DiskStatusRepaired},
	}}, nil)
	c.m.EXPECT().AllocDiskID(A, A).Return(&proto.AllocDiskIDResponse{DiskID: diskID}, nil)
	c.m.EXPECT().AddDisk(A, A).Return(nil, nil)
	require.NoError(t, c.c.initDisks(ctx, c.c.cfg.NodeConfig.ID))

	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)

	// AddShard
	req := &proto.AddShardRequest{
		DiskID:  diskID,
		ShardID: 1,
		Range:   *rg,
		Epoch:   1,
		Nodes:   []proto.ShardNode{{DiskID: diskID}},
	}
	c.m.EXPECT().GetSpace(A, A).Return(nil, errors.New("get space"))
	require.Error(t, c.c.AddShard(ctx, req))
	c.m.EXPECT().GetSpace(A, A).Return(&proto.GetSpaceResponse{Info: proto.SpaceMeta{Sid: 1}}, nil).AnyTimes()
	req.DiskID = diskID + 1
	require.Error(t, c.c.AddShard(ctx, req))

	req.DiskID = diskID
	require.NoError(t, c.c.AddShard(ctx, req))

	_, err := c.c.getShard(diskID+1, 1)
	require.Error(t, err)
	_, err = c.c.getShard(diskID, 2)
	require.Error(t, err)
	s, err := c.c.getShard(diskID, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), s.(*shard).GetEpoch())

	// GetShardInfo
	_, err = c.c.GetShardInfo(ctx, diskID+1, 1)
	require.Error(t, err)
	_, err = c.c.GetShardInfo(ctx, diskID, 1)
	require.Error(t, err)

	shardInfo, err := c.c.GetShardInfo(ctx, diskID, 1)
	require.NoError(t, err)
	t.Logf("%+v", shardInfo)

	// GetSpace
	_, err = c.c.GetSpace(ctx, 2)
	require.Error(t, err)
	_, err = c.c.GetSpace(ctx, 1)
	require.NoError(t, err)

	// executeShardTask
	task := proto.ShardTask{DiskID: diskID + 1}
	require.Error(t, (*catalogTask)(c.c).executeShardTask(ctx, task))
	task = proto.ShardTask{DiskID: diskID, Sid: 2}
	require.Error(t, (*catalogTask)(c.c).executeShardTask(ctx, task))

	task = proto.ShardTask{Type: proto.ShardTask_Checkpoint, DiskID: diskID, Sid: 1, ShardID: 1}
	require.NoError(t, (*catalogTask)(c.c).executeShardTask(ctx, task))
	task = proto.ShardTask{Type: proto.ShardTask_ClearShard, DiskID: diskID, Sid: 1, ShardID: 2, Epoch: 1}
	require.NoError(t, (*catalogTask)(c.c).executeShardTask(ctx, task))

	// Report
	s.(*shard).lastStableIndex = s.(*shard).GetAppliedIndex() + 1
	t.Logf("%+v", c.c.getAlteredShardReports())
	t.Logf("%+v", c.c.getAlteredShardCheckpointTasks())

	c.m.EXPECT().Heartbeat(A, A).Return(nil, errors.New("heartbeat")).AnyTimes()
	go c.c.loop(ctx)
	time.Sleep(1500 * time.Millisecond)
}
