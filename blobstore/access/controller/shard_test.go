package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var (
	gAny    = gomock.Any()
	errMock = errors.New("fake error")
)

func TestShardController(t *testing.T) {
	ctx := context.Background()

	stopCh := make(chan struct{})
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().AuthSpace(gAny, gAny).Return(nil)
	cmCli.EXPECT().GetSpaceByName(gAny, gAny).Return(&clustermgr.Space{
		SpaceID: 1,
		Name:    "spaceTest",
	}, nil)
	retCatlog := &clustermgr.GetCatalogChangesRet{}
	retCatlog.RouteVersion = 1
	cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)

	cmCli.EXPECT().GetService(gAny, gAny).Return(clustermgr.ServiceInfo{
		Nodes: []clustermgr.ServiceNode{
			{ClusterID: 1, Name: proto.ServiceNameProxy, Host: "proxy-1", Idc: "test-idc"},
			{ClusterID: 1, Name: proto.ServiceNameProxy, Host: "proxy-2", Idc: "test-idc"},
		},
	}, nil)
	svrCtrl, err := NewServiceController(ServiceConfig{IDC: "test-idc"}, cmCli, nil, nil)
	require.NoError(t, err)

	require.NoError(t, err)
	s, err := NewShardController(shardCtrlConf{}, cmCli, svrCtrl, stopCh)
	require.Nil(t, err)
	require.NotEqual(t, errMock, err)

	blobName := []byte("blob1")
	shardKeys := [][]byte{blobName}
	// empty tree
	_, err = s.GetShard(ctx, shardKeys)
	require.NotNil(t, err)

	sh := &shard{
		shardID: 1,
		version: 1,
	}
	// rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 2)  // keys len=1; subs len=2. will panic
	rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	for i := range rangePtr.Subs {
		rangePtr.Subs[i].Min = uint64(i * 1)
		rangePtr.Subs[i].Max = uint64(i+1) * 1
	}
	sh.rangeExt = *rangePtr
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
	}

	// add one, not found blob
	svr.addShardNoLock(sh)
	require.Equal(t, 1, len(svr.shards))
	_, err = svr.GetShard(ctx, shardKeys)
	require.NotNil(t, err)
	svr.delShardNoLock(sh)
	require.Equal(t, 0, len(svr.shards))

	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 1, 8)
	{
		// add 8 shard
		shards := make([]*shard, 8)
		for i := 0; i < 8; i++ {
			sd := &shard{
				shardID:      proto.ShardID(i + 1),
				leaderDiskID: 1,
				version:      proto.RouteVersion(i + 1),
				units: []clustermgr.ShardUnit{
					{
						Suid:   proto.EncodeSuid(proto.ShardID(i+1), 0, 0),
						DiskID: 1,
						// Host:   "testHost1",
					},
					{
						Suid:   proto.EncodeSuid(proto.ShardID(i+1), 1, 0),
						DiskID: 2,
					},
					{
						Suid:   proto.EncodeSuid(proto.ShardID(i+1), 2, 0),
						DiskID: 3,
					},
				},
				punishCtrl: svrCtrl,
			}
			sd.rangeExt = *ranges[i]
			shards[i] = sd
		}

		for i := 0; i < 8; i++ {
			svr.addShardNoLock(shards[i])
		}
		require.Equal(t, 8, len(svr.shards))
		svr.punishCtrl = svrCtrl
		svr.version = 8

		ret, err := svr.GetShard(ctx, [][]byte{[]byte("blob1__xxx")}) // expect 2 keys
		sk := [][]byte{[]byte("blob1__xxx")}
		bd := sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, sk).GetBoundary()
		t.Logf("shard key 1, key boundary=%d, shardBounary=%d, range=%s, treeLen=%d", bd, ret.(*shard).rangeExt.MaxBoundary(), ret.(*shard).String(), svr.ranges.Len())
		// for i := range shards {
		//	sBd := shards[i].rangeExt.MaxBoundary()
		//	t.Logf("shard=%d, boundary=%d, isLess=%v", shards[i].shardID, sBd, bd.Less(sBd))
		// }
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(2), ret.(*shard).shardID)

		ret, err = svr.GetShard(ctx, [][]byte{[]byte("blob2__yy"), []byte("11")})
		bd = sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, [][]byte{[]byte("blob2__yy"), []byte("11")}).GetBoundary()
		t.Logf("shard key 2,  get boundary=%d", bd)
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(7), ret.(*shard).shardID)
	}

	{
		// get
		si, ok := svr.getShardByID(1)
		require.True(t, ok)
		require.Equal(t, proto.ShardID(1), si.shardID)

		svr.spaceID = 1
		spID := svr.GetSpaceID()
		require.Equal(t, proto.SpaceID(1), spID)

		// update shard, switch leader 3 -> 2 ; (1, 2 leader, 4)
		newShard := *si
		// newShard.version++ // shard node switch leader don't increment version
		newShard.leaderDiskID = 2
		newShard.units[2].Learner = true
		newShard.units = append(newShard.units, clustermgr.ShardUnit{
			Suid:   proto.EncodeSuid(newShard.shardID, 2, 1),
			DiskID: 4,
		})
		err = svr.UpdateShard(ctx, shardnode.ShardStats{
			Suid:         proto.EncodeSuid(newShard.shardID, 1, 0),
			LeaderDiskID: newShard.leaderDiskID,
			LeaderSuid:   proto.EncodeSuid(newShard.shardID, 1, 0),
			RouteVersion: newShard.version,
			Range:        newShard.rangeExt,
			Units:        newShard.units,
		})
		require.NoError(t, err)
		require.Equal(t, newShard.leaderDiskID, si.leaderDiskID)

		si, ok = svr.getShardByID(1)
		require.True(t, ok)
		require.Equal(t, clustermgr.ShardUnit{Suid: proto.EncodeSuid(1, 0, 0), DiskID: 1}, si.units[0])
		require.Equal(t, clustermgr.ShardUnit{Suid: proto.EncodeSuid(1, 1, 0), DiskID: 2}, si.units[1])
		// require.Equal(t, clustermgr.ShardUnit{Suid: proto.EncodeSuid(1, 2, 1), DiskID: 4}, si.units[2])

		cmCli.EXPECT().ShardNodeDiskInfo(gAny, proto.DiskID(2)).Return(&clustermgr.ShardNodeDiskInfo{
			DiskInfo:                   clustermgr.DiskInfo{Host: "testHost1", Idc: "test-idc"},
			ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 2},
		}, nil)
		opInfo, err := si.GetMember(ctx, acapi.GetShardModeLeader, 0)
		require.NoError(t, err)
		require.Equal(t, ShardOpInfo{
			DiskID:       2,
			Suid:         proto.EncodeSuid(1, 1, 0),
			RouteVersion: 1,
		}, opInfo)

		// update route, disk:3 -> disk:4
		si.units[2] = clustermgr.ShardUnit{Suid: proto.EncodeSuid(1, 2, 1), DiskID: 4}

		// switch leader 2 -> 4;  (1, 2, 4 leader)
		newShard.units[2] = newShard.units[3]
		newShard.units = newShard.units[:3]
		newShard.leaderDiskID = 4
		newShard.leaderSuid = proto.EncodeSuid(newShard.shardID, 2, 1)
		newShard.version = si.version
		err = svr.UpdateShard(ctx, shardnode.ShardStats{
			Suid:         newShard.units[2].Suid,
			LeaderDiskID: newShard.leaderDiskID,
			LeaderSuid:   newShard.units[2].Suid,
			RouteVersion: newShard.version,
			Range:        newShard.rangeExt,
			Units:        newShard.units,
		})
		require.NoError(t, err)
		require.Equal(t, newShard.leaderDiskID, si.leaderDiskID)

		si, ok = svr.getShardByID(1)
		require.True(t, ok)
		require.Equal(t, newShard, *si)
	}
}

func TestShardUpdate(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
	}

	// concurrence
	{
		// update
		cmCli.EXPECT().GetCatalogChanges(gAny, gAny).DoAndReturn(
			func(ctx context.Context, args *clustermgr.GetCatalogChangesArgs) (ret *clustermgr.GetCatalogChangesRet, err error) {
				time.Sleep(time.Millisecond * 10)
				return &clustermgr.GetCatalogChangesRet{}, errMock
			}).Times(1)
		svr.cmCli = cmCli

		resultCh := make(chan error, 3)
		for i := 0; i < 3; i++ {
			go func() {
				err := svr.UpdateRoute(ctx)
				resultCh <- err
			}()
		}
		for i := 0; i < 3; i++ {
			err := <-resultCh
			require.ErrorIs(t, err, errMock)
		}
	}

	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 1, 10)
	{
		// panic
		val := clustermgr.CatalogChangeShardAdd{
			ShardID: 0,
		}
		data, err := val.Marshal()
		require.NoError(t, err)

		retCatlog := &clustermgr.GetCatalogChangesRet{
			RouteVersion: 1,
			Items: []clustermgr.CatalogChangeItem{
				{
					Type:         proto.CatalogChangeItemAddShard,
					RouteVersion: 1,
					Item: &types.Any{
						TypeUrl: "",
						Value:   data,
					},
				},
			},
		}
		cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)

		require.Panics(t, func() {
			err = svr.UpdateRoute(ctx)
		})
	}

	{
		// update, leader disk is 0, 2 success and 2 wrong
		val := clustermgr.CatalogChangeShardAdd{
			ShardID:      1,
			RouteVersion: 1,
			Units: []clustermgr.ShardUnitInfo{
				{
					Suid:         proto.EncodeSuid(1, 0, 0),
					DiskID:       1,
					AppliedIndex: 0,
					LeaderDiskID: 1,
					Range:        *ranges[0],
					RouteVersion: 1,
					Host:         "testHost1",
					Learner:      false,
				},
			},
		}
		data1, err := val.Marshal()
		require.NoError(t, err)

		val = clustermgr.CatalogChangeShardAdd{
			ShardID:      2,
			RouteVersion: 2,
			Units: []clustermgr.ShardUnitInfo{
				{
					Suid:         proto.EncodeSuid(2, 0, 0),
					DiskID:       2,
					LeaderDiskID: 0, // wrong
					Range:        *ranges[1],
					RouteVersion: 2,
				},
			},
		}
		data2, err := val.Marshal()
		require.NoError(t, err)

		val3 := clustermgr.CatalogChangeShardUpdate{
			ShardID:      1,
			RouteVersion: 3,
			Unit: clustermgr.ShardUnitInfo{
				Suid:         proto.EncodeSuid(1, 0, 1),
				DiskID:       3,
				LeaderDiskID: 0, // wrong
				RouteVersion: 3,
				Range:        *ranges[0],
			},
		}
		data3, err := val3.Marshal()
		require.NoError(t, err)

		val4 := clustermgr.CatalogChangeShardUpdate{
			ShardID:      1,
			RouteVersion: 3,
			Unit: clustermgr.ShardUnitInfo{
				Suid:         proto.EncodeSuid(1, 0, 1),
				DiskID:       3,
				LeaderDiskID: 3,
				RouteVersion: 3,
				Range:        *ranges[0],
			},
		}
		data4, err := val4.Marshal()
		require.NoError(t, err)

		retCatlog := &clustermgr.GetCatalogChangesRet{
			RouteVersion: val4.RouteVersion,
			Items: []clustermgr.CatalogChangeItem{
				{
					Type:         proto.CatalogChangeItemAddShard,
					RouteVersion: 1,
					Item:         &types.Any{TypeUrl: "", Value: data1}, // success
				},
				{
					Type:         proto.CatalogChangeItemAddShard,
					RouteVersion: 2,
					Item:         &types.Any{Value: data2}, // wrong
				},
				{
					Type:         proto.CatalogChangeItemUpdateShard,
					RouteVersion: 3,
					Item:         &types.Any{Value: data3}, // wrong
				},
				{
					Type:         proto.CatalogChangeItemUpdateShard,
					RouteVersion: 4,
					Item:         &types.Any{Value: data4}, // success
				},
			},
		}
		cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)

		err = svr.UpdateRoute(ctx)
		require.Equal(t, errCatalogNoLeader, err)
		require.Equal(t, proto.RouteVersion(3), svr.version)
		require.Equal(t, 2, len(svr.shards))
	}

	{
		// add shard id=9
		const shardID = 9
		const version = 4
		val := clustermgr.CatalogChangeShardAdd{
			ShardID:      shardID,
			RouteVersion: version,
			Units: []clustermgr.ShardUnitInfo{
				{
					Suid:         proto.EncodeSuid(shardID, 0, 0),
					DiskID:       1,
					AppliedIndex: 0,
					LeaderDiskID: 2,
					Range:        *ranges[8],
					RouteVersion: version,
					Host:         "testHost1",
					Learner:      false,
				},
				{
					Suid:         proto.EncodeSuid(shardID, 1, 0),
					DiskID:       2,
					AppliedIndex: 0,
					LeaderDiskID: 2,
					Range:        *ranges[8],
					RouteVersion: version,
					Host:         "testHost2",
					Learner:      false,
				},
				{
					Suid:         proto.EncodeSuid(shardID, 2, 0),
					DiskID:       3,
					AppliedIndex: 0,
					LeaderDiskID: 2,
					Range:        *ranges[8],
					RouteVersion: version,
					Host:         "testHost3",
					Learner:      false,
				},
			},
		}
		data, err := val.Marshal()
		require.NoError(t, err)
		retCatlog := &clustermgr.GetCatalogChangesRet{
			RouteVersion: version,
			Items: []clustermgr.CatalogChangeItem{
				{
					RouteVersion: version,
					Type:         proto.CatalogChangeItemAddShard,
					Item: &types.Any{
						TypeUrl: "",
						Value:   data,
					},
				},
			},
		}
		cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)
		svr.cmCli = cmCli
		err = svr.UpdateRoute(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.RouteVersion(version), svr.version)

		si, ok := svr.getShardByID(shardID)
		require.True(t, ok)
		require.Equal(t, proto.ShardID(shardID), si.shardID)

		require.Equal(t, 3, len(svr.shards))

		expect := shard{
			shardID:      shardID,
			leaderDiskID: val.Units[0].LeaderDiskID,
			leaderSuid:   val.Units[1].Suid,
			version:      version,
			rangeExt:     val.Units[1].Range,
			units:        convertShardUnitInfo(val.Units),
			punishCtrl:   svr.punishCtrl,
		}
		require.Equal(t, expect, *si)
	}

	{
		// add shard id=10
		const shardID = 10
		const oldVersion = 4
		const version = 5
		sd := &shard{
			shardID: proto.ShardID(shardID),
			version: oldVersion,
			units:   make([]clustermgr.ShardUnit, 1),
		}
		sd.rangeExt = *ranges[9]
		svr.addShardNoLock(sd)
		require.Equal(t, 4, len(svr.shards))

		val := clustermgr.CatalogChangeShardUpdate{
			ShardID:      shardID,
			RouteVersion: version,
			Unit: clustermgr.ShardUnitInfo{
				Suid:         proto.EncodeSuid(shardID, 0, 0),
				DiskID:       2,
				AppliedIndex: 0,
				LeaderDiskID: 2,
				Range:        *ranges[9],
				RouteVersion: version,
				Host:         "testHost2",
				Learner:      false,
			},
		}
		data, err := val.Marshal()
		require.NoError(t, err)
		retCatlog := &clustermgr.GetCatalogChangesRet{
			RouteVersion: version,
			Items: []clustermgr.CatalogChangeItem{
				{
					Type:         proto.CatalogChangeItemUpdateShard,
					RouteVersion: version,
					Item: &types.Any{
						TypeUrl: "",
						Value:   data,
					},
				},
			},
		}
		cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)
		svr.cmCli = cmCli
		err = svr.UpdateRoute(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.RouteVersion(version), svr.version)

		si, ok := svr.getShardByID(shardID)
		require.True(t, ok)
		require.Equal(t, proto.ShardID(shardID), si.shardID)

		require.Equal(t, 4, len(svr.shards))
	}

	{
		rv := svr.version + 1
		shardID := proto.ShardID(9)
		oldShard, exist := svr.getShardNoLock(shardID)
		require.True(t, exist)

		val := clustermgr.CatalogChangeShardUpdate{
			ShardID:      shardID,
			RouteVersion: rv,
			Unit: clustermgr.ShardUnitInfo{
				Suid:         proto.EncodeSuid(shardID, 2, 1),
				DiskID:       3,
				LeaderDiskID: 2,
				Range:        *ranges[8],
				RouteVersion: rv,
				Host:         "testHost3",
				Learner:      false,
			},
		}
		data, err := val.Marshal()
		require.NoError(t, err)

		item := clustermgr.CatalogChangeItem{
			RouteVersion: svr.version + 1,
			Type:         proto.CatalogChangeItemUpdateShard,
			Item: &types.Any{
				TypeUrl: "",
				Value:   data,
			},
		}
		svr.handleShardUpdate(ctx, item)

		sd, exist := svr.getShardNoLock(shardID)
		require.True(t, exist)

		expect := shard{
			shardID:      oldShard.shardID,
			leaderDiskID: 2,
			leaderSuid:   proto.EncodeSuid(shardID, 1, 0),
			version:      rv,
			rangeExt:     oldShard.rangeExt,
			units:        oldShard.units,
			punishCtrl:   oldShard.punishCtrl,
		}
		idx := val.Unit.Suid.Index()
		expect.units[idx] = clustermgr.ShardUnit{
			Suid:    val.Unit.Suid,
			DiskID:  val.Unit.DiskID,
			Learner: val.Unit.Learner,
		}

		require.Equal(t, expect, *sd)
	}

	{
		rv := svr.version + 1
		shardID := proto.ShardID(9)
		oldShard, exist := svr.getShardNoLock(shardID)
		require.True(t, exist)

		val := clustermgr.CatalogChangeShardUpdate{
			ShardID:      shardID,
			RouteVersion: rv,
			Unit: clustermgr.ShardUnitInfo{
				Suid:         proto.EncodeSuid(shardID, 1, 1),
				DiskID:       4,
				LeaderDiskID: 0,
				Range:        *ranges[8],
				RouteVersion: rv,
			},
		}
		data, err := val.Marshal()
		require.NoError(t, err)

		item := clustermgr.CatalogChangeItem{
			RouteVersion: rv,
			Type:         proto.CatalogChangeItemUpdateShard,
			Item: &types.Any{
				TypeUrl: "",
				Value:   data,
			},
		}
		svr.handleShardUpdate(ctx, item)

		sd, exist := svr.getShardNoLock(shardID)
		require.True(t, exist)

		expect := shard{
			shardID:      oldShard.shardID,
			leaderDiskID: 4,
			leaderSuid:   proto.EncodeSuid(shardID, 1, 1),
			version:      rv,
			rangeExt:     oldShard.rangeExt,
			units:        oldShard.units,
			punishCtrl:   oldShard.punishCtrl,
		}
		idx := val.Unit.Suid.Index()
		expect.units[idx] = clustermgr.ShardUnit{
			Suid:    val.Unit.Suid,
			DiskID:  val.Unit.DiskID,
			Learner: val.Unit.Learner,
		}

		require.Equal(t, expect, *sd)

		// error
		val.RouteVersion = 1
		data, err = val.Marshal()
		require.NoError(t, err)
		item.RouteVersion = val.RouteVersion
		item.Item.Value = data
		err = svr.handleShardUpdate(ctx, item)
		require.ErrorIs(t, err, errCatalogInvalid)
	}
}

func TestShardGetShard(t *testing.T) {
	ctx := context.Background()
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
	}

	ctr := gomock.NewController(t)
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().GetService(gAny, gAny).Return(clustermgr.ServiceInfo{
		Nodes: []clustermgr.ServiceNode{
			{ClusterID: 1, Name: proto.ServiceNameProxy, Host: "proxy-1", Idc: "test-idc"},
		},
	}, nil)
	cmCli.EXPECT().ShardNodeDiskInfo(gAny, proto.DiskID(1)).Return(&clustermgr.ShardNodeDiskInfo{
		DiskInfo:                   clustermgr.DiskInfo{Host: "testHost1", Idc: "test-idc"},
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 1},
	}, nil)
	cmCli.EXPECT().ShardNodeDiskInfo(gAny, proto.DiskID(2)).Return(&clustermgr.ShardNodeDiskInfo{
		DiskInfo:                   clustermgr.DiskInfo{Host: "testHost2", Idc: "test-idc"},
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 2},
	}, nil)
	cmCli.EXPECT().ShardNodeDiskInfo(gAny, proto.DiskID(3)).Return(&clustermgr.ShardNodeDiskInfo{
		DiskInfo:                   clustermgr.DiskInfo{Host: "testHost3", Idc: "test-idc"},
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: 3},
	}, nil)
	svrCtrl, err := NewServiceController(ServiceConfig{IDC: "test-idc"}, cmCli, nil, nil)
	require.NoError(t, err)
	svrCtrl.GetShardnodeHost(ctx, 1)
	svrCtrl.GetShardnodeHost(ctx, 2)
	svrCtrl.GetShardnodeHost(ctx, 3)

	// add 8 shard
	shards := make([]*shard, 8)
	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, 8)

	for i := 0; i < 8; i++ {
		sd := &shard{
			shardID:      proto.ShardID(i + 1),
			version:      1,
			leaderDiskID: 1,
			leaderSuid:   proto.EncodeSuid(proto.ShardID(i+1), 0, 0),
			units: []clustermgr.ShardUnit{
				{
					Suid:   proto.EncodeSuid(proto.ShardID(i+1), 0, 0),
					DiskID: 1,
					Host:   "testHost1",
				},
				{
					Suid:   proto.EncodeSuid(proto.ShardID(i+1), 1, 0),
					DiskID: 2,
					Host:   "testHost2",
				},
				{
					Suid:   proto.EncodeSuid(proto.ShardID(i+1), 2, 0),
					DiskID: 3,
					Host:   "testHost3",
				},
			},
			punishCtrl: svrCtrl,
		}
		sd.rangeExt = *ranges[i]
		shards[i] = sd
	}
	// shards[7].rangeExt.Subs[0].Max = math.MaxUint64

	for i := 0; i < 8; i++ {
		svr.addShardNoLock(shards[i])
	}
	require.Equal(t, 8, len(svr.shards))

	{
		sd, err := svr.GetShard(ctx, [][]byte{[]byte("blob1"), []byte("1")})
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(2), sd.GetShardID())

		sd, err = svr.GetShard(ctx, [][]byte{[]byte("blob1"), {}})
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(2), sd.GetShardID())

		// shard others
		sd, err = svr.GetShardByID(ctx, proto.ShardID(1))
		require.NoError(t, err)

		shardInfo, err := sd.GetMember(ctx, acapi.GetShardModeLeader, 0)
		require.NoError(t, err)
		require.Equal(t, shards[0].shardID, shardInfo.Suid.ShardID())

		shardInfo, err = sd.GetMember(ctx, acapi.GetShardModeRandom, 0)
		require.NoError(t, err)
		require.Equal(t, shards[0].shardID, shardInfo.Suid.ShardID())

		shardID := sd.GetShardID()
		require.Equal(t, shards[0].shardID, shardID)
	}

	// get first shard
	{
		sd, err := svr.GetFisrtShard(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(1), sd.GetShardID())

		newDisk, err := sd.GetMember(ctx, 0, 2)
		require.NoError(t, err)
		require.NotEqual(t, proto.DiskID(2), newDisk.DiskID)
		require.Contains(t, []proto.DiskID{1, 3}, newDisk.DiskID)
	}

	// get shard by range
	{
		sd, err := svr.GetShardByID(ctx, proto.ShardID(2))
		require.NoError(t, err)
		shardRange := sd.GetRange()

		sd, err = svr.GetShardByRange(ctx, shardRange)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(2), sd.GetShardID())
		require.Equal(t, shardRange, sd.GetRange())

		sd, err = svr.GetShardByRange(ctx, shards[0].rangeExt)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(1), sd.GetShardID())

		sd, err = svr.GetShardByRange(ctx, shards[7].rangeExt)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(8), sd.GetShardID())

		sd, err = svr.GetNextShard(ctx, shardRange)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(3), sd.GetShardID())
		require.NotEqual(t, shardRange, sd.GetRange())

		// err == nil && shard == nil, means last shard, reach end
		sd, err = svr.GetNextShard(ctx, shards[7].rangeExt)
		require.NoError(t, err)
		require.Nil(t, sd)
	}
}
