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

	s, err := NewShardController(shardCtrlConf{}, cmCli, stopCh)
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
				version:      1,
				units:        nil,
			}
			sd.rangeExt = *ranges[i]
			shards[i] = sd
		}

		for i := 0; i < 8; i++ {
			svr.addShardNoLock(shards[i])
		}
		require.Equal(t, 8, len(svr.shards))

		ret, err := svr.GetShard(ctx, [][]byte{[]byte("blob1__xxx")}) // expect 2 keys
		sk := [][]byte{[]byte("blob1__xxx")}
		bd := sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, sk).GetBoundary()
		t.Logf("shard key 1,  get boundary=%d", bd)
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

		// update shard
		newShard := *si
		newShard.version++
		err = svr.UpdateShard(ctx, shardnode.ShardStats{
			Suid:         proto.EncodeSuid(newShard.shardID, 1, 2),
			LeaderDiskID: newShard.leaderDiskID,
			RouteVersion: newShard.version,
			Range:        newShard.rangeExt,
			Units:        newShard.units,
		})
		require.NoError(t, err)
		require.NotEqual(t, svr.version, si.version)
		// require.Equal(t, svr.version, newShard.version)

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
		// add shard id=9
		const shardID = 9
		const version = 3
		val := clustermgr.CatalogChangeShardAdd{
			ShardID:      shardID,
			RouteVersion: version,
			Units: []clustermgr.ShardUnitInfo{
				{
					Suid:         proto.EncodeSuid(shardID, 0, 0),
					DiskID:       1,
					AppliedIndex: 0,
					LeaderDiskID: 1,
					Range:        *ranges[8],
					RouteVersion: version,
					Host:         "testHost1",
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

		require.Equal(t, 1, len(svr.shards))
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
		require.Equal(t, 2, len(svr.shards))

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
					Type: proto.CatalogChangeItemUpdateShard,
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

		require.Equal(t, 2, len(svr.shards))
	}
}

func TestShardGetShard(t *testing.T) {
	ctx := context.Background()
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
	}
	// add 8 shard
	shards := make([]*shard, 8)
	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, 8)

	for i := 0; i < 8; i++ {
		sd := &shard{
			shardID:      proto.ShardID(i + 1),
			version:      1,
			leaderDiskID: 1,
			units: []clustermgr.ShardUnit{
				{
					Suid:    proto.EncodeSuid(proto.ShardID(i+1), 0, 0),
					DiskID:  1,
					Learner: true,
					Host:    "testHost1",
				},
				{
					Suid:    proto.EncodeSuid(proto.ShardID(i+1), 1, 0),
					DiskID:  2,
					Learner: true,
					Host:    "testHost2",
				},
				{
					Suid:    proto.EncodeSuid(proto.ShardID(i+1), 2, 0),
					DiskID:  3,
					Learner: true,
					Host:    "testHost3",
				},
			},
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

		shardInfo := sd.GetMember(acapi.GetShardModeLeader, 0)
		require.Equal(t, shards[0].shardID, shardInfo.Suid.ShardID())

		shardInfo = sd.GetMember(acapi.GetShardModeRandom, 0)
		require.Equal(t, shards[0].shardID, shardInfo.Suid.ShardID())

		shardID := sd.GetShardID()
		require.Equal(t, shards[0].shardID, shardID)
	}

	// get first shard
	{
		sd, err := svr.GetFisrtShard(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(1), sd.GetShardID())

		newDisk := sd.GetMember(0, 2)
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
