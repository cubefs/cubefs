// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package shardnode

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	"github.com/cubefs/cubefs/blobstore/shardnode/mock"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

var (
	C      = gomock.NewController
	A      = gomock.Any()
	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")

	tcpAddrBlob     = "127.0.0.1:19911"
	tcpAddrItem     = "127.0.0.1:19912"
	tcpAddrShard    = "127.0.0.1:19913"
	tcpAddrTcmalloc = "127.0.0.1:19914"

	sid     = proto.SpaceID(1)
	diskID  = uint32(200)
	shardID = proto.ShardID(1)
	suid    = proto.EncodeSuid(shardID, 0, 0)
	rg      = sharding.New(sharding.RangeType_RangeTypeHash, 1)

	fieldsMetas = []cmapi.FieldMeta{
		{
			ID:        proto.FieldID(0),
			Name:      "test_field",
			FieldType: proto.FieldTypeString,
		},
	}
	testSpace = cmapi.Space{
		SpaceID:    sid,
		Name:       "test_space",
		FieldMetas: fieldsMetas,
	}
)

func genDiskID() proto.DiskID {
	return proto.DiskID(atomic.AddUint32(&diskID, 1))
}

type mockServiceCfg struct {
	tp     *base.MockTransport
	disks  map[proto.DiskID]*storage.MockDisk
	shards map[proto.Suid]*mock.MockSpaceShardHandler
}

func newBaseTp(t *testing.T) *base.MockTransport {
	// transport with cm
	tp := allocator.NewMockAllocTransport(t).(*base.MockTransport)
	tp.EXPECT().GetAllSpaces(A).Return([]cmapi.Space{
		testSpace,
	}, nil).AnyTimes()
	return tp
}

func newMockService(t *testing.T, cfg mockServiceCfg) (*service, func(), error) {
	s := &service{}
	s.transport = cfg.tp

	sg := mock.NewMockShardGetter(C(t))
	sg.EXPECT().GetShard(A, A).DoAndReturn(
		func(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error) {
			shard, ok := cfg.shards[suid]
			if !ok {
				return nil, errors.ErrShardNotExist
			}
			return shard, nil
		},
	).AnyTimes()

	cg := catalog.NewCatalog(ctx, &catalog.Config{
		Transport:   s.transport,
		ShardGetter: sg,
	})
	s.catalog = cg
	s.taskPool = taskpool.New(1, 1)

	// set disk
	s.disks = make(map[proto.DiskID]*storage.Disk)
	for id, d := range cfg.disks {
		s.disks[id] = d.GetDisk()
	}

	clearFunc := func() {
		for _, d := range cfg.disks {
			d.Close()
		}
	}

	return s, clearFunc, nil
}

func newMockRpcServer(s *service, addr string) (*rpc2.Server, func()) {
	router := newHandler(&RpcService{
		service: s,
	})
	svr := &rpc2.Server{
		Name: "mock_server",
	}
	svr.Addresses = []rpc2.NetworkAddress{{
		Network: "tcp",
		Address: addr,
	}}
	svr.Handler = router.MakeHandler()
	shutdown := func() {
		go func() {
			svr.Shutdown(ctx)
		}()
	}
	return svr, shutdown
}

func TestRpcService_Blob(t *testing.T) {
	sh := mock.NewMockSpaceShardHandler(C(t))
	sh.EXPECT().Insert(A, A, A).Return(nil).AnyTimes()

	// blob
	blob := proto.Blob{
		Name: []byte("test_get_blob"),
		Location: proto.Location{
			SliceSize: 32,
		},
	}
	raw, _ := blob.Marshal()
	vg := mock.NewMockValGetter(C(t))
	vg.EXPECT().Value().Return(raw).AnyTimes()
	vg.EXPECT().Close().AnyTimes()
	sh.EXPECT().Get(A, A, A).Return(vg, nil).AnyTimes()

	sh.EXPECT().Delete(A, A, A).Return(nil).AnyTimes()
	sh.EXPECT().Update(A, A, A).Return(nil).AnyTimes()

	sh.EXPECT().List(A, A, A, A, A, A).
		DoAndReturn(func(
			ctx context.Context,
			h storage.OpHeader,
			prefix, marker []byte,
			count uint64,
			rangeFunc func([]byte) error,
		) (nextMarker []byte, err error) {
			rangeFunc(raw)
			return nil, nil
		}).AnyTimes()

	suid := proto.EncodeSuid(shardID, 1, 0)
	mockShards := make(map[proto.Suid]*mock.MockSpaceShardHandler)
	mockShards[suid] = sh

	s, clear, err := newMockService(t, mockServiceCfg{
		tp:     newBaseTp(t),
		disks:  nil,
		shards: mockShards,
	})
	require.Nil(t, err)
	svr, shutdown := newMockRpcServer(s, tcpAddrBlob)
	defer shutdown()
	go func() {
		clear()
		svr.Serve()
	}()
	svr.WaitServe()

	cli := shardnode.New(rpc2.Client{})

	header := shardnode.ShardOpHeader{
		SpaceID: sid,
		Suid:    suid,
	}
	// create
	name := []byte("test_blob")
	_, err = cli.CreateBlob(context.Background(), tcpAddrBlob, shardnode.CreateBlobArgs{
		Header:    header,
		Name:      name,
		CodeMode:  codemode.EC6P6,
		Size_:     192,
		SliceSize: 32,
	})
	require.Equal(t, errors.ErrBlobAlreadyExists.Error(), err.Error())

	// create with empty name, failed
	_, err = cli.CreateBlob(context.Background(), tcpAddrBlob, shardnode.CreateBlobArgs{})
	require.Equal(t, errors.ErrBlobNameEmpty.Error(), err.Error())

	// get
	getRet, err := cli.GetBlob(context.Background(), tcpAddrBlob, shardnode.GetBlobArgs{
		Header: header,
		Name:   name,
	})
	require.Nil(t, err)
	blob = getRet.Blob
	require.Equal(t, []byte("test_get_blob"), blob.GetName())

	// delete
	err = cli.DeleteBlob(context.Background(), tcpAddrBlob, shardnode.DeleteBlobArgs{
		Header: header,
		Name:   name,
	})
	require.Nil(t, err)

	getRet, err = cli.FindAndDeleteBlob(context.Background(), tcpAddrBlob, shardnode.DeleteBlobArgs{
		Header: header,
		Name:   name,
	})
	require.Nil(t, err)
	require.Equal(t, []byte("test_get_blob"), blob.GetName())

	// seal
	err = cli.SealBlob(context.Background(), tcpAddrBlob, shardnode.SealBlobArgs{
		Header: header,
		Name:   name,
		Size_:  192,
	})
	require.Nil(t, err)

	// list
	_, err = cli.ListBlob(context.Background(), tcpAddrBlob, shardnode.ListBlobArgs{
		Header: header,
		Count:  2,
	})
	require.Nil(t, err)

	// alloc slice
	_, err = cli.AllocSlice(context.Background(), tcpAddrBlob, shardnode.AllocSliceArgs{
		Header:   header,
		Name:     name,
		CodeMode: codemode.EC6P6,
		Size_:    192,
	})
	require.Nil(t, err)
}

func TestRpcService_Item(t *testing.T) {
	sh := mock.NewMockSpaceShardHandler(C(t))
	sh.EXPECT().Insert(A, A, A).Return(nil).AnyTimes()
	// item
	item := shardnode.Item{
		ID: []byte("test_item"),
		Fields: []shardnode.Field{{
			ID:    fieldsMetas[0].ID,
			Value: []byte("value"),
		}},
	}

	sh.EXPECT().GetItem(A, A, A).Return(item, nil).Times(2).AnyTimes()
	sh.EXPECT().UpdateItem(A, A, A).Return(nil).AnyTimes()
	sh.EXPECT().ListItem(A, A, A, A, A).Return([]shardnode.Item{item}, nil, nil).AnyTimes()
	sh.EXPECT().Delete(A, A, A).Return(nil).AnyTimes()

	suid := proto.EncodeSuid(shardID, 1, 0)
	mockShards := make(map[proto.Suid]*mock.MockSpaceShardHandler)
	mockShards[suid] = sh

	s, clear, err := newMockService(t, mockServiceCfg{
		tp:     newBaseTp(t),
		disks:  nil,
		shards: mockShards,
	})
	require.Nil(t, err)

	svr, shutdown := newMockRpcServer(s, tcpAddrItem)
	defer shutdown()
	go func() {
		clear()
		svr.Serve()
	}()
	svr.WaitServe()

	cli := shardnode.New(rpc2.Client{})

	header := shardnode.ShardOpHeader{
		SpaceID: sid,
		Suid:    suid,
	}
	// insert
	err = cli.AddItem(context.Background(), tcpAddrItem, shardnode.InsertItemArgs{
		Header: header,
		Item: shardnode.Item{
			ID: []byte("test_item"),
			Fields: []shardnode.Field{{
				ID:    fieldsMetas[0].ID,
				Value: []byte("value"),
			}},
		},
	})
	require.Nil(t, err)

	// insert with empty id, failed
	err = cli.AddItem(context.Background(), tcpAddrItem, shardnode.InsertItemArgs{})
	require.Equal(t, errors.ErrItemIDEmpty.Error(), err.Error())

	// get
	itm, err := cli.GetItem(context.Background(), tcpAddrItem, shardnode.GetItemArgs{
		Header: header,
		ID:     []byte("test_item"),
	})
	require.Nil(t, err)
	require.Equal(t, []byte("test_item"), itm.GetID())

	// update
	err = cli.UpdateItem(context.Background(), tcpAddrItem, shardnode.UpdateItemArgs{
		Header: header,
		Item: shardnode.Item{
			ID: []byte("test_item"),
			Fields: []shardnode.Field{{
				ID:    fieldsMetas[0].ID,
				Value: []byte("value"),
			}},
		},
	})
	require.Nil(t, err)

	// delete
	err = cli.DeleteItem(context.Background(), tcpAddrItem, shardnode.DeleteItemArgs{
		Header: header,
		ID:     []byte("test_item"),
	})
	require.Nil(t, err)

	// list
	ret, err := cli.ListItem(context.Background(), tcpAddrItem, shardnode.ListItemArgs{
		Header: header,
		Count:  2,
	})
	require.Nil(t, err)
	require.Equal(t, 1, len(ret.Items))
}

func TestRpcService_Shard(t *testing.T) {
	diskID := genDiskID()
	d, _, err := storage.NewMockDisk(t, diskID)
	require.Nil(t, err)
	disks := make(map[proto.DiskID]*storage.MockDisk)
	disks[diskID] = d

	s, clear, err := newMockService(t, mockServiceCfg{
		tp:    newBaseTp(t),
		disks: disks,
	})
	require.Nil(t, err)

	svr, shutdown := newMockRpcServer(s, tcpAddrShard)
	defer func() {
		clear()
		shutdown()
	}()

	go func() {
		svr.Serve()
	}()
	svr.WaitServe()

	cli := shardnode.New(rpc2.Client{})
	// add shard
	err = cli.AddShard(context.Background(), tcpAddrShard, shardnode.AddShardArgs{
		DiskID: diskID,
		Suid:   suid,
		Range:  *rg,
		Units: []cmapi.ShardUnit{
			{
				Suid:   suid,
				DiskID: diskID,
			},
		},
		RouteVersion: 0,
	})
	require.Nil(t, err)

	// same suid
	err = cli.AddShard(context.Background(), tcpAddrShard, shardnode.AddShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	require.Nil(t, err)

	// same shardID
	err = cli.AddShard(context.Background(), tcpAddrShard, shardnode.AddShardArgs{
		DiskID: diskID,
		Suid:   proto.EncodeSuid(suid.ShardID(), 0, 1),
	})
	require.Equal(t, errors.ErrShardConflicts.Error(), err.Error())

	// get shard
	info, err := cli.GetShardUintInfo(context.Background(), tcpAddrShard, shardnode.GetShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	require.Nil(t, err)
	require.Equal(t, diskID, info.DiskID)
	require.Equal(t, suid, info.Suid)

	// update shard
	newDiskID := diskID + 1
	newSuid := proto.EncodeSuid(suid.ShardID(), suid.Index()+1, 0)
	err = cli.UpdateShard(context.Background(), tcpAddrShard, shardnode.UpdateShardArgs{
		DiskID:          diskID,
		Suid:            suid,
		ShardUpdateType: proto.ShardUpdateTypeAddMember,
		Unit: cmapi.ShardUnit{
			DiskID: newDiskID,
			Suid:   newSuid,
		},
	})
	require.Nil(t, err)

	stats, err := cli.GetShardStats(context.Background(), tcpAddrShard, shardnode.GetShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	require.Nil(t, err)
	require.Equal(t, diskID, info.DiskID)
	require.Equal(t, suid, info.Suid)
	require.Equal(t, 2, len(stats.Units))

	// transferleader
	err = cli.TransferShardLeader(context.Background(), tcpAddrShard, shardnode.TransferShardLeaderArgs{
		DiskID:     diskID,
		Suid:       suid,
		DestDiskID: newDiskID,
	})
	require.Nil(t, err)

	// list shards
	shardRet, err := cli.ListShards(context.Background(), tcpAddrShard, shardnode.ListShardArgs{
		DiskID: diskID,
		Count:  100,
	})
	require.Nil(t, err)
	require.Equal(t, 1, len(shardRet.Shards))

	// list volume
	volRet, err := cli.ListVolume(context.Background(), tcpAddrShard, shardnode.ListVolumeArgs{
		CodeMode: codemode.EC6P6,
	})
	require.Nil(t, err)
	require.True(t, len(volRet.Vids) > 0)

	// shard db stats
	_, err = cli.DBStats(context.Background(), tcpAddrShard, shardnode.DBStatsArgs{
		DiskID: diskID,
		DBName: "kv",
	})
	require.Nil(t, err)
}

func TestRpcService_Tcmalloc(t *testing.T) {
	s, clear, err := newMockService(t, mockServiceCfg{
		tp: newBaseTp(t),
	})
	require.Nil(t, err)

	svr, shutdown := newMockRpcServer(s, tcpAddrTcmalloc)
	defer func() {
		clear()
		shutdown()
	}()

	go func() {
		svr.Serve()
	}()
	svr.WaitServe()

	cli := shardnode.New(rpc2.Client{})

	ret, err := cli.TCMallocStats(context.Background(), tcpAddrTcmalloc, shardnode.TCMallocArgs{})
	require.Nil(t, err)
	require.True(t, len(ret.Stats) > 0)

	ret, err = cli.TCMallocRate(context.Background(), tcpAddrTcmalloc, shardnode.TCMallocArgs{})
	require.Nil(t, err)
	require.True(t, len(ret.Stats) > 0)

	ret, err = cli.TCMallocFree(context.Background(), tcpAddrTcmalloc, shardnode.TCMallocArgs{})
	require.Nil(t, err)
	require.True(t, len(ret.Stats) > 0)
}

func TestRpcService_InitDisks(t *testing.T) {
	tp := newBaseTp(t)
	s, clear, err := newMockService(t, mockServiceCfg{
		tp: tp,
	})
	require.Nil(t, err)
	defer clear()

	// empty path, not register
	path1 := "/tmp/test_init_path1"
	// repairing, path cleared
	path2 := "/tmp/test_init_path2"
	// repaired but not clear
	path3 := "/tmp/test_init_path3"
	// repairing, open failed
	path4 := "/tmp/test_init_path4"

	s.cfg.DisksConfig.Disks = []string{
		path1,
		path2,
		path3,
		path4,
	}
	s.cfg.StoreConfig.KVOption.CreateIfMissing = true
	s.cfg.StoreConfig.RaftOption.CreateIfMissing = true
	s.cfg.StoreConfig.KVOption.ColumnFamily = append(s.cfg.StoreConfig.KVOption.ColumnFamily, "data")
	s.cfg.StoreConfig.RaftOption.ColumnFamily = append(s.cfg.StoreConfig.RaftOption.ColumnFamily, "raft-wal")

	defer func() {
		for _, path := range s.cfg.DisksConfig.Disks {
			os.RemoveAll(path)
		}
	}()

	for _, path := range s.cfg.DisksConfig.Disks[2:] {
		path = path + "/sys"
		err := os.MkdirAll(path, 0o755)
		require.Nil(t, err)

		filePath := path + "/Disk.meta"
		file, err := os.Create(filePath)
		require.Nil(t, err)
		file.Close()
	}

	tp.EXPECT().ListDisks(A).Return([]cmapi.ShardNodeDiskInfo{
		{
			DiskInfo: cmapi.DiskInfo{
				Path:   path2,
				Status: proto.DiskStatusRepairing,
			},
		},
		{
			DiskInfo: cmapi.DiskInfo{
				Path:   path3,
				Status: proto.DiskStatusRepairing,
			},
		},
		{
			DiskInfo: cmapi.DiskInfo{
				Path:   path4,
				Status: proto.DiskStatusRepairing,
			},
		},
	}, nil)
	tp.EXPECT().NodeID().Return(proto.NodeID(1)).Times(4)
	err = s.initDisks(ctx)
	require.NotNil(t, err)
	require.True(t, strings.Contains(err.Error(), "disk device has been replaced"))
}
