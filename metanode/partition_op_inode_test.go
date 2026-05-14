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

package metanode

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/timeutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInodeGet(t *testing.T) {
	initMp(t, proto.StoreModeMem)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inoId := uint64(time.Now().Unix())
	ino := NewInode(inoId, 0)
	ino.AccessTime = time.Now().Unix() - 3600
	mp.inodeTree.ReplaceOrInsert(handle, ino, false)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	req := &InodeGetReq{
		Inode: inoId,
	}

	now := time.Now()
	time.Sleep(time.Second * 2)

	pkt := &Packet{}
	err = mp.InodeGet(req, pkt)

	require.True(t, pkt.ResultCode == proto.OpOk)
	require.NoError(t, err)

	resp := &proto.InodeGetResponse{}

	err = json.Unmarshal(pkt.Data, resp)
	require.NoError(t, err)

	t.Logf("now %s, atime %s", now.String(), resp.Info.AccessTime.String())

	require.True(t, resp.Info.AccessTime.After(now))

	req.InnerReq = true
	err = mp.InodeGet(req, pkt)
	require.NoError(t, err)
	err = json.Unmarshal(pkt.Data, resp)
	require.NoError(t, err)
	require.True(t, resp.Info.AccessTime.Unix() == ino.AccessTime)
}

func TestInodeGetPerf(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	cnt := 10240000
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	for idx := 1; idx < cnt; idx++ {
		ino := NewInode(uint64(idx), 0)
		ino.PoolId = proto.DefaultSSDPoolId
		mp.inodeTree.ReplaceOrInsert(handle, ino, true)
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	testNum := 1024
	for i := 1; i < 10; i++ {
		ids := make([]uint64, 0, testNum)
		for i := 0; i < testNum; i++ {
			ids = append(ids, rand.Uint64()%uint64(testNum)+1)
		}

		start := time.Now()
		for _, id := range ids {
			ino := NewInode(id, 0)
			ino.PoolId = proto.DefaultSSDPoolId
			item, _ := mp.inodeTree.CopyGet(ino)
			require.NotNil(t, item)
			newIno := item
			newIno.AccessTime = timeutil.GetCurrentTimeUnix()
		}
		t.Logf("TestInodeGetPerf: cnt %d, cost %dus", testNum, time.Since(start).Microseconds())
	}
}

func prepareInodeForInodeTest(t *testing.T, mp MetaPartition, mode uint32) (resp *proto.CreateInodeResponse) {
	p := &Packet{}
	req := &proto.CreateInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Mode:        mode,
		StorageType: proto.StorageClass_Replica_SSD,
		PoolId:      proto.DefaultSSDPoolId,
	}
	err := mp.CreateInode(req, p, "")
	require.NoError(t, err)

	resp = &proto.CreateInodeResponse{}
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpCreateInode(t *testing.T, mp MetaPartition) {
	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	require.EqualValues(t, FileModeType, resp.Info.Mode)
	require.EqualValues(t, 1, resp.Info.Nlink)

	resp = prepareInodeForInodeTest(t, mp, DirModeType)
	require.EqualValues(t, DirModeType, resp.Info.Mode)
	require.EqualValues(t, 2, resp.Info.Nlink)
}

func TestOpCreateInode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	testOpCreateInode(t, mp)
}

func TestOpCreateInode_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	testOpCreateInode(t, mp)
	os.RemoveAll(mp.config.RocksDBDir)
}

func linkForInodeTest(t *testing.T, mp MetaPartition, ino uint64) (resp *proto.LinkInodeResponse) {
	p := &Packet{}
	req := &proto.LinkInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
	}
	resp = &proto.LinkInodeResponse{}
	err := mp.CreateInodeLink(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpLinkInode(t *testing.T, mp MetaPartition) {
	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	require.EqualValues(t, FileModeType, resp.Info.Mode)
	require.EqualValues(t, 1, resp.Info.Nlink)

	linkResp := linkForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 2, linkResp.Info.Nlink)

	resp = prepareInodeForInodeTest(t, mp, DirModeType)
	require.EqualValues(t, DirModeType, resp.Info.Mode)
	require.EqualValues(t, 2, resp.Info.Nlink)

	linkResp = linkForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 3, linkResp.Info.Nlink)
}

func TestOpLinkInode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	testOpLinkInode(t, mp)
}

func TestOpLinkInode_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	testOpLinkInode(t, mp)
	os.RemoveAll(mp.config.RocksDBDir)
}

func unlinkInodeForInodeTest(t *testing.T, mp MetaPartition, ino uint64) (resp *proto.UnlinkInodeResponse) {
	p := &Packet{}
	req := &proto.UnlinkInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
	}
	resp = &proto.UnlinkInodeResponse{}
	err := mp.UnlinkInode(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpUnlinkInode(t *testing.T, mp MetaPartition) {
	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	require.EqualValues(t, FileModeType, resp.Info.Mode)
	require.EqualValues(t, 1, resp.Info.Nlink)

	linkResp := linkForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 2, linkResp.Info.Nlink)

	unlkinkResp := unlinkInodeForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 1, unlkinkResp.Info.Nlink)

	unlkinkResp = unlinkInodeForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 0, unlkinkResp.Info.Nlink)

	snap, err := mp.GetSnapShot()
	if err != nil {
		return
	}
	defer snap.Close()
	cnt := 0
	err = snap.Range(InodeType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	resp = prepareInodeForInodeTest(t, mp, DirModeType)
	require.EqualValues(t, DirModeType, resp.Info.Mode)
	require.EqualValues(t, 2, resp.Info.Nlink)

	unlkinkResp = unlinkInodeForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 0, unlkinkResp.Info.Nlink)
}

func TestOpUnlinkInode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	testOpUnlinkInode(t, mp)
}

func TestOpUnlinkInode_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	testOpUnlinkInode(t, mp)
	os.RemoveAll(mp.config.RocksDBDir)
}

func getExtentsForInodeTest(t *testing.T, mp MetaPartition, ino uint64) (resp *proto.GetExtentsResponse) {
	p := &Packet{}
	req := &proto.GetExtentsRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
	}
	resp = &proto.GetExtentsResponse{}
	err := mp.ExtentsList(req, p)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, p.ResultCode)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func appendExtentForInodeTest(t *testing.T, mp MetaPartition, ino uint64, extent proto.ExtentKey) (status uint8) {
	p := &Packet{}
	req := &proto.AppendExtentKeyWithCheckRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Extent:      extent,
	}
	err := mp.ExtentAppendWithCheck(req, p, "")
	require.NoError(t, err)
	status = p.ResultCode
	return
}

func testOpAppendInode(t *testing.T, mp MetaPartition) {
	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	require.EqualValues(t, FileModeType, resp.Info.Mode)
	require.EqualValues(t, 1, resp.Info.Nlink)
	ek := proto.ExtentKey{
		PartitionId: 0,
		ExtentId:    0,
		FileOffset:  0,
		Size:        util.MB,
	}
	status := appendExtentForInodeTest(t, mp, resp.Info.Inode, ek)
	require.EqualValues(t, proto.OpOk, status)
	extResp := getExtentsForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 1, len(extResp.Extents))

	ek = proto.ExtentKey{
		PartitionId: 0,
		ExtentId:    1,
		FileOffset:  2 * util.MB,
		Size:        util.MB,
	}
	status = appendExtentForInodeTest(t, mp, resp.Info.Inode, ek)
	require.EqualValues(t, proto.OpOk, status)
	extResp = getExtentsForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 2, len(extResp.Extents))

	// NOTE: random write to hole
	ek = proto.ExtentKey{
		PartitionId: 0,
		ExtentId:    2,
		FileOffset:  util.MB,
		Size:        util.MB,
	}
	status = appendExtentForInodeTest(t, mp, resp.Info.Inode, ek)
	require.EqualValues(t, proto.OpOk, status)
	extResp = getExtentsForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 3, len(extResp.Extents))
}

func TestOpAppendInode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	testOpAppendInode(t, mp)
}

func TestOpAppendInode_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	testOpAppendInode(t, mp)
	os.RemoveAll(mp.config.RocksDBDir)
}

func testOpUnlinkFile(t *testing.T, mp MetaPartition) {
	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	require.EqualValues(t, FileModeType, resp.Info.Mode)
	require.EqualValues(t, 1, resp.Info.Nlink)
	ek := proto.ExtentKey{
		PartitionId: 0,
		ExtentId:    0,
		FileOffset:  0,
		Size:        util.MB,
	}
	status := appendExtentForInodeTest(t, mp, resp.Info.Inode, ek)
	require.EqualValues(t, proto.OpOk, status)
	extResp := getExtentsForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 1, len(extResp.Extents))

	unlinkResp := unlinkInodeForInodeTest(t, mp, resp.Info.Inode)
	require.EqualValues(t, 0, unlinkResp.Info.Nlink)
	require.EqualValues(t, resp.Info.PoolId, unlinkResp.Info.PoolId)
}

func TestOpUnlinkFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	testOpUnlinkFile(t, mp)
}

func TestOpUnlinkFile_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	testOpUnlinkFile(t, mp)
	os.RemoveAll(mp.config.RocksDBDir)
}

func TestOpUpdateExtentKeyAfterMigrationRejectsGenerationMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	mp.SetEnableAuditLog(true)

	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	req := &proto.UpdateExtentKeyAfterMigrationRequest{
		PartitionID:      mp.GetBaseConfig().PartitionId,
		Inode:            resp.Info.Inode,
		LeaseExpire:      0,
		Generation:       resp.Info.Generation + 1,
		StorageClass:     proto.StorageClass_Replica_HDD,
		PoolId:           proto.DefaultHDDPoolId,
		NewObjExtentKeys: nil,
	}
	p := &Packet{}

	err := mp.UpdateExtentKeyAfterMigration(req, p, "127.0.0.1")
	require.NoError(t, err)
	require.EqualValues(t, proto.OpLeaseGenerationNotMatch, p.ResultCode)
}

func TestOpAppendExtentWithAuditLogForMigration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	mp.SetEnableAuditLog(true)

	resp := prepareInodeForInodeTest(t, mp, FileModeType)
	req := &proto.AppendExtentKeyWithCheckRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       resp.Info.Inode,
		IsMigration: true,
		PoolId:      proto.DefaultSSDPoolId,
		Extent: proto.ExtentKey{
			PartitionId: 1,
			ExtentId:    1,
			FileOffset:  0,
			Size:        util.MB,
		},
	}
	p := &Packet{}
	err := mp.ExtentAppendWithCheck(req, p, "127.0.0.1")
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, p.ResultCode)
}

func TestUpdateInodeMetaSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	const ino = 30001
	prepareInodeForFsmInodeTest(t, mp, ino)

	pkt := &Packet{}
	err := mp.UpdateInodeMeta(&proto.UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	}, pkt)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, pkt.ResultCode)
}

func TestUpdateInodeMetaSuccess_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeRocksDb)
	const ino = 30002
	prepareInodeForFsmInodeTest(t, mp, ino)
	before, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, before)

	pkt := &Packet{}
	err = mp.UpdateInodeMeta(&proto.UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	}, pkt)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, pkt.ResultCode)

	after, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, after)
	require.EqualValues(t, before.Generation+1, after.Generation)
}

func TestUpdateInodeMetaInodeNotExist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)

	pkt := &Packet{}
	err := mp.UpdateInodeMeta(&proto.UpdateInodeMetaRequest{
		Inode:       99999,
		PartitionID: mp.config.PartitionId,
	}, pkt)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpNotExistErr, pkt.ResultCode)
}

func TestUpdateInodeMetaMarkedDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	const ino = 30003
	prepareInodeForFsmInodeTest(t, mp, ino)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, inode)
	inode.Flag |= DeleteMarkFlag
	err = mp.inodeTree.Update(handle, inode)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	pkt := &Packet{}
	err = mp.UpdateInodeMeta(&proto.UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	}, pkt)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpNotExistErr, pkt.ResultCode)
}

func TestUpdateInodeMetaSubmitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	raft := raftstoremock.NewMockPartition(ctrl)
	raft.EXPECT().Submit(gomock.Any()).Return(nil, fmt.Errorf("raft submit failed")).AnyTimes()
	raft.EXPECT().IsRaftLeader().Return(true).AnyTimes()
	raft.EXPECT().LeaderTerm().Return(uint64(1), uint64(1)).AnyTimes()
	mp.raftPartition = raft

	pkt := &Packet{}
	err := mp.UpdateInodeMeta(&proto.UpdateInodeMetaRequest{
		Inode:       1,
		PartitionID: mp.config.PartitionId,
	}, pkt)
	require.Error(t, err)
	require.EqualValues(t, proto.OpAgain, pkt.ResultCode)
}
