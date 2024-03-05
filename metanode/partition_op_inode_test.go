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
	"testing"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func prepareInodeForInodeTest(t *testing.T, mp MetaPartition, mode uint32) (resp *proto.CreateInodeResponse) {
	p := &Packet{}
	req := &proto.CreateInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Mode:        mode,
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

	cnt, err := mp.GetInodeRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

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
	err := mp.ExtentAppendWithCheck(req, p)
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

	cnt, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)
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
}
