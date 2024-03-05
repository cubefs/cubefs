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

	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func prepareInodeForDentryTest(t *testing.T, mp MetaPartition, mode uint32) (resp *proto.CreateInodeResponse) {
	p := &Packet{}
	req := &proto.CreateInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Mode:        mode,
	}
	resp = &proto.CreateInodeResponse{}
	err := mp.CreateInode(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func getDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string) (resp *proto.LookupResponse) {
	p := &Packet{}
	req := &proto.LookupRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
	}
	resp = &proto.LookupResponse{}
	err := mp.Lookup(req, p)
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func createDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string, ino uint64) {
	p := &Packet{}
	req := &proto.CreateDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
		Inode:       ino,
	}
	err := mp.CreateDentry(req, p, "")
	require.NoError(t, err)
}

func getInodeForDentryTest(t *testing.T, mp MetaPartition, ino uint64) (resp *proto.InodeGetResponse) {
	p := &Packet{}
	req := &proto.InodeGetRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
	}
	resp = &proto.InodeGetResponse{}
	err := mp.InodeGet(req, p)
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpCreateDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	resp := getDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")
	require.EqualValues(t, fileIno.Info.Inode, resp.Inode)

	getResp := getInodeForDentryTest(t, mp, dirIno.Info.Inode)
	require.EqualValues(t, 3, getResp.Info.Nlink)
}

func TestOpCreateDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testOpCreateDentry(t, mp)
}

func TestOpCreateDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testOpCreateDentry(t, mp)
}

func deleteDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string) (resp *proto.DeleteDentryResponse) {
	p := &Packet{}
	req := &proto.DeleteDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
	}
	resp = &proto.DeleteDentryResponse{}
	err := mp.DeleteDentry(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpDeleteDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	deleteDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")

	getResp := getInodeForDentryTest(t, mp, dirIno.Info.Inode)
	require.EqualValues(t, 2, getResp.Info.Nlink)
}

func TestOpDeleteDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testOpDeleteDentry(t, mp)
}

func TestOpDeleteDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testOpDeleteDentry(t, mp)
}

func updateDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string, newIno uint64) (resp *proto.UpdateDentryResponse) {
	p := &Packet{}
	req := &proto.UpdateDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
		Inode:       newIno,
	}
	resp = &proto.UpdateDentryResponse{}
	err := mp.UpdateDentry(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testUpdateDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)
	otherFileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	updateDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", otherFileIno.Info.Inode)

	getResp := getDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")
	require.EqualValues(t, otherFileIno.Info.Inode, getResp.Inode)
}

func TestUpdateDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testUpdateDentry(t, mp)
}

func TestUpdateDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testUpdateDentry(t, mp)
}
