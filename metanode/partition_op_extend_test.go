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
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func prepareInodeForExtendTest(t *testing.T, mp MetaPartition) (ino uint64) {
	p := &Packet{}
	req := &CreateInoReq{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Mode:        FileModeType,
	}
	err := mp.CreateInode(req, p, "")
	require.NoError(t, err)
	resp := &CreateInoResp{}
	err = json.Unmarshal(p.Data, resp)
	require.NoError(t, err)
	ino = resp.Info.Inode
	return
}

func checkXattrForExtendTest(t *testing.T, mp MetaPartition, ino uint64, key string, expectedValue string) {
	p := &Packet{}
	req := &proto.GetXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         key,
	}
	err := mp.GetXAttr(req, p)
	require.NoError(t, err)
	resp := &proto.GetXAttrResponse{}
	err = json.Unmarshal(p.Data, resp)
	require.NoError(t, err)
	require.EqualValues(t, expectedValue, resp.Value)
}

func checkXattrNotExistsForExtendTest(t *testing.T, mp MetaPartition, ino uint64, key string) {
	p := &Packet{}
	req := &proto.GetXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         key,
	}
	err := mp.GetXAttr(req, p)
	require.NoError(t, err)
	resp := &proto.GetXAttrResponse{}
	err = json.Unmarshal(p.Data, resp)
	require.NoError(t, err)
	require.Empty(t, resp.Value)
}

func testSetXAttr(t *testing.T, mp MetaPartition) {
	ino := prepareInodeForExtendTest(t, mp)
	p := &Packet{}
	req := &proto.SetXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         "Key",
		Value:       "Value",
	}
	err := mp.SetXAttr(req, p)
	require.NoError(t, err)
	checkXattrForExtendTest(t, mp, ino, "Key", "Value")
}

func TestSetXAttr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeMem)
	testSetXAttr(t, mp)
}

func TestSetXAttr_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeRocksDb)
	testSetXAttr(t, mp)
}

func testRemoveXAttr(t *testing.T, mp MetaPartition) {
	ino := prepareInodeForExtendTest(t, mp)
	p := &Packet{}
	req := &proto.SetXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         "Key",
		Value:       "Value",
	}
	err := mp.SetXAttr(req, p)
	require.NoError(t, err)
	checkXattrForExtendTest(t, mp, ino, "Key", "Value")

	delReq := &proto.RemoveXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         "Key",
	}
	err = mp.RemoveXAttr(delReq, p)
	require.NoError(t, err)
	checkXattrNotExistsForExtendTest(t, mp, ino, "Key")
}

func TestRemoveXAttr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeMem)
	testRemoveXAttr(t, mp)
}

func TestRemoveXAttr_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeRocksDb)
	testRemoveXAttr(t, mp)
}

func testUpdateXAttr(t *testing.T, mp MetaPartition) {
	ino := prepareInodeForExtendTest(t, mp)
	p := &Packet{}
	req := &proto.SetXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         "Key",
		Value:       "0,0,0",
	}
	err := mp.SetXAttr(req, p)
	require.NoError(t, err)
	checkXattrForExtendTest(t, mp, ino, "Key", "0,0,0")

	updateReq := &proto.UpdateXAttrRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
		Key:         "Key",
		Value:       "1,1,1",
	}
	err = mp.UpdateXAttr(updateReq, p)
	require.NoError(t, err)
	checkXattrForExtendTest(t, mp, ino, "Key", "1,1,1")
}

func TestUpdateXAttr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeMem)
	testUpdateXAttr(t, mp)
}

func TestUpdateXAttr_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForFsmExtendTest(t, mockCtrl, proto.StoreModeRocksDb)
	testUpdateXAttr(t, mp)
}
