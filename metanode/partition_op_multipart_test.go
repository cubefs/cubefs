// Copyright 2018 The CubeFS Authors.
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

func prepareMultipartForMultipartTest(t *testing.T, mp MetaPartition, path string, extend map[string]string) (resp *proto.CreateMultipartResponse) {
	p := &Packet{}
	req := &proto.CreateMultipartRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Path:        path,
		Extend:      extend,
	}
	err := mp.CreateMultipart(req, p)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, p.ResultCode)
	resp = &proto.CreateMultipartResponse{}
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func getMultipartForMultipartTest(t *testing.T, mp MetaPartition, id, path string) (resp *proto.GetMultipartResponse) {
	p := &Packet{}
	req := &proto.GetMultipartRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		Path:        path,
		MultipartId: id,
	}
	err := mp.GetMultipart(req, p)
	require.NoError(t, err)
	if p.ResultCode != proto.OpOk {
		require.EqualValues(t, proto.OpNotExistErr, p.ResultCode)
		return
	}
	resp = &proto.GetMultipartResponse{}
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpCreateMultipart(t *testing.T, mp MetaPartition) {
	extend := make(map[string]string)
	extend["Hello"] = "World"
	resp := prepareMultipartForMultipartTest(t, mp, "/test", extend)

	getResp := getMultipartForMultipartTest(t, mp, resp.Info.ID, "/test")
	require.Equal(t, extend, getResp.Info.Extend)
}

func TestOpCreateMultipart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmMultipartTest(t, ctrl, proto.StoreModeMem)
	testOpCreateMultipart(t, mp)
}

func TestOpCreateMultipart_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmMultipartTest(t, ctrl, proto.StoreModeRocksDb)
	testOpCreateMultipart(t, mp)
}

func removeMultipartForMultipartTest(t *testing.T, mp MetaPartition, id, path string) {
	p := &Packet{}
	req := &proto.RemoveMultipartRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionId: mp.GetBaseConfig().PartitionId,
		MultipartId: id,
		Path:        path,
	}
	err := mp.RemoveMultipart(req, p)
	require.NoError(t, err)
	if p.ResultCode != proto.OpOk {
		require.EqualValues(t, proto.OpNotExistErr, p.ResultCode)
		return
	}
	return
}

func testOpRemoveMultipart(t *testing.T, mp MetaPartition) {
	extend := make(map[string]string)
	extend["Hello"] = "World"
	resp := prepareMultipartForMultipartTest(t, mp, "/test", extend)
	removeMultipartForMultipartTest(t, mp, resp.Info.ID, resp.Info.Path)

	getResp := getMultipartForMultipartTest(t, mp, resp.Info.ID, resp.Info.Path)
	require.Nil(t, getResp)
}

func TestOpRemoveMultipart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmMultipartTest(t, ctrl, proto.StoreModeMem)
	testOpRemoveMultipart(t, mp)
}

func TestOpRemoveMultipart_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmMultipartTest(t, ctrl, proto.StoreModeRocksDb)
	testOpRemoveMultipart(t, mp)
}
