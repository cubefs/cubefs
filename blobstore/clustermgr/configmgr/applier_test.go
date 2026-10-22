// Copyright 2022 The CubeFS Authors.
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

package configmgr

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockclustermgr"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestConfigMgr_Others(t *testing.T) {
	testDir, err := os.MkdirTemp("", "cf")
	defer os.RemoveAll(testDir)
	require.NoError(t, err)

	ctx := context.Background()
	ctr := gomock.NewController(t)
	mockKvMgr := mock.NewMockKvMgrAPI(ctr)

	cfMap := map[string]interface{}{
		"forbid_sync_config": false,
	}

	configmgr, err := New(mockKvMgr, cfMap)
	require.NoError(t, err)

	testModuleName := "configMgr"
	configmgr.SetModuleName(testModuleName)
	module := configmgr.GetModuleName()
	require.Equal(t, testModuleName, module)

	err = configmgr.LoadData(ctx)
	require.NoError(t, err)

	configmgr.SetRaftServer(nil)

	err = configmgr.Flush(ctx)
	require.NoError(t, err)

	configmgr.NotifyLeaderChange(ctx, 0, "")

	// cover Get path when key is not in DB or default config
	mockKvMgr.EXPECT().Get("unknown_key").Return(nil, errors.New("not found"))
	_, err = configmgr.Get(ctx, "unknown_key")
	require.Error(t, err)
}

func TestConfigMgr_Apply(t *testing.T) {
	testDir, err := os.MkdirTemp("", "cf")
	defer os.RemoveAll(testDir)
	require.NoError(t, err)

	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	ctr := gomock.NewController(t)
	mockKvMgr := mock.NewMockKvMgrAPI(ctr)
	mockKvMgr.EXPECT().Set(gomock.Any(), gomock.Any()).Return(nil)
	mockKvMgr.EXPECT().Delete(gomock.Any()).Return(nil)

	cfMap := map[string]interface{}{
		"forbid_sync_config": false,
	}

	configmgr, err := New(mockKvMgr, cfMap)
	require.NoError(t, err)

	// OperTypeSetConfig error
	{
		operTypes := make([]int32, 0)
		datas := make([][]byte, 0)
		ctxs := make([]base.ProposeContext, 0)
		data := []byte("-1")
		operTypes = append(operTypes, OperTypeSetConfig)
		datas = append(datas, data)
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
		err = configmgr.Apply(ctx, operTypes, datas, ctxs)
		require.Error(t, err)
	}

	// OperTypeDeleteConfig error
	{
		operTypes := make([]int32, 0)
		datas := make([][]byte, 0)
		ctxs := make([]base.ProposeContext, 0)
		data := []byte("-1")
		operTypes = append(operTypes, OperTypeDeleteConfig)
		datas = append(datas, data)
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
		err = configmgr.Apply(ctx, operTypes, datas, ctxs)
		require.Error(t, err)
	}

	// OperTypeDeleteConfig error
	{
		operTypes := make([]int32, 0)
		datas := make([][]byte, 0)
		ctxs := make([]base.ProposeContext, 0)
		data := []byte("-1")
		operTypes = append(operTypes, 3)
		datas = append(datas, data)
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
		err = configmgr.Apply(ctx, operTypes, datas, ctxs)
		require.Error(t, err)
	}

	operTypes := make([]int32, 0)
	datas := make([][]byte, 0)
	ctxs := make([]base.ProposeContext, 0)

	// OperTypeSetConfig
	{
		b, _ := json.Marshal(true)
		data, err := json.Marshal(&clustermgr.ConfigSetArgs{
			Key:   "forbid_sync_config",
			Value: string(b),
		})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeSetConfig)
		datas = append(datas, data)
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
	}

	// OperTypeDeleteConfig
	{
		data, err := json.Marshal(&clustermgr.ConfigArgs{
			Key: "forbid_sync_config",
		})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeDeleteConfig)
		datas = append(datas, data)
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
	}

	err = configmgr.Apply(ctx, operTypes, datas, ctxs)
	require.NoError(t, err)

	// OperTypeSetConfig: Set returns error
	{
		ctr2 := gomock.NewController(t)
		mockKvMgr2 := mock.NewMockKvMgrAPI(ctr2)
		mockKvMgr2.EXPECT().Set(gomock.Any(), gomock.Any()).Return(errors.New("set error"))
		cm2, err := New(mockKvMgr2, cfMap)
		require.NoError(t, err)
		data, _ := json.Marshal(&clustermgr.ConfigSetArgs{Key: "forbid_sync_config", Value: "true"})
		err = cm2.Apply(ctx, []int32{OperTypeSetConfig}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.Error(t, err)
	}

	// OperTypeDeleteConfig: Delete returns error
	{
		ctr3 := gomock.NewController(t)
		mockKvMgr3 := mock.NewMockKvMgrAPI(ctr3)
		mockKvMgr3.EXPECT().Delete(gomock.Any()).Return(errors.New("delete error"))
		cm3, err := New(mockKvMgr3, cfMap)
		require.NoError(t, err)
		data, _ := json.Marshal(&clustermgr.ConfigArgs{Key: "forbid_sync_config"})
		err = cm3.Apply(ctx, []int32{OperTypeDeleteConfig}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.Error(t, err)
	}
}
