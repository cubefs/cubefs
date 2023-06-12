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
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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

	err = configmgr.Flush(ctx)
	require.NoError(t, err)

	configmgr.NotifyLeaderChange(ctx, 0, "")
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
}
