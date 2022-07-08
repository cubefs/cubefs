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

package base

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func BenchmarkEncodeProposeInfo(b *testing.B) {
	module := "testModule"
	operType := int32(1)
	data := []byte(strings.Repeat("a", 300))
	ctx := ProposeContext{ReqID: "aaa"}

	for i := 0; i < b.N; i++ {
		EncodeProposeInfo(module, operType, data, ctx)
	}
}

func BenchmarkDecodeProposeInfo(b *testing.B) {
	module := "testModule"
	operType := int32(1)
	data := []byte(strings.Repeat("a", 300))
	ctx := ProposeContext{ReqID: "aaa"}
	proposeInfo := EncodeProposeInfo(module, operType, data, ctx)

	for i := 0; i < b.N; i++ {
		DecodeProposeInfo(proposeInfo)
	}
}

func TestDecodeProposeInfo(t *testing.T) {
	module := "testModule"
	operType := int32(1)
	data := []byte(strings.Repeat("a", 300))
	ctx := ProposeContext{ReqID: "aaa"}
	proposeInfo := EncodeProposeInfo(module, operType, data, ctx)
	t.Log("proposeInfo length: ", len(proposeInfo))

	decodeProposeInfo := DecodeProposeInfo(proposeInfo)
	t.Log(decodeProposeInfo.Module)
	t.Log(decodeProposeInfo.OperType)
	t.Log(decodeProposeInfo.Context.ReqID)

	assert.Equal(t, module, decodeProposeInfo.Module)
	assert.Equal(t, operType, decodeProposeInfo.OperType)
	assert.Equal(t, data, decodeProposeInfo.Data)
	assert.Equal(t, ctx.ReqID, decodeProposeInfo.Context.ReqID)
}

func TestRaftNode(t *testing.T) {
	tmpDBPath := "/tmp/tmpraftdb" + strconv.Itoa(rand.Intn(1000000000))
	os.MkdirAll(tmpDBPath, 0o755)
	defer os.RemoveAll(tmpDBPath)
	raftDB, err := raftdb.OpenRaftDB(tmpDBPath, false)
	assert.NoError(t, err)
	defer raftDB.Close()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaftSever := mocks.NewMockRaftServer(ctrl)
	mockRaftSever.EXPECT().Stop().Return()
	mockRaftSever.EXPECT().Truncate(gomock.Any()).Return(nil)

	raftNode, err := NewRaftNode(&RaftNodeConfig{
		FlushNumInterval: 1, TruncateNumInterval: 1, ApplyIndex: 1,
		Members: []RaftMember{{ID: 1, Host: "127.0.0.1", Learner: false, NodeHost: "127.0.0.1"}},
	}, raftDB, nil)
	assert.NoError(t, err)
	raftNode.nodes[1] = "127.0.0.1"
	raftNode.SetRaftServer(mockRaftSever)
	defer raftNode.Stop()
	go raftNode.Start()

	moduleName := "TestModule"

	// RegistRaftApplier
	{
		type testTarget struct {
			TestModule RaftApplier
		}

		applier := NewMockRaftApplier(ctrl)
		applier.EXPECT().Apply(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		applier.EXPECT().Flush(gomock.Any()).MaxTimes(1000000).Return(nil)
		applier.EXPECT().NotifyLeaderChange(gomock.Any(), gomock.Any(), gomock.Any()).Return()
		applier.EXPECT().SetModuleName(moduleName).Return()
		applier.EXPECT().GetModuleName().Return(moduleName)

		raftNode.RegistRaftApplier(&testTarget{TestModule: applier})
	}

	// apply index
	{
		index := raftNode.GetStableApplyIndex()
		assert.Equal(t, uint64(1), index)

		err = raftNode.RecordApplyIndex(ctx, uint64(2), false)
		assert.NoError(t, err)

		err = raftNode.RecordApplyIndex(ctx, uint64(2), true)
		assert.NoError(t, err)
	}

	{
		host := "127.0.0.1"
		raftNode.NotifyLeaderChange(ctx, uint64(1), "")
		err := raftNode.ModuleApply(ctx, moduleName, []int32{1}, [][]byte{}, []ProposeContext{})
		assert.NoError(t, err)
		raftNode.SetLeaderHost(1, host)
		getHost := raftNode.GetLeaderHost()
		assert.Equal(t, host, getHost)
	}

	{
		err = raftNode.RecordApplyIndex(ctx, uint64(3), true)
		assert.NoError(t, err)
		err = raftNode.RecordApplyIndex(ctx, uint64(4), false)
		assert.NoError(t, err)
		err = raftNode.RecordApplyIndex(ctx, uint64(5), false)
		assert.NoError(t, err)

		// wait for raftNode background flush start
		time.Sleep(time.Duration(defaultFlushCheckIntervalS+1) * time.Second)
	}
}
