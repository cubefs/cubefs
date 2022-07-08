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

package scopemgr

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"testing"

	base_ "github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestScopeMgr(t *testing.T) {
	tmpDBPath := "/tmp/tmpnormaldb" + strconv.Itoa(rand.Intn(10000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := normaldb.OpenNormalDB(tmpDBPath, false)
	assert.NoError(t, err)
	defer db.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftServer := mocks.NewMockRaftServer(ctrl)
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	scopeMgr, err := NewScopeMgr(db)
	assert.NoError(t, err)
	scopeMgr.SetRaftServer(mockRaftServer)

	name1 := "testname1"
	// test alloc: successful case
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
			return nil
		})
		count := 10

		base, new, err := scopeMgr.Alloc(ctx, name1, count)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), base)
		assert.Equal(t, uint64(10), new)

		data, _ := json.Marshal(&allocCtx{Name: name1, Current: 10})

		scopeMgr.Apply(ctx, []int32{OperTypeAllocScope}, [][]byte{data}, []base_.ProposeContext{{ReqID: span.TraceID()}})
		current := scopeMgr.GetCurrent(name1)
		assert.Equal(t, uint64(count), current)
	}

	// test alloc: current continue from 10
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil)

		base, new, err := scopeMgr.Alloc(ctx, name1, 10)
		assert.NoError(t, err)
		assert.Equal(t, uint64(11), base)
		assert.Equal(t, uint64(20), new)

		current := scopeMgr.GetCurrent(name1)
		assert.Equal(t, uint64(20), current)
	}

	// test alloc: count < 0
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data []byte) error {
			return nil
		})

		now := scopeMgr.GetCurrent(name1)
		_, _, err = scopeMgr.Alloc(ctx, name1, -10)
		assert.Error(t, err)
		assert.Equal(t, uint64(20), now)

		data, _ := json.Marshal(&allocCtx{Name: name1, Current: 30})
		scopeMgr.Apply(ctx, []int32{OperTypeAllocScope}, [][]byte{data}, []base_.ProposeContext{{ReqID: span.TraceID()}})
		now = scopeMgr.GetCurrent(name1)
		assert.Equal(t, uint64(30), now)

		base, current, err := scopeMgr.Alloc(ctx, name1, 1000009)
		assert.NoError(t, err)
		assert.Equal(t, base, uint64(31))
		assert.Equal(t, current, uint64(1000030))
	}

	// test alloc: raft return err
	{
		mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errors.New("err"))

		_, _, err := scopeMgr.Alloc(ctx, name1, 10)
		assert.Error(t, err)
	}

	// test applier other function
	{
		moduleName := scopeMgr.GetModuleName()
		assert.Equal(t, module, module)
		scopeMgr.SetModuleName(moduleName)
		scopeMgr.NotifyLeaderChange(ctx, 1, "")
		scopeMgr.Flush(ctx)
	}
}
