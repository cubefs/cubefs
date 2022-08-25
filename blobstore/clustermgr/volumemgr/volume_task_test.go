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

package volumemgr

import (
	"context"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestTaskProc(t *testing.T) {
	tmpTaskDb := path.Join(os.TempDir(), "taskDb-"+strconv.Itoa(rand.Intn(100)))
	defer os.RemoveAll(tmpTaskDb)

	db, err := volumedb.Open(tmpTaskDb, false, func(option *kvstore.RocksDBOption) {
		option.WriteBufferSize = 1 << 22
	})
	require.Nil(t, err)
	volumeTbl, err := volumedb.OpenVolumeTable(db)
	require.Nil(t, err)

	ctrl := gomock.NewController(t)
	raftServer := mocks.NewMockRaftServer(ctrl)
	raftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	raftServer.EXPECT().IsLeader().AnyTimes().Return(true)

	dnClient := mocks.NewMockStorageAPI(ctrl)
	dnClient.EXPECT().SetChunkReadonly(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	dnClient.EXPECT().SetChunkReadwrite(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	diskmgr := NewMockDiskMgrAPI(ctrl)
	diskmgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().Return(&blobnode.DiskInfo{Host: "127.0.0.1:8080"}, nil)

	volMgr := &VolumeMgr{
		volumeTbl:      volumeTbl,
		taskMgr:        newTaskManager(10),
		raftServer:     raftServer,
		all:            newShardedVolumes(8),
		diskMgr:        diskmgr,
		blobNodeClient: dnClient,
	}

	allocConfig := allocConfig{
		codeModes:       map[codemode.CodeMode]codeModeConf{1: {mode: 1}},
		freezeThreshold: 0,
	}
	volAllocator := newVolumeAllocator(allocConfig)
	volMgr.allocator = volAllocator

	volRec := &volumedb.VolumeRecord{
		Vid:      2,
		CodeMode: 1,
		Status:   proto.VolumeStatusLock,
	}
	taskRec := &volumedb.VolumeTaskRecord{
		Vid:      2,
		TaskType: base.VolumeTaskTypeLock,
		TaskId:   uuid.NewString(),
	}
	volumeTbl.PutVolumeAndTask(volRec, taskRec)

	err = volMgr.reloadTasks()
	require.Nil(t, err)

	vunits := []*volumeUnit{
		{
			vuidPrefix: proto.EncodeVuidPrefix(1, 0),
			epoch:      0,
			nextEpoch:  1,
			vuInfo: &cm.VolumeUnitInfo{
				Vuid:   proto.EncodeVuid(proto.EncodeVuidPrefix(1, 0), 0),
				DiskID: 1000,
			},
		},
		{
			vuidPrefix: proto.EncodeVuidPrefix(1, 1),
			epoch:      0,
			nextEpoch:  1,
			vuInfo: &cm.VolumeUnitInfo{
				Vuid:   proto.EncodeVuid(proto.EncodeVuidPrefix(1, 1), 0),
				DiskID: 2000,
			},
		},
	}
	vol := &volume{
		vid:    1,
		vUnits: vunits,
		volInfoBase: cm.VolumeInfoBase{
			CodeMode: 1,
			Status:   proto.VolumeStatusIdle,
		},
	}
	volMgr.all.putVol(vol)
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// volume lock
	volMgr.applyVolumeTask(ctx, 1, uuid.New().String(), base.VolumeTaskTypeLock)
	require.Equal(t, proto.VolumeStatusLock, vol.volInfoBase.Status)
	taskid, hit := volMgr.lastTaskIdMap.Load(vol.vid)
	require.True(t, hit)
	time.Sleep(100 * time.Millisecond)
	volMgr.applyRemoveVolumeTask(ctx, vol.vid, taskid.(string), base.VolumeTaskTypeLock)
	_, hit = volMgr.lastTaskIdMap.Load(vol.vid)
	require.False(t, hit)

	// volume unlock
	volMgr.applyVolumeTask(ctx, 1, uuid.New().String(), base.VolumeTaskTypeUnlock)
	taskid, hit = volMgr.lastTaskIdMap.Load(vol.vid)
	require.True(t, hit)
	time.Sleep(100 * time.Millisecond) // wait task finish
	volMgr.applyRemoveVolumeTask(ctx, vol.vid, taskid.(string), base.VolumeTaskTypeUnlock)
	require.Equal(t, proto.VolumeStatusIdle, vol.volInfoBase.Status)

	// delete task
	task := newVolTask(taskRec.Vid, taskRec.TaskType, taskRec.TaskId, volMgr.setVolumeStatus)
	err = volMgr.deleteTask(ctx, task)
	require.NoError(t, err)
}
