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

package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

// github.com/cubefs/cubefs/blobstore/scheduler/... module scheduler interfaces
//go:generate mockgen -destination=./client_mock_test.go -package=scheduler -mock_names ClusterMgrAPI=MockClusterMgrAPI,BlobnodeAPI=MockBlobnodeAPI,IVolumeUpdater=MockVolumeUpdater,ProxyAPI=MockMqProxyAPI github.com/cubefs/cubefs/blobstore/scheduler/client ClusterMgrAPI,BlobnodeAPI,IVolumeUpdater,ProxyAPI
//go:generate mockgen -destination=./base_mock_test.go -package=scheduler -mock_names IConsumer=MockConsumer,IProducer=MockProducer github.com/cubefs/cubefs/blobstore/scheduler/base IConsumer,IProducer
//go:generate mockgen -destination=./scheduler_mock_test.go -package=scheduler -mock_names ITaskRunner=MockTaskRunner,IVolumeCache=MockVolumeCache,MMigrator=MockMigrater,IVolumeInspector=MockVolumeInspector,IClusterTopology=MockClusterTopology github.com/cubefs/cubefs/blobstore/scheduler ITaskRunner,IVolumeCache,MMigrator,IVolumeInspector,IClusterTopology

const (
	testTopic = "test_topic"
)

var (
	any       = gomock.Any()
	errMock   = errors.New("fake error")
	testDisk1 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusNormal,
		DiskID:       1,
		FreeChunkCnt: 10,
		UsedChunkCnt: 20,
		MaxChunkCnt:  700,
	}
	testDisk2 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusNormal,
		DiskID:       2,
		FreeChunkCnt: 10,
		UsedChunkCnt: 10,
		MaxChunkCnt:  700,
	}
)

func NewBroker(t *testing.T) *sarama.MockBroker {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	var msg sarama.ByteEncoder = []byte("FOO")
	for i := 0; i < 1000; i++ {
		mockFetchResponse.SetMessage(testTopic, 0, int64(i), msg)
	}

	broker0 := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:0")
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(testTopic, 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	return broker0
}

func mockGenMigrateTask(taskType proto.TaskType, idc string, diskID proto.DiskID, vid proto.Vid, state proto.MigrateState, volInfoMap map[proto.Vid]*client.VolumeInfoSimple) (task *proto.MigrateTask) {
	srcs := volInfoMap[vid].VunitLocations

	codeMode := volInfoMap[vid].CodeMode
	vunitInfo := MockAlloc(volInfoMap[vid].VunitLocations[0].Vuid)
	task = &proto.MigrateTask{
		TaskID:       client.GenMigrateTaskID(taskType, diskID, vid),
		TaskType:     taskType,
		State:        state,
		SourceIDC:    idc,
		SourceDiskID: diskID,
		SourceVuid:   volInfoMap[vid].VunitLocations[0].Vuid,
		Sources:      srcs,
		CodeMode:     codeMode,

		Destination: vunitInfo.Location(),
		Ctime:       time.Now().String(),
		MTime:       time.Now().String(),
	}
	return task
}

func MockGenVolInfo(vid proto.Vid, cm codemode.CodeMode, status proto.VolumeStatus) *client.VolumeInfoSimple {
	vol := client.VolumeInfoSimple{}
	cmInfo := cm.Tactic()
	vunitCnt := cmInfo.M + cmInfo.N + cmInfo.L
	host := "127.0.0.0:xxx"
	locations := make([]proto.VunitLocation, vunitCnt)
	var idx uint8
	for i := 0; i < vunitCnt; i++ {
		locations[i].Vuid, _ = proto.NewVuid(vid, idx, 1)
		locations[i].Host = host
		locations[i].DiskID = proto.DiskID(locations[i].Vuid)
		idx++
	}
	vol.Status = status
	vol.VunitLocations = locations
	vol.Vid = vid
	vol.CodeMode = cm
	return &vol
}

func MockAlloc(vuid proto.Vuid) *client.AllocVunitInfo {
	vid := vuid.Vid()
	idx := vuid.Index()
	epoch := vuid.Epoch()
	epoch++
	newVuid, _ := proto.NewVuid(vid, idx, epoch)
	return &client.AllocVunitInfo{
		VunitLocation: proto.VunitLocation{
			Vuid:   newVuid,
			DiskID: proto.DiskID(newVuid),
			Host:   "127.0.0.0:xxx",
		},
	}
}
