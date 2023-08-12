// Copyright 2023 The CubeFS Authors.
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

package mocktest

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type mockClientAPI struct {
	master.ClientAPI
}

func (m mockClientAPI) GetMetaPartition(partitionID uint64) (partition *proto.MetaPartitionInfo, err error) {
	return &proto.MetaPartitionInfo{
		PartitionID: 0,
		Start:       0,
		End:         0,
		MaxInodeID:  0,
		InodeCount:  0,
		DentryCount: 0,
		VolName:     "",
		Replicas: []*proto.MetaReplicaInfo{
			{
				Addr:        "172.16.1.101:17210",
				DomainAddr:  "172.16.1.101:17210",
				MaxInodeID:  0,
				ReportTime:  0,
				Status:      0,
				IsLeader:    false,
				InodeCount:  0,
				MaxInode:    0,
				DentryCount: 0,
			},
			{
				Addr:        "172.16.1.102:17210",
				DomainAddr:  "172.16.1.102:17210",
				MaxInodeID:  0,
				ReportTime:  0,
				Status:      0,
				IsLeader:    true,
				InodeCount:  0,
				MaxInode:    0,
				DentryCount: 0,
			},
			{
				Addr:        "172.16.1.103:17210",
				DomainAddr:  "172.16.1.103:17210",
				MaxInodeID:  0,
				ReportTime:  0,
				Status:      0,
				IsLeader:    false,
				InodeCount:  0,
				MaxInode:    0,
				DentryCount: 0,
			},
		},
		ReplicaNum: 0,
		Status:     0,
		IsRecover:  false,
		Hosts:      []string{"172.16.1.101:17210", "172.16.1.102:17210", "172.16.1.103:17210"},
		Peers: []proto.Peer{
			{
				ID:   1,
				Addr: "172.16.1.101:17210",
			},
			{
				ID:   2,
				Addr: "172.16.1.102:17210",
			},
			{
				ID:   3,
				Addr: "172.16.1.103:17210",
			},
		},
		Zones:         []string{"default", "default", "default"},
		OfflinePeerID: 0,
		MissNodes: map[string]int64{
			"172.16.1.103:17210": 1690280680,
		},
		LoadResponse: []*proto.MetaPartitionLoadResponse{},
	}, nil
}

func (m mockClientAPI) GetMetaPartitions(volName string) (views []*proto.MetaPartitionView, err error) {
	return []*proto.MetaPartitionView{
		{
			PartitionID: 1,
			Start:       0,
			End:         0,
			MaxInodeID:  0,
			InodeCount:  0,
			DentryCount: 0,
			FreeListLen: 0,
			IsRecover:   false,
			Members:     []string{},
			LeaderAddr:  "",
			Status:      0,
		},
	}, nil
}

func (m mockClientAPI) GetDataPartitions(volName string) (view *proto.DataPartitionsView, err error) {
	return &proto.DataPartitionsView{
		DataPartitions: []*proto.DataPartitionResponse{
			{
				PartitionType: 0,
				PartitionID:   1,
				Status:        0,
				ReplicaNum:    0,
				Hosts:         []string{},
				LeaderAddr:    "",
				Epoch:         0,
				IsRecover:     false,
				PartitionTTL:  0,
				IsDiscard:     false,
			},
		},
	}, nil
}
