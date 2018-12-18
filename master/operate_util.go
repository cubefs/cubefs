// Copyright 2018 The Containerfs Authors.
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

package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
)

func newCreateDataPartitionRequest(volName string, ID uint64, randomWrite bool, members []proto.Peer, dataPartitionSize int) (req *proto.CreateDataPartitionRequest) {
	req = &proto.CreateDataPartitionRequest{
		PartitionId:   ID,
		PartitionSize: dataPartitionSize,
		VolumeId:      volName,
		RandomWrite:   randomWrite,
		Members:       members,
	}
	return
}

func newDeleteDataPartitionRequest(ID uint64) (req *proto.DeleteDataPartitionRequest) {
	req = &proto.DeleteDataPartitionRequest{
		PartitionId: ID,
	}
	return
}

func newOfflineDataPartitionRequest(ID uint64, removePeer, addPeer proto.Peer) (req *proto.DataPartitionOfflineRequest) {
	req = &proto.DataPartitionOfflineRequest{
		PartitionId: ID,
		RemovePeer:  removePeer,
		AddPeer:     addPeer,
	}
	return
}

func newLoadDataPartitionMetricRequest(ID uint64) (req *proto.LoadDataPartitionRequest) {
	req = &proto.LoadDataPartitionRequest{
		PartitionId: ID,
	}
	return
}

func UnmarshalTaskResponse(task *proto.AdminTask) (err error) {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return
	}
	var response interface{}
	switch task.OpCode {
	case proto.OpDataNodeHeartbeat:
		response = &proto.DataNodeHeartBeatResponse{}
	case proto.OpCreateDataPartition:
		response = &proto.CreateDataPartitionResponse{}
	case proto.OpDeleteDataPartition:
		response = &proto.DeleteDataPartitionResponse{}
	case proto.OpLoadDataPartition:
		response = &proto.LoadDataPartitionResponse{}
	case proto.OpDeleteFile:
		response = &proto.DeleteFileResponse{}
	case proto.OpMetaNodeHeartbeat:
		response = &proto.MetaNodeHeartbeatResponse{}
	case proto.OpCreateMetaPartition:
		response = &proto.CreateMetaPartitionResponse{}
	case proto.OpDeleteMetaPartition:
		response = &proto.DeleteMetaPartitionResponse{}
	case proto.OpUpdateMetaPartition:
		response = &proto.UpdateMetaPartitionResponse{}
	case proto.OpLoadMetaPartition:
		response = task.Response.(*proto.LoadMetaPartitionMetricResponse)
	case proto.OpOfflineMetaPartition:
		response = task.Response.(*proto.MetaPartitionOfflineResponse)
	case proto.OpOfflineDataPartition:
		response = task.Response.(*proto.DataPartitionOfflineResponse)

	default:
		log.LogError(fmt.Sprintf("unknown operate code(%v)", task.OpCode))
	}

	if response == nil {
		return fmt.Errorf("UnmarshalTaskResponse failed")
	}
	if err = json.Unmarshal(bytes, response); err != nil {
		return
	}
	task.Response = response
	return
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func Warn(clusterID, msg string) {
	umpKey := fmt.Sprintf("%s_%s", clusterID, UmpModuleName)
	WarnBySpecialUmpKey(umpKey, msg)
}

func WarnBySpecialUmpKey(umpKey, msg string) {
	log.LogWarn(msg)
	ump.Alarm(umpKey, msg)
}
