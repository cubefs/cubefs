// Copyright 2018 The Chubao Authors.
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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
	"math/rand"
	"strings"
	"time"
)

func newCreateDataPartitionRequest(volName string, ID uint64, members []proto.Peer, dataPartitionSize int) (req *proto.CreateDataPartitionRequest) {
	req = &proto.CreateDataPartitionRequest{
		PartitionId:   ID,
		PartitionSize: dataPartitionSize,
		VolumeId:      volName,
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

func newOfflineDataPartitionRequest(ID uint64, removePeer, addPeer proto.Peer) (req *proto.DataPartitionDecommissionRequest) {
	req = &proto.DataPartitionDecommissionRequest{
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

func unmarshalTaskResponse(task *proto.AdminTask) (err error) {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return
	}
	var response interface{}
	switch task.OpCode {
	case proto.OpDataNodeHeartbeat:
		response = &proto.DataNodeHeartbeatResponse{}
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
	case proto.OpDecommissionMetaPartition:
		response = task.Response.(*proto.MetaPartitionDecommissionResponse)
	case proto.OpDecommissionDataPartition:
		response = task.Response.(*proto.DataPartitionDecommissionResponse)

	default:
		log.LogError(fmt.Sprintf("unknown operate code(%v)", task.OpCode))
	}

	if response == nil {
		return fmt.Errorf("unmarshalTaskResponse failed")
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

func reshuffleHosts(oldHosts []string) (newHosts []string, err error) {
	if oldHosts == nil || len(oldHosts) == 0 {
		log.LogError(fmt.Sprintf("action[reshuffleHosts],err:%v", proto.ErrReshuffleArray))
		err = proto.ErrReshuffleArray
		return
	}

	lenOldHosts := len(oldHosts)
	newHosts = make([]string, lenOldHosts)
	if lenOldHosts == 1 {
		copy(newHosts, oldHosts)
		return
	}

	for i := lenOldHosts; i > 1; i-- {
		rand.Seed(time.Now().UnixNano())
		oCurrPos := rand.Intn(i)
		oldHosts[i-1], oldHosts[oCurrPos] = oldHosts[oCurrPos], oldHosts[i-1]
	}
	copy(newHosts, oldHosts)
	return
}

// Warn provides warnings when exits
func Warn(clusterID, msg string) {
	key := fmt.Sprintf("%s_%s", clusterID, ModuleName)
	WarnBySpecialKey(key, msg)
}

// WarnBySpecialKey provides warnings when exits
func WarnBySpecialKey(key, msg string) {
	log.LogWarn(msg)
	ump.Alarm(key, msg)
}

func keyNotFound(name string) (err error) {
	return errors.NewErrorf("parameter %v not found", name)
}

func unmatchedKey(name string) (err error) {
	return errors.NewErrorf("parameter %v not match", name)
}

func notFoundMsg(name string) (err error) {
	return errors.NewErrorf("%v not found", name)
}

func metaPartitionNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("meta partition[%v]", id))
}

func metaReplicaNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("meta replica[%v]", addr))
}

func dataPartitionNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("data partition[%v]", id))
}

func dataReplicaNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("data replica[%v]", addr))
}

func rackNotFound(name string) (err error) {
	return notFoundMsg(fmt.Sprintf("rack[%v]", name))
}

func dataNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("data node[%v]", addr))
}

func metaNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("meta node[%v]", addr))
}

func volNotFound(name string) (err error) {
	return notFoundMsg(fmt.Sprintf("vol[%v]", name))
}

func matchKey(serverKey, clientKey string) bool {
	h := md5.New()
	_, err := h.Write([]byte(serverKey))
	if err != nil {
		log.LogWarnf("action[matchKey] write server key[%v] failed,err[%v]", serverKey, err)
		return false
	}
	cipherStr := h.Sum(nil)
	return strings.ToLower(clientKey) == strings.ToLower(hex.EncodeToString(cipherStr))
}
