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

package master

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func newCreateDataPartitionRequest(volName string, ID uint64, replicaNum int, members []proto.Peer,
	dataPartitionSize, leaderSize int, hosts []string, createType int, partitionType int,
	decommissionedDisks []string, verSeq uint64,
) (req *proto.CreateDataPartitionRequest) {
	req = &proto.CreateDataPartitionRequest{
		PartitionTyp:        partitionType,
		PartitionId:         ID,
		PartitionSize:       dataPartitionSize,
		ReplicaNum:          replicaNum,
		VolumeId:            volName,
		Members:             members,
		Hosts:               hosts,
		CreateType:          createType,
		LeaderSize:          leaderSize,
		DecommissionedDisks: decommissionedDisks,
		VerSeq:              verSeq,
	}
	return
}

func newDeleteDataPartitionRequest(ID uint64, decommissionType uint32, raftForceDel bool) (req *proto.DeleteDataPartitionRequest) {
	req = &proto.DeleteDataPartitionRequest{
		PartitionId:      ID,
		DecommissionType: decommissionType,
		Force:            raftForceDel,
	}
	return
}

func newAddDataPartitionRaftMemberRequest(ID uint64, addPeer proto.Peer, repairingStatus bool) (req *proto.AddDataPartitionRaftMemberRequest) {
	req = &proto.AddDataPartitionRaftMemberRequest{
		PartitionId:     ID,
		AddPeer:         addPeer,
		RepairingStatus: repairingStatus,
	}
	return
}

func newRemoveDataPartitionRaftMemberRequest(ID uint64, removePeer proto.Peer, repairingStatus bool) (req *proto.RemoveDataPartitionRaftMemberRequest) {
	req = &proto.RemoveDataPartitionRaftMemberRequest{
		PartitionId:     ID,
		RemovePeer:      removePeer,
		RepairingStatus: repairingStatus,
	}
	return
}

func newLoadDataPartitionMetricRequest(ID uint64) (req *proto.LoadDataPartitionRequest) {
	req = &proto.LoadDataPartitionRequest{
		PartitionId: ID,
	}
	return
}

func newStopDataPartitionRepairRequest(ID uint64, stop bool) (req *proto.StopDataPartitionRepairRequest) {
	req = &proto.StopDataPartitionRepairRequest{
		PartitionId: ID,
		Stop:        stop,
	}
	return
}

func newSetRepairingStatusRequest(ID uint64, repairingStatus bool) (req *proto.SetDataPartitionRepairingStatusRequest) {
	req = &proto.SetDataPartitionRepairingStatusRequest{
		PartitionId:     ID,
		RepairingStatus: repairingStatus,
	}
	return
}

func newRecoverDataReplicaMetaRequest(ID uint64, peers []proto.Peer, hosts []string) (req *proto.RecoverDataReplicaMetaRequest) {
	req = &proto.RecoverDataReplicaMetaRequest{
		PartitionId: ID,
		Peers:       peers,
		Hosts:       hosts,
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
	case proto.OpDeleteDataPartition:
		response = &proto.DeleteDataPartitionResponse{}
	case proto.OpLoadDataPartition:
		response = &proto.LoadDataPartitionResponse{}
	case proto.OpDeleteFile:
		response = &proto.DeleteFileResponse{}
	case proto.OpMetaNodeHeartbeat:
		response = &proto.MetaNodeHeartbeatResponse{}
	case proto.OpDeleteMetaPartition:
		response = &proto.DeleteMetaPartitionResponse{}
	case proto.OpUpdateMetaPartition:
		response = &proto.UpdateMetaPartitionResponse{}
	case proto.OpDecommissionMetaPartition:
		response = &proto.MetaPartitionDecommissionResponse{}
	case proto.OpVersionOperation:
		response = &proto.MultiVersionOpResponse{}
	case proto.OpLcNodeHeartbeat:
		response = &proto.LcNodeHeartbeatResponse{}
	case proto.OpLcNodeScan:
		response = &proto.LcNodeRuleTaskResponse{}
	case proto.OpLcNodeSnapshotVerDel:
		response = &proto.SnapshotVerDelTaskResponse{}
	case proto.OpFlashNodeHeartbeat:
		response = &proto.FlashNodeHeartbeatResponse{}
	case proto.OpFlashNodeScan:
		response = &proto.FlashNodeManualTaskResponse{}

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
	if len(arr) == 0 {
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

func containsID(arr []uint64, element uint64) bool {
	if len(arr) == 0 {
		return false
	}

	for _, e := range arr {
		if e == element {
			return true
		}
	}

	return false
}

func reshuffleHosts(oldHosts []string) (newHosts []string, err error) {
	if len(oldHosts) == 0 {
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
	exporter.Warning(msg)
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

func nodeSetNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("node set[%v]", id))
}

func dataNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("data node[%v]", addr))
}

func metaNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("meta node[%v]", addr))
}

func lcNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("lc node[%v]", addr))
}

func matchKey(serverKey, clientKey string) bool {
	h := md5.New()
	_, err := h.Write([]byte(serverKey))
	if err != nil {
		log.LogWarnf("action[matchKey] write server key[%v] failed,err[%v]", serverKey, err)
		return false
	}
	cipherStr := h.Sum(nil)
	return strings.EqualFold(clientKey, hex.EncodeToString(cipherStr))
}

func newRecoverBackupDataPartitionReplicaRequest(ID uint64, disk string) (req *proto.RecoverBackupDataReplicaRequest) {
	req = &proto.RecoverBackupDataReplicaRequest{
		PartitionId: ID,
		Disk:        disk,
	}
	return
}

func newRecoverBadDiskRequest(disk string) (req *proto.RecoverBadDiskRequest) {
	req = &proto.RecoverBadDiskRequest{
		DiskPath: disk,
	}
	return
}

func newDeleteBackupDirectoriesRequest(disk string) (req *proto.DeleteBackupDirectoriesRequest) {
	req = &proto.DeleteBackupDirectoriesRequest{
		DiskPath: disk,
	}
	return
}

func newDeleteLostDiskRequest(disk string) (req *proto.DeleteLostDiskRequest) {
	req = &proto.DeleteLostDiskRequest{
		DiskPath: disk,
	}
	return
}

func newReloadDiskRequest(disk string) (req *proto.ReloadDiskRequest) {
	req = &proto.ReloadDiskRequest{
		DiskPath: disk,
	}
	return
}
