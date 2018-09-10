// Copyright 2018 The ChuBao Authors.
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
	"github.com/tiglabs/containerfs/proto"
	"time"
)

type DataReplica struct {
	Addr                    string
	dataNode                *DataNode
	ReportTime              int64
	FileCount               uint32
	loc                     uint8
	Status                  int8
	LoadPartitionIsResponse bool
	Total                   uint64 `json:"TotalSize"`
	Used                    uint64 `json:"UsedSize"`
}

func NewDataReplica(dataNode *DataNode) (replica *DataReplica) {
	replica = new(DataReplica)
	replica.dataNode = dataNode
	replica.Addr = dataNode.Addr
	replica.ReportTime = time.Now().Unix()
	return
}

func (replica *DataReplica) SetAlive() {
	replica.ReportTime = time.Now().Unix()
}

func (replica *DataReplica) CheckMiss(missSec int64) (isMiss bool) {
	if time.Now().Unix()-replica.ReportTime > missSec {
		isMiss = true
	}
	return
}

func (replica *DataReplica) IsLive(timeOutSec int64) (avail bool) {
	if replica.dataNode.isActive == true && replica.Status != proto.Unavaliable &&
		replica.IsActive(timeOutSec) == true {
		avail = true
	}

	return
}

func (replica *DataReplica) IsActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *DataReplica) GetReplicaNode() (node *DataNode) {
	return replica.dataNode
}

/*check replica location is avail ,must IsActive=true and replica.Status!=DataReplicaUnavailable*/
func (replica *DataReplica) CheckLocIsAvailContainsDiskError() (avail bool) {
	dataNode := replica.GetReplicaNode()
	dataNode.Lock()
	defer dataNode.Unlock()
	if dataNode.isActive == true && replica.IsActive(DefaultDataPartitionTimeOutSec) == true {
		avail = true
	}

	return
}
