// Copyright 2018 The Container File System Authors.
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

/* TODO rename the file as data_partition_replica.go? */

// DataReplica represents the replica of a data partition
// TODO rename to DataPartitionReplica?
type DataReplica struct {
	Addr                    string
	dataNode                *DataNode
	ReportTime              int64
	FileCount               uint32
	loc                     uint8
	Status                  int8
	LoadPartitionIsResponse bool // TODO what is LoadPartitionIsResponse?
	Total                   uint64 `json:"TotalSize"`
	Used                    uint64 `json:"UsedSize"`
	IsLeader                bool
	NeedsToCompare          bool
	DiskPath                string
}

func newDataReplica(dataNode *DataNode) (replica *DataReplica) {
	replica = new(DataReplica)
	replica.dataNode = dataNode
	replica.Addr = dataNode.Addr
	replica.ReportTime = time.Now().Unix()
	return
}

func (replica *DataReplica) setAlive() {
	replica.ReportTime = time.Now().Unix()
}

func (replica *DataReplica) isMissing(missSec int64) (isMissing bool) {
	if time.Now().Unix()-replica.ReportTime > missSec {
		isMissing = true
	}
	return
}

func (replica *DataReplica) isLive(timeOutSec int64) (avail bool) {
	if replica.dataNode.isActive == true && replica.Status != proto.Unavaliable &&
		replica.isActive(timeOutSec) == true {
		avail = true
	}

	return
}

func (replica *DataReplica) isActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *DataReplica) getReplicaNode() (node *DataNode) {
	return replica.dataNode
}

// check if the replica's location is available
func (replica *DataReplica) isLocationAvailable() (avail bool) {
	dataNode := replica.getReplicaNode()
	dataNode.Lock()
	defer dataNode.Unlock()
	if dataNode.isActive == true && replica.isActive(defaultDataPartitionTimeOutSec) == true {
		avail = true
	}

	return
}
