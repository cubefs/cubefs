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
	"github.com/cubefs/cubefs/util/log"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// DataReplica represents the replica of a data partition
type DataReplica struct {
	proto.DataReplica
	dataNode *DataNode
	loc      uint8
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

func (replica *DataReplica) isMissing(interval int64) (isMissing bool) {
	if time.Now().Unix()-replica.ReportTime > interval {
		isMissing = true
	}
	return
}

func (replica *DataReplica) isLive(timeOutSec int64) (isAvailable bool) {
	log.LogDebugf("action[isLive] replica addr %v, datanode active %v replica status %v and is actvie %v",
		replica.Addr, replica.dataNode.isActive, replica.Status, replica.isActive(timeOutSec))
	if replica.dataNode.isActive && replica.Status != proto.Unavailable &&
		replica.isActive(timeOutSec) {
		isAvailable = true
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
func (replica *DataReplica) isLocationAvailable() (isAvailable bool) {
	dataNode := replica.getReplicaNode()
	dataNode.Lock()
	defer dataNode.Unlock()
	if dataNode.isActive == true && replica.isActive(defaultDataPartitionTimeOutSec) == true {
		isAvailable = true
	}

	return
}
