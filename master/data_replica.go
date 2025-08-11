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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

// DataReplica represents the replica of a data partition
type DataReplica struct {
	proto.DataReplica
	dataNode *DataNode
	// loc      uint8
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

func (replica *DataReplica) isNormal(id uint64, ReportTime int64) (isNormal bool) {
	if replica.dataNode.isActive &&
		(replica.Status == proto.ReadWrite || replica.Status == proto.ReadOnly) &&
		timeutil.GetCurrentTimeUnix()-replica.ReportTime <= ReportTime {
		return true
	}
	log.LogDebugf("action[isLive] partition %v, replica addr %v, datanode active %v replica status %v and is active %v",
		id, replica.Addr, replica.dataNode.isActive, replica.Status, replica.ReportTime)
	return false
}

func (replica *DataReplica) isLive(id uint64, timeOutSec int64) (isAvailable bool) {
	if replica.dataNode.isActive && replica.Status != proto.Unavailable &&
		replica.isActive(timeOutSec) {
		isAvailable = true
	} else {
		log.LogDebugf("action[isLive] partition %v, replica addr %v, datanode active %v replica status %v and is active %v",
			id, replica.Addr, replica.dataNode.isActive, replica.Status, replica.isActive(timeOutSec))
	}
	return
}

func (replica *DataReplica) isActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *DataReplica) getReplicaNode() (node *DataNode) {
	return replica.dataNode
}

func (replica *DataReplica) isRepairing() bool {
	return replica.Status == proto.Recovering
}

func (replica *DataReplica) isUnavailable() bool {
	return replica.Status == proto.Unavailable
}
