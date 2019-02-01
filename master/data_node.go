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
	"math/rand"
	"sync"
	"time"

	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
)

// DataNode stores all the information about a data node
type DataNode struct {
	Total          uint64 `json:"TotalWeight"`
	Used           uint64 `json:"UsedWeight"`
	AvailableSpace uint64
	ID             uint64
	RackName       string `json:"Rack"`
	Addr           string
	ReportTime     time.Time
	isActive       bool
	sync.RWMutex
	UsageRatio           float64 // used / total space
	SelectedTimes        uint64  // number times that this datanode has been selected as the location for a data partition.
	Carry                float64 // carry is a factor used in cacluate the node's weight
	TaskManager          *AdminTaskManager
	dataPartitionReports []*proto.PartitionReport
	DataPartitionCount   uint32
	NodeSetID            uint64
}

func newDataNode(addr, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.Carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.TaskManager = newAdminTaskManager(dataNode.Addr, clusterID)
	return
}

func (dataNode *DataNode) checkLiveness() {
	dataNode.Lock()
	defer dataNode.Unlock()
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

func (dataNode *DataNode) badPartitionIDs(disk string) (partitionIds []uint64) {
	partitionIds = make([]uint64, 0)
	dataNode.RLock()
	defer dataNode.RUnlock()
	for _, partitionReports := range dataNode.dataPartitionReports {
		if partitionReports.DiskPath == disk {
			partitionIds = append(partitionIds, partitionReports.PartitionID)
		}
	}

	return
}

func (dataNode *DataNode) updateNodeMetric(resp *proto.DataNodeHeartbeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	dataNode.AvailableSpace = resp.Available
	dataNode.RackName = resp.RackName
	dataNode.DataPartitionCount = resp.CreatedPartitionCnt
	dataNode.dataPartitionReports = resp.PartitionReports
	if dataNode.Total == 0 {
		dataNode.UsageRatio = 0.0
	} else {
		dataNode.UsageRatio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	}
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

func (dataNode *DataNode) isWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive == true && dataNode.AvailableSpace > 10*util.GB {
		ok = true
	}

	return
}

func (dataNode *DataNode) isAvailCarryNode() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.Carry >= 1
}

// SetCarry implements "SetCarry" in the Node interface
func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Carry = carry
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.UsageRatio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectedTimes++
	dataNode.Carry = dataNode.Carry - 1.0
}

func (dataNode *DataNode) clear() {
	dataNode.TaskManager.exitCh <- struct{}{}
}

func (dataNode *DataNode) createHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}

func (dataNode *DataNode) toJSON() (body []byte, err error) {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return json.Marshal(dataNode)
}
