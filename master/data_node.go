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
	"math/rand"
	"sync"
	"time"

	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
)

type DataNode struct {
	Total      uint64 `json:"TotalWeight"`
	Used       uint64 `json:"UsedWeight"`
	Available  uint64
	Id         uint64
	RackName   string `json:"Rack"`
	Addr       string
	ReportTime time.Time
	isActive   bool
	sync.RWMutex
	Ratio              float64
	SelectCount        uint64
	Carry              float64
	Sender             *AdminTaskSender
	dataPartitionInfos []*proto.PartitionReport
	DataPartitionCount uint32
	NodeSetId          uint64
}

func NewDataNode(addr, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.Carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.Sender = NewAdminTaskSender(dataNode.Addr, clusterID)
	return
}

/*check node heartbeat if reportTime > DataNodeTimeOut,then IsActive is false*/
func (dataNode *DataNode) checkHeartBeat() {
	dataNode.Lock()
	defer dataNode.Unlock()
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(DefaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

func (dataNode *DataNode) getBadDiskPartitions(disk string) (partitionIds []uint64) {
	partitionIds = make([]uint64, 0)
	dataNode.RLock()
	defer dataNode.RUnlock()
	for _, partitionReports := range dataNode.dataPartitionInfos {
		if partitionReports.DiskPath == disk {
			partitionIds = append(partitionIds, partitionReports.PartitionID)
		}
	}

	return
}

func (dataNode *DataNode) UpdateNodeMetric(resp *proto.DataNodeHeartBeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	dataNode.Available = resp.Available
	dataNode.RackName = resp.RackName
	dataNode.DataPartitionCount = resp.CreatedPartitionCnt
	dataNode.dataPartitionInfos = resp.PartitionInfo
	if dataNode.Total == 0 {
		dataNode.Ratio = 0.0
	} else {
		dataNode.Ratio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	}
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

func (dataNode *DataNode) IsWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive == true && dataNode.Available > util.DefaultDataPartitionSize {
		ok = true
	}

	return
}

func (dataNode *DataNode) IsAvailCarryNode() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.Carry >= 1
}

func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Carry = carry
}

func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Ratio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectCount++
	dataNode.Carry = dataNode.Carry - 1.0
}

func (dataNode *DataNode) clean() {
	dataNode.Sender.exitCh <- struct{}{}
}

func (dataNode *DataNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}

func (dataNode *DataNode) toJson() (body []byte, err error) {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return json.Marshal(dataNode)
}
