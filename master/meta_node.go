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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

// MetaNode defines the structure of a meta node
type MetaNode struct {
	ID                          uint64
	Addr                        string
	IsActive                    bool
	Sender                      *AdminTaskManager `graphql:"-"`
	ZoneName                    string            `json:"Zone"`
	MaxMemAvailWeight           uint64            `json:"MaxMemAvailWeight"`
	Total                       uint64            `json:"TotalWeight"`
	Used                        uint64            `json:"UsedWeight"`
	Ratio                       float64
	SelectCount                 uint64
	Carry                       float64
	Threshold                   float32
	ReportTime                  time.Time
	metaPartitionInfos          []*proto.MetaPartitionReport
	PhysicalMetaPartitionCount  int
	MetaPartitionCount          int
	NodeSetID                   uint64
	sync.RWMutex                `graphql:"-"`
	ToBeOffline                 bool
	ToBeMigrated                bool
	PersistenceMetaPartitions   []uint64
	ProfPort                    string
	Version                     string
	RocksdbDisks                []*proto.MetaNodeDiskInfo
	RocksdbHostSelectCount      uint64
	RocksdbHostSelectCarry      float64
	RocksdbDiskThreshold        float32
	MemModeRocksdbDiskThreshold float32
}

func newMetaNode(addr, zoneName, clusterID string, version string) (node *MetaNode) {
	return &MetaNode{
		Addr:                   addr,
		ZoneName:               zoneName,
		Sender:                 newAdminTaskManager(addr, zoneName, clusterID),
		Carry:                  rand.Float64(),
		RocksdbHostSelectCarry: rand.Float64(),
		Version:                version,
	}
}

func (metaNode *MetaNode) clean() {
	metaNode.Sender.exitCh <- struct{}{}
}

func (metaNode *MetaNode) GetID() uint64 {
	metaNode.RLock()
	defer metaNode.RUnlock()
	return metaNode.ID
}

func (metaNode *MetaNode) GetAddr() string {
	metaNode.RLock()
	defer metaNode.RUnlock()
	return metaNode.Addr
}

// SetCarry implements the Node interface
func (metaNode *MetaNode) SetCarry(carry float64, storeMode proto.StoreMode) {
	metaNode.Lock()
	defer metaNode.Unlock()
	switch storeMode {
	case proto.StoreModeMem, proto.StoreModeDef:
		metaNode.Carry = carry
	case proto.StoreModeRocksDb:
		metaNode.RocksdbHostSelectCarry = carry
	default:
		panic(fmt.Sprintf("error store mode:%v", storeMode))
	}
}

func (metaNode *MetaNode) GetCarry(storeMode proto.StoreMode) float64 {
	metaNode.RLock()
	defer metaNode.RUnlock()
	switch storeMode {
	case proto.StoreModeMem, proto.StoreModeDef:
		return metaNode.Carry
	case proto.StoreModeRocksDb:
		return metaNode.RocksdbHostSelectCarry
	default:
		panic(fmt.Sprintf("error store mode:%v", storeMode))
	}
}

func (metaNode *MetaNode) GetWeight(maxTotal uint64, storeMode proto.StoreMode) (weight float64) {
	metaNode.RLock()
	defer metaNode.RUnlock()
	switch storeMode {
	case proto.StoreModeMem, proto.StoreModeDef:
		if metaNode.Used < 0 {
			weight = 1.0
		} else {
			weight = (float64)(maxTotal-metaNode.Used) / (float64)(maxTotal)
		}
	case proto.StoreModeRocksDb:
		weight = metaNode.getRocksdbDisksWeight(maxTotal)
	default:
		panic(fmt.Sprintf("error store mode:%v", storeMode))
	}
	return
}

// SelectNodeForWrite implements the Node interface
func (metaNode *MetaNode) SelectNodeForWrite(storeMode proto.StoreMode) {
	metaNode.Lock()
	defer metaNode.Unlock()
	switch storeMode {
	case proto.StoreModeMem, proto.StoreModeDef:
		metaNode.SelectCount++
		metaNode.Carry = metaNode.Carry - 1.0
	case proto.StoreModeRocksDb:
		metaNode.RocksdbHostSelectCount++
		metaNode.RocksdbHostSelectCarry = metaNode.RocksdbHostSelectCarry - 1.0
	default:
		panic(fmt.Sprintf("error store mode:%v", storeMode))
	}
}

func (metaNode *MetaNode) isWritable(storeMode proto.StoreMode) bool {
	metaNode.RLock()
	defer metaNode.RUnlock()
	if !metaNode.IsActive || metaNode.MaxMemAvailWeight <= gConfig.metaNodeReservedMem || metaNode.reachesThreshold() ||
		metaNode.PhysicalMetaPartitionCount >= defaultMaxMetaPartitionCountOnEachNode || metaNode.ToBeOffline == true ||
		metaNode.ToBeMigrated == true || metaNode.reachesRocksdbDisksThreshold(storeMode) {
		return false
	}
	return true
}

func (metaNode *MetaNode) isMixedMetaNode() bool {
	return IsMixedMetaNode(metaNode.Addr)
}

// A carry node is the meta node whose carry is greater than one.
func (metaNode *MetaNode) isCarryNode(storeMode proto.StoreMode) (ok bool) {
	metaNode.RLock()
	defer metaNode.RUnlock()
	switch storeMode {
	case proto.StoreModeMem, proto.StoreModeDef:
		return metaNode.Carry >= 1
	case proto.StoreModeRocksDb:
		return metaNode.RocksdbHostSelectCarry >= 1
	default:
		panic(fmt.Sprintf("error store mode:%v", storeMode))
	}
}

func (metaNode *MetaNode) setNodeActive() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.ReportTime = time.Now()
	metaNode.IsActive = true
}

func (metaNode *MetaNode) updateMetric(resp *proto.MetaNodeHeartbeatResponse, threshold, rocksdbDiskThreshold, memModeRocksDBDiskThreshold float32) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.metaPartitionInfos = resp.MetaPartitionReports
	metaNode.PhysicalMetaPartitionCount = len(metaNode.metaPartitionInfos)
	metaNode.MetaPartitionCount = metaNode.metaPartitionCountWithoutLock()
	metaNode.Total = resp.Total
	metaNode.Used = resp.Used
	if resp.Total == 0 {
		metaNode.Ratio = 0
	} else {
		metaNode.Ratio = float64(resp.Used) / float64(resp.Total)
	}
	if resp.Total > resp.Used {
		metaNode.MaxMemAvailWeight = resp.Total - resp.Used
	} else {
		metaNode.MaxMemAvailWeight = 0
	}
	metaNode.ZoneName = resp.ZoneName
	metaNode.Threshold = threshold
	metaNode.ProfPort = resp.ProfPort
	metaNode.RocksdbDiskThreshold = rocksdbDiskThreshold
	metaNode.MemModeRocksdbDiskThreshold = memModeRocksDBDiskThreshold
}

func (metaNode *MetaNode) metaPartitionCountWithoutLock() (count int) {
	for _, metaPartition := range metaNode.metaPartitionInfos {
		if len(metaPartition.VirtualMPs) == 0 {
			count++
		} else {
			count += len(metaPartition.VirtualMPs)
		}
	}
	return
}

func (metaNode *MetaNode) updateRocksdbDisks(resp *proto.MetaNodeHeartbeatResponse) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.RocksdbDisks = resp.RocksDBDiskInfo
}

func (metaNode *MetaNode) reachesThreshold() bool {
	if metaNode.Threshold <= 0 {
		metaNode.Threshold = defaultMetaPartitionMemUsageThreshold
	}
	return float32(float64(metaNode.Used)/float64(metaNode.Total)) > metaNode.Threshold
}

func (metaNode *MetaNode) reachesRocksdbDisksThreshold(storeMode proto.StoreMode) bool {
	var total, used uint64 = 0, 0
	if metaNode.RocksdbDiskThreshold <= 0 {
		metaNode.RocksdbDiskThreshold = defaultRocksdbDiskUsageThreshold
	}
	if metaNode.MemModeRocksdbDiskThreshold <= 0 {
		metaNode.MemModeRocksdbDiskThreshold = defaultMemModeRocksdbDiskUsageThreshold
	}
	if len(metaNode.RocksdbDisks) == 0 {
		return true
	}
	for _, disk := range metaNode.RocksdbDisks {
		total += disk.Total
		used += disk.Used
	}
	if total == 0 {
		return true
	}
	threshold := metaNode.MemModeRocksdbDiskThreshold
	if storeMode == proto.StoreModeRocksDb {
		threshold = metaNode.RocksdbDiskThreshold
	}
	return float32(used)/float32(total) > threshold
}

func (metaNode *MetaNode) getRocksdbDisksWeight(maxTotal uint64) (weight float64) {
	for _, disk := range metaNode.RocksdbDisks {
		w := float64(disk.Total - disk.Used) / float64(maxTotal)
		if w > weight {
			weight = w
		}
	}
	if weight > 1 {
		weight = 1
	}
	return
}

func (metaNode *MetaNode) createHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}

func (metaNode *MetaNode) checkHeartbeat() {
	metaNode.Lock()
	defer metaNode.Unlock()
	if time.Since(metaNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		metaNode.IsActive = false
	}
}
