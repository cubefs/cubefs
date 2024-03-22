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
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/log"
)

// MetaNode defines the structure of a meta node
type MetaNode struct {
	ID                        uint64
	Addr                      string
	DomainAddr                string
	IsActive                  bool
	Sender                    *AdminTaskManager `graphql:"-"`
	ZoneName                  string            `json:"Zone"`
	MaxMemAvailWeight         uint64            `json:"MaxMemAvailWeight"`
	Total                     uint64            `json:"TotalWeight"`
	Used                      uint64            `json:"UsedWeight"`
	Ratio                     float64
	SelectCount               uint64
	MemorySelectCount         uint64
	RocksdbHostSelectCount    uint64
	Threshold                 float32
	ReportTime                time.Time
	metaPartitionInfos        []*proto.MetaPartitionReport
	MetaPartitionCount        int
	NodeSetID                 uint64
	sync.RWMutex              `graphql:"-"`
	ToBeOffline               bool
	PersistenceMetaPartitions []uint64
	RdOnly                    bool
	MigrateLock               sync.RWMutex
	CpuUtil                   atomicutil.Float64 `json:"-"`

	RocksdbDisks         []*proto.MetaNodeRocksdbInfo
	RocksdbDiskThreshold float32
}

func newMetaNode(addr, zoneName, clusterID string) (node *MetaNode) {
	node = &MetaNode{
		Addr:     addr,
		ZoneName: zoneName,
		Sender:   newAdminTaskManager(addr, clusterID),
	}
	node.CpuUtil.Store(0)
	return
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

// SelectNodeForWrite implements the Node interface
func (metaNode *MetaNode) SelectNodeForWrite(resource NodeResourceType) {
	metaNode.Lock()
	defer metaNode.Unlock()
	switch resource {
	case MetaNodeRocksdb:
		metaNode.RocksdbHostSelectCount++
	case MetaNodeMemory:
		metaNode.MemorySelectCount++
	}
	metaNode.SelectCount++
}

func (metaNode *MetaNode) GetRocksdbTotal() (total uint64) {
	for _, disk := range metaNode.RocksdbDisks {
		total += disk.Total
	}
	return
}

func (metaNode *MetaNode) GetRocksdbUsed() (used uint64) {
	for _, disk := range metaNode.RocksdbDisks {
		used += disk.Used
	}
	return
}

func (metaNode *MetaNode) reachesRocksdbDisksThreshold() bool {
	var total, used uint64 = 0, 0
	if metaNode.RocksdbDiskThreshold <= 0 {
		metaNode.RocksdbDiskThreshold = defaultRocksdbDiskUsageThreshold
	}
	if len(metaNode.RocksdbDisks) == 0 {
		return true
	}
	total = metaNode.GetRocksdbTotal()
	used = metaNode.GetRocksdbUsed()
	if total == 0 {
		log.LogErrorf("[reachesRocksdbDisksThreshold] metanode(%v) total is 0", metaNode.Addr)
		return true
	}
	threshold := metaNode.RocksdbDiskThreshold
	return float32(used)/float32(total) > threshold
}

func (metaNode *MetaNode) isWritable(storeMode proto.StoreMode) (ok bool) {
	metaNode.RLock()
	defer metaNode.RUnlock()

	log.LogDebugf("[isWritable] mn(%v) check writable store mode(%v)", metaNode.Addr, storeMode)

	if metaNode.IsActive {
		switch storeMode {
		case proto.StoreModeMem:
			if metaNode.MaxMemAvailWeight > gConfig.metaNodeReservedMem &&
				!metaNode.reachesThreshold() &&
				metaNode.MetaPartitionCount < defaultMaxMetaPartitionCountOnEachNode &&
				!metaNode.RdOnly {
				ok = true
			}
		case proto.StoreModeRocksDb:
			if !metaNode.RdOnly && !metaNode.reachesRocksdbDisksThreshold() {
				ok = true
			}
		default:
		}
	}
	return
}

func (metaNode *MetaNode) setNodeActive() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.ReportTime = time.Now()
	metaNode.IsActive = true
}

func (metaNode *MetaNode) updateRocksdbDisks(resp *proto.MetaNodeHeartbeatResponse) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.RocksdbDisks = resp.RocksDBDiskInfo
}

func (metaNode *MetaNode) updateMetric(resp *proto.MetaNodeHeartbeatResponse, threshold, rocksdbDiskThreshold, memModeRocksDBDiskThreshold float32) {
	metaNode.Lock()
	defer metaNode.Unlock()

	metaNode.DomainAddr = util.ParseIpAddrToDomainAddr(metaNode.Addr)
	metaNode.metaPartitionInfos = resp.MetaPartitionReports
	metaNode.MetaPartitionCount = len(metaNode.metaPartitionInfos)
	metaNode.Total = resp.Total
	metaNode.Used = resp.MemUsed
	if resp.Total == 0 {
		metaNode.Ratio = 0
	} else {
		metaNode.Ratio = float64(resp.MemUsed) / float64(resp.Total)
	}
	left := int64(resp.Total - resp.MemUsed)
	if left < 0 {
		metaNode.MaxMemAvailWeight = 0
	} else {
		metaNode.MaxMemAvailWeight = uint64(left)
	}
	metaNode.ZoneName = resp.ZoneName
	metaNode.Threshold = threshold
	metaNode.RocksdbDiskThreshold = rocksdbDiskThreshold
}

func (metaNode *MetaNode) reachesThreshold() bool {
	if metaNode.Threshold <= 0 {
		metaNode.Threshold = defaultMetaPartitionMemUsageThreshold
	}
	return float32(float64(metaNode.Used)/float64(metaNode.Total)) > metaNode.Threshold
}

func (metaNode *MetaNode) createHeartbeatTask(masterAddr string, fileStatsEnable bool) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	request.FileStatsEnable = fileStatsEnable
	task = proto.NewAdminTask(proto.OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}

func (metaNode *MetaNode) createVersionTask(volume string, version uint64, op uint8, addr string, verList []*proto.VolVersionInfo) (task *proto.AdminTask) {
	request := &proto.MultiVersionOpRequest{
		VolumeID:   volume,
		VerSeq:     version,
		Op:         op,
		Addr:       addr,
		VolVerList: verList,
	}
	task = proto.NewAdminTask(proto.OpVersionOperation, metaNode.Addr, request)
	return
}

func (metaNode *MetaNode) checkHeartbeat() {
	metaNode.Lock()
	defer metaNode.Unlock()
	if time.Since(metaNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		metaNode.IsActive = false
	}
}

// LeaderMetaNode define the leader metaPartitions in meta node
type LeaderMetaNode struct {
	addr           string
	metaPartitions []*MetaPartition
}

type sortLeaderMetaNode struct {
	nodes        []*LeaderMetaNode
	leaderCountM map[string]int
	average      int
	mu           sync.RWMutex
}

func (s *sortLeaderMetaNode) Less(i, j int) bool {
	return len(s.nodes[i].metaPartitions) > len(s.nodes[j].metaPartitions)
}

func (s *sortLeaderMetaNode) Swap(i, j int) {
	s.nodes[i], s.nodes[j] = s.nodes[j], s.nodes[i]
}

func (s *sortLeaderMetaNode) Len() int {
	return len(s.nodes)
}

func (s *sortLeaderMetaNode) getLeaderCount(addr string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderCountM[addr]
}

func (s *sortLeaderMetaNode) changeLeader(l *LeaderMetaNode) {
	for _, mp := range l.metaPartitions {
		if count := s.getLeaderCount(l.addr); count <= s.average {
			log.LogInfof("now leader count is[%d], average is[%d]", count, s.average)
			break
		}

		// mp's leader not in this metaNode, skip it
		oldLeader, err := mp.getMetaReplicaLeader()
		if err != nil {
			log.LogErrorf("mp[%v] no leader, can not change leader err[%v]", mp, err)
			continue
		}

		// get the leader metaPartition count meta node which smaller than (old leader count - 1) addr as new leader
		addr := oldLeader.Addr
		s.mu.RLock()
		for i := 0; i < len(mp.Replicas); i++ {
			if s.leaderCountM[mp.Replicas[i].Addr] < s.leaderCountM[oldLeader.Addr]-1 {
				addr = mp.Replicas[i].Addr
			}
		}
		s.mu.RUnlock()

		if addr == oldLeader.Addr {
			log.LogDebugf("newAddr:%s,oldAddr:%s is same", addr, oldLeader.Addr)
			continue
		}

		// one mp change leader failed not influence others
		if err = mp.tryToChangeLeaderByHost(addr); err != nil {
			log.LogErrorf("mp[%v] change to addr[%v] err[%v]", mp, addr, err)
			continue
		}
		s.mu.Lock()
		s.leaderCountM[addr]++
		s.leaderCountM[oldLeader.Addr]--
		s.mu.Unlock()
		log.LogDebugf("mp[%v] oldLeader[%v,nowCount:%d] change to newLeader[%v,nowCount:%d] success", mp.PartitionID, oldLeader.Addr, s.leaderCountM[oldLeader.Addr], addr, s.leaderCountM[addr])
	}
}

func (s *sortLeaderMetaNode) balanceLeader() {
	for _, node := range s.nodes {
		log.LogDebugf("node[%v] leader count is:%d,average:%d", node.addr, len(node.metaPartitions), s.average)
		s.changeLeader(node)
	}
}
