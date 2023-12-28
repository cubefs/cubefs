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
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"
)

type taskFunc func()

func (c *Cluster) startPeriodicBackgroundSchedulingTasks() {
	// vol tasks
	c.startTask(c.doCheckVolStatus, time.Second*time.Duration(c.cfg.IntervalToCheckDataPartition), 0, false)
	c.startTask(c.doCheckVolReduceReplicaNum, 5*time.Minute, 2*time.Minute, false)
	c.startTask(c.doCheckUpdateVolsResponseCache, time.Second*time.Duration(c.cfg.IntervalToCheckDataPartition), 0, false)
	c.startTask(c.doConvertRenamedOldVol, 2*time.Minute, 2*time.Minute, false)

	// data partition  tasks
	c.startTask(c.doCheckCreateDataPartitions, 2*time.Minute, 5*time.Minute, false)
	c.startTask(c.doCheckDataPartitions, time.Second*time.Duration(c.cfg.IntervalToCheckDataPartition), 0, false)
	//c.startTask(c.doLoadDataPartitions, time.Second*5, 0, false)
	//c.startTask(c.doReleaseDataPartition, time.Second*defaultIntervalToFreeDataPartition, 0, false)
	c.startTask(c.doCheckBadDiskRecovery, time.Second*defaultIntervalToCheckDataPartition, 0, false)
	c.startTask(c.doCheckUpdatePartitionReplicaNum, 2*time.Minute, 0, false)

	// ec data partition tasks
	c.startTask(c.doCheckEcDataPartitions, time.Minute, 0, false)

	// meta partition  tasks
	c.startTask(c.doCheckCreateMetaPartitions, 2*time.Second*defaultIntervalToWaitMetaPartitionElectionLeader, 5*time.Minute, false)
	c.startTask(c.doCheckMetaPartitions, time.Second*time.Duration(c.cfg.IntervalToCheckDataPartition), 0, false)
	c.startTask(c.doCheckLoadMetaPartitions, time.Hour, 0, false)
	c.startTask(c.doCheckMetaPartitionRecoveryProgress, 3*time.Minute, 0, false)

	// cluster tasks
	c.startTask(c.doUpdateClusterView, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doUpdateFlashGroupResponseCache, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckDataNodeHeartbeat, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckMetaNodeHeartbeat, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckEcNodeHeartbeat, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckCodecNodeHeartbeat, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckFlashNodeHeartbeat, time.Second*defaultIntervalToCheckHeartbeat, 0, true)
	c.startTask(c.doCheckAvailSpace, 2*time.Minute, 0, false)
	c.startTask(c.doCheckMergeZoneNodeset, 24*time.Hour, 0, false)
}

func (c *Cluster) startTask(f taskFunc, period time.Duration, firstDelay time.Duration, forceStart bool) {
	c.taskWg.Add(1)
	go func(f taskFunc, period time.Duration, delayTime time.Duration, forceStart bool) {
		funcPointer := reflect.ValueOf(f).Pointer()
		name := runtime.FuncForPC(funcPointer).Name()
		log.LogWarnf("start task %v, at term:%v", name, c.raftTermOnStartTask)
		startTaskTime := time.Now()
		ticker := time.NewTicker(period)
		defer func() {
			c.taskWg.Done()
			ticker.Stop()
		}()
		for {
			select {
			case <-ticker.C:
				if time.Since(startTaskTime) < delayTime {
					break
				}
				// 只有一半以上的节点存活才执行后台任务
				if !forceStart && !c.isMajorNodesAlive(c.cfg.NodesLiveRatio) {
					log.LogWarnf("too many nodes are dead")
					break
				}
				if c.partition != nil && c.partition.IsRaftLeader() && !c.leaderHasChanged() {
					f()
				}
			case <-c.taskExitC:
				log.LogWarnf("task %v will be exited , raftTermOnStartTask:%v", name, c.raftTermOnStartTask)
				return
			}
		}
	}(f, period, firstDelay, forceStart)
}

func (c *Cluster) isMajorNodesAlive(ratio float32) bool {
	if ratio < defaultNodesLiveRatio {
		ratio = defaultNodesLiveRatio
	}
	return c.getLiveDataNodesRate() > ratio && c.getLiveMetaNodesRate() > ratio
}

func (c *Cluster) getLiveDataNodesRate() (rate float32) {
	dataNodes := make([]proto.NodeView, 0)
	liveDataNodes := make([]proto.NodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		view := proto.NodeView{Addr: dataNode.Addr, Status: dataNode.isActive}
		dataNodes = append(dataNodes, view)
		if dataNode.isActive && time.Since(dataNode.ReportTime) < time.Second*time.Duration(2*defaultIntervalToCheckHeartbeat) {
			liveDataNodes = append(liveDataNodes, view)
		}
		return true
	})
	return float32(len(liveDataNodes)) / float32(len(dataNodes))
}

func (c *Cluster) getLiveMetaNodesRate() (rate float32) {
	metaNodes := make([]proto.NodeView, 0)
	liveMetaNodes := make([]proto.NodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		view := proto.NodeView{Addr: metaNode.Addr, Status: metaNode.IsActive, ID: metaNode.ID}
		metaNodes = append(metaNodes, view)
		if metaNode.IsActive && time.Since(metaNode.ReportTime) < time.Second*time.Duration(2*defaultIntervalToCheckHeartbeat) {
			liveMetaNodes = append(liveMetaNodes, view)
		}
		return true
	})
	return float32(len(liveMetaNodes)) / float32(len(metaNodes))
}

func (c *Cluster) leaderHasChanged() bool {
	raftTermOnStartTask := atomic.LoadUint64(&c.raftTermOnStartTask)
	_, curTerm := c.partition.LeaderTerm()
	if raftTermOnStartTask != curTerm {
		return true
	}
	return false
}

func (c *Cluster) doCheckVolStatus() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckVolStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckVolStatus occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.checkStatus(c)
	}
}

func (c *Cluster) doCheckVolReduceReplicaNum() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckVolReduceReplicaNum occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckVolReduceReplicaNum occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.checkReplicaNum(c)
	}
}

func (c *Cluster) doCheckUpdateVolsResponseCache() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckUpdateVolsResponseCache occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckUpdateVolsResponseCache occurred panic")
		}
	}()
	_, err := c.updateVolsResponseCache()
	if err != nil {
		log.LogErrorf("action[doCheckUpdateVolsResponseCache] occurred error,err:%v", err)
	}
}

func (c *Cluster) doConvertRenamedOldVol() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doConvertRenamedOldVol occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doConvertRenamedOldVol occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.doConvertRenamedOldVol(c)
	}
}

func (c *Cluster) doCheckCreateDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckCreateDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckCreateDataPartitions occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.checkAutoDataPartitionCreation(c)
	}
}

func (c *Cluster) doCheckDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckDataPartitions occurred panic")
		}
	}()
	c.checkDataPartitions()
}

func (c *Cluster) doLoadDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doLoadDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doLoadDataPartitions occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.loadDataPartition(c)
	}
}

func (c *Cluster) doReleaseDataPartition() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("releaseDataPartitionAfterLoad occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"releaseDataPartitionAfterLoad occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.releaseDataPartitions(c.cfg.numberOfDataPartitionsToFree, c.cfg.secondsToFreeDataPartitionAfterLoad)
	}
}

func (c *Cluster) doCheckBadDiskRecovery() {
	if c.vols != nil {
		c.checkDiskRecoveryProgress()
		c.checkEcDiskRecoveryProgress()
		c.checkMigratedDataPartitionsRecoveryProgress()
	}
}

func (c *Cluster) doCheckUpdatePartitionReplicaNum() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckUpdatePartitionReplicaNum occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckUpdatePartitionReplicaNum occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		if !c.cfg.AutoUpdatePartitionReplicaNum {
			break
		}
		if vol.status() == proto.VolStMarkDelete {
			continue
		}
		vol.checkAndUpdateDataPartitionReplicaNum(c)
		vol.checkAndUpdateMetaPartitionReplicaNum(c)
	}
}

func (c *Cluster) doCheckEcDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckEcDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckEcDataPartitions occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		canReadWriteEpCnt := vol.checkEcDataPartitions(c)
		_, err := vol.ecDataPartitions.updateResponseCache(true, 0)
		msg := fmt.Sprintf("action[checkEcDataPartitions],vol[%v] can readWrite ec partitions:%v ,err:%v", vol.Name, canReadWriteEpCnt, err)
		log.LogDebugf(msg)
	}
}

func (c *Cluster) doCheckCreateMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoMetaPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoMetaPartitionCreation occurred panic")
		}
	}()
	ctx := c.buildCreateMetaPartitionContext()
	vols := c.copyVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		vol.checkAutoMetaPartitionCreation(c, ctx)
	}
}

func (c *Cluster) doCheckMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaPartitions occurred panic")
		}
	}()
	ctx := c.buildCreateMetaPartitionContext()
	vols := c.allVols()
	for _, vol := range vols {
		if c.leaderHasChanged() {
			return
		}
		writableMpCount := vol.checkMetaPartitions(c, ctx)
		vol.setWritableMpCount(int64(writableMpCount))
	}
}

func (c *Cluster) doCheckLoadMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			if c.leaderHasChanged() {
				return
			}
			c.doLoadMetaPartition(mp)
		}
	}
}

func (c *Cluster) doCheckMetaPartitionRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckMetaPartitionRecoveryProgress occurred panic")
		}
	}()
	c.checkMetaPartitionRecoveryProgress()
	c.checkMigratedMetaPartitionRecoveryProgress()
}

func (c *Cluster) checkLeaderAddr() {
	leaderId, _ := c.partition.LeaderTerm()
	c.leaderInfo.addr = AddrDatabase[leaderId]
}

func (c *Cluster) doUpdateClusterView() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doUpdateClusterView occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doUpdateClusterView occurred panic")
		}
	}()
	c.updateClusterViewResponseCache()
}

func (c *Cluster) doUpdateFlashGroupResponseCache() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doUpdateFlashGroupResponseCache occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doUpdateFlashGroupResponseCache occurred panic")
		}
	}()
	c.updateFlashGroupResponseCache()
}

func (c *Cluster) doCheckDataNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckDataNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckDataNodeHeartbeat occurred panic")
		}
	}()
	c.checkLeaderAddr()
	tasks := make([]*proto.AdminTask, 0)
	c.dataNodes.Range(func(addr, dataNode interface{}) bool {
		node := dataNode.(*DataNode)
		node.checkLiveness()
		task := node.createHeartbeatTask(c.masterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.addDataNodeTasks(tasks)
}

func (c *Cluster) doCheckMetaNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckMetaNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckMetaNodeHeartbeat occurred panic")
		}
	}()
	c.checkLeaderAddr()
	tasks := make([]*proto.AdminTask, 0)
	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.createHeartbeatTask(c.masterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.addMetaNodeTasks(tasks)
}

func (c *Cluster) doCheckEcNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckEcNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckEcNodeHeartbeat occurred panic")
		}
	}()
	c.checkLeaderAddr()
	tasks := make([]*proto.AdminTask, 0)
	c.ecNodes.Range(func(addr, ecNode interface{}) bool {
		node := ecNode.(*ECNode)
		node.checkLiveness()
		task := node.createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpEcNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addEcNodeTasks(tasks)
}

func (c *Cluster) doCheckCodecNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckCodecNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckCodecNodeHeartbeat occurred panic")
		}
	}()
	c.checkLeaderAddr()
	tasks := make([]*proto.AdminTask, 0)
	c.codecNodes.Range(func(addr, codecNode interface{}) bool {
		node := codecNode.(*CodecNode)
		task := createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpCodecNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addCodecNodeTasks(tasks)
}

func (c *Cluster) doCheckFlashNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckFlashNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckFlashNodeHeartbeat occurred panic")
		}
	}()
	c.checkLeaderAddr()
	tasks := make([]*proto.AdminTask, 0)
	c.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		task := node.createHeartbeatTask(c.masterAddr())
		tasks = append(tasks, task)
		return true
	})
	go c.syncProcessCheckFlashNodeHeartbeatTasks(tasks)
}

func (c *Cluster) doCheckAvailSpace() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckAvailSpace occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckAvailSpace occurred panic")
		}
	}()
	c.updateDataNodeStatInfo()
	c.updateMetaNodeStatInfo()
	c.updateEcNodeStatInfo()
	c.updateVolStatInfo()
	c.updateZoneStatInfo()
}

func (c *Cluster) doCheckMergeZoneNodeset() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doCheckMergeZoneNodeset occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doCheckMergeZoneNodeset occurred panic")
		}
	}()
	if !c.AutoMergeNodeSet {
		return
	}
	zones := c.t.getAllZones()
	for _, zone := range zones {
		if c.leaderHasChanged() {
			return
		}
		zone.mergeNodeSetForMetaNode(c)
		zone.mergeNodeSetForDataNode(c)
	}
}
