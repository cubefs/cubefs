// Copyright 2023 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/util/log"
)

type lifecycleManager struct {
	sync.RWMutex
	cluster          *Cluster
	lcConfigurations map[string]*proto.LcConfiguration
	lcNodeStatus     *lcNodeStatus
	lcRuleTaskStatus *lcRuleTaskStatus
	idleLcNodeCh     chan struct{}
	exitCh           chan struct{}
}

type lcNodeStatus struct {
	sync.RWMutex
	idleNodes    map[string]string      //ip:ip
	workingNodes map[string]interface{} //ip:task
}

type lcRuleTaskStatus struct {
	sync.RWMutex
	Scanned     []*proto.RuleTask
	Scanning    []*proto.RuleTask
	ToBeScanned []*proto.RuleTask
	Results     map[string]*proto.LcNodeRuleTaskResponse
	StartTime   *time.Time
	EndTime     *time.Time
}

func newLifecycleManager() *lifecycleManager {
	log.LogInfof("action[newLifecycleManager] construct")
	lcMgr := &lifecycleManager{
		lcConfigurations: make(map[string]*proto.LcConfiguration),
		lcNodeStatus: &lcNodeStatus{
			workingNodes: make(map[string]interface{}),
			idleNodes:    make(map[string]string),
		},
		lcRuleTaskStatus: &lcRuleTaskStatus{
			Results: make(map[string]*proto.LcNodeRuleTaskResponse),
		},
		idleLcNodeCh: make(chan struct{}),
		exitCh:       make(chan struct{}),
	}
	return lcMgr
}

func (lcMgr *lifecycleManager) startLcScan() {
	// stop if already scanning
	if lcMgr.scanning() {
		log.LogWarnf("rescheduleScanRoutine: scanning is not completed, lcRuleTaskStatus(%v)", lcMgr.lcRuleTaskStatus)
		return
	}

	tasks := lcMgr.genEnabledRuleTasks()
	if len(tasks) <= 0 {
		log.LogDebugf("startLcScan: no enabled lifecycle rule task to schedule!")
		return
	} else {
		log.LogDebugf("startLcScan: %v lifecycle rule tasks to schedule!", len(tasks))
	}

	// start scan init
	lcMgr.lcRuleTaskStatus = &lcRuleTaskStatus{
		Results: make(map[string]*proto.LcNodeRuleTaskResponse),
	}
	lcMgr.lcNodeStatus.workingNodes = make(map[string]interface{})
	lcMgr.lcRuleTaskStatus.ToBeScanned = append(lcMgr.lcRuleTaskStatus.ToBeScanned, tasks...)

	go lcMgr.process()
}

// generate tasks for every bucket
func (lcMgr *lifecycleManager) genEnabledRuleTasks() []*proto.RuleTask {
	lcMgr.RLock()
	defer lcMgr.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range lcMgr.lcConfigurations {
		ts := v.GenEnabledRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (lcMgr *lifecycleManager) scanning() bool {
	log.LogInfof("scanning lcNodeStatus: %v", lcMgr.lcNodeStatus)
	if len(lcMgr.lcRuleTaskStatus.ToBeScanned) > 0 {
		return true
	}

	//handling exceptions
	if len(lcMgr.lcRuleTaskStatus.Scanning) > 0 {
		scanning := lcMgr.lcRuleTaskStatus.Scanning
		var nodes []string
		lcMgr.cluster.lcNodes.Range(func(addr, value interface{}) bool {
			nodes = append(nodes, addr.(string))
			return true
		})

		for _, task := range scanning {
			workingNodes := lcMgr.lcNodeStatus.workingNodes
			var node string
			for nodeAddr, t := range workingNodes {
				if task.Id == t.(*proto.RuleTask).Id {
					node = nodeAddr
				}
			}
			if exist(node, nodes) {
				log.LogInfof("scanning task: %v, exist node: %v, all nodes: %v", task, node, nodes)
				return true
			}
			log.LogInfof("scanning task: %v, but not exist node: %v, all nodes: %v", task, node, nodes)
		}
	}

	return false
}

func exist(node string, nodes []string) bool {
	for _, n := range nodes {
		if node == n {
			return true
		}
	}
	return false
}

func (lcMgr *lifecycleManager) process() {
	log.LogInfof("lifecycleManager process start, rule num(%v)", len(lcMgr.lcRuleTaskStatus.ToBeScanned))
	now := time.Now()
	lcMgr.lcRuleTaskStatus.StartTime = &now
	for lcMgr.scanning() {
		log.LogDebugf("wait idleLcNodeCh... ToBeScanned num(%v), Scanning num(%v)", len(lcMgr.lcRuleTaskStatus.ToBeScanned), len(lcMgr.lcRuleTaskStatus.Scanning))
		select {
		case <-lcMgr.exitCh:
			log.LogDebug("exitCh notified")
			return
		case <-lcMgr.idleLcNodeCh:
			log.LogDebug("idleLcNodeCh notified")

			// get ToBeScanned task
			task := lcMgr.lcRuleTaskStatus.getOneToBeScannedTask()
			if task == nil {
				log.LogWarn("no ToBeScanned task")
				continue
			}

			// get idle lcnode
			nodeAddr := lcMgr.lcNodeStatus.getOneIdleNode(task)
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode")
				continue
			}

			val, ok := lcMgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("nodeAddr(%v) is not available for scanning!", nodeAddr)
				t := lcMgr.lcNodeStatus.removeNode(nodeAddr)
				if t != nil {
					lcMgr.lcRuleTaskStatus.redoTask(t.(*proto.RuleTask))
				}
				continue
			}
			node := val.(*LcNode)
			adminTask := node.createLcScanTask(lcMgr.cluster.masterAddr(), task)
			lcMgr.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add lifecycle scan task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
	now = time.Now()
	lcMgr.lcRuleTaskStatus.EndTime = &now
	log.LogInfof("lifecycleManager process finish, lcRuleTaskStatus results(%v)", lcMgr.lcRuleTaskStatus.Results)
}

func (lcMgr *lifecycleManager) notifyIdleLcNode() {
	select {
	case lcMgr.idleLcNodeCh <- struct{}{}:
		log.LogDebug("action[handleLcNodeHeartbeatResp], lifecycleManager scan routine notified!")
	default:
		log.LogDebug("action[handleLcNodeHeartbeatResp], lifecycleManager skipping notify!")
	}
}

func (lcMgr *lifecycleManager) SetS3BucketLifecycle(lcConf *proto.LcConfiguration) error {
	lcMgr.Lock()
	defer lcMgr.Unlock()

	lcMgr.lcConfigurations[lcConf.VolName] = lcConf

	return nil
}

func (lcMgr *lifecycleManager) GetS3BucketLifecycle(VolName string) (lcConf *proto.LcConfiguration) {
	lcMgr.RLock()
	defer lcMgr.RUnlock()

	var ok bool
	lcConf, ok = lcMgr.lcConfigurations[VolName]
	if !ok {
		return nil
	}

	return lcConf
}

func (lcMgr *lifecycleManager) DelS3BucketLifecycle(VolName string) {
	lcMgr.Lock()
	defer lcMgr.Unlock()

	delete(lcMgr.lcConfigurations, VolName)
}

func (rs *lcRuleTaskStatus) getOneToBeScannedTask() (task *proto.RuleTask) {
	rs.Lock()
	defer rs.Unlock()
	if len(rs.ToBeScanned) > 0 {
		task = rs.ToBeScanned[0]
		rs.ToBeScanned = rs.ToBeScanned[1:]
		rs.Scanning = append(rs.Scanning, task)
	}
	return
}

func (rs *lcRuleTaskStatus) redoTask(task *proto.RuleTask) {
	rs.Lock()
	defer rs.Unlock()
	if task != nil {
		for i := 0; i < len(rs.Scanning); i++ {
			if rs.Scanning[i].Id == task.Id {
				rs.Scanning = append(rs.Scanning[:i], rs.Scanning[i+1:]...)
				break
			}
		}
		rs.ToBeScanned = append(rs.ToBeScanned, task)
	}
	return
}

func (rs *lcRuleTaskStatus) finishTask(task *proto.RuleTask, resp *proto.LcNodeRuleTaskResponse) {
	rs.Lock()
	defer rs.Unlock()
	if task != nil {
		for i := 0; i < len(rs.Scanning); i++ {
			if rs.Scanning[i].Id == task.Id {
				rs.Scanning = append(rs.Scanning[:i], rs.Scanning[i+1:]...)
				break
			}
		}
		rs.Scanned = append(rs.Scanned, task)
		rs.Results[resp.ID] = resp
	}
	return
}

func (rs *lcRuleTaskStatus) deleteScanningTask(task *proto.RuleTask) {
	rs.Lock()
	defer rs.Unlock()
	if task != nil {
		for i := 0; i < len(rs.Scanning); i++ {
			if rs.Scanning[i].Id == task.Id {
				rs.Scanning = append(rs.Scanning[:i], rs.Scanning[i+1:]...)
				break
			}
		}
	}
	return
}

func (ns *lcNodeStatus) getOneIdleNode(task interface{}) (nodeAddr string) {
	ns.Lock()
	defer ns.Unlock()
	if len(ns.idleNodes) > 0 {
		for n := range ns.idleNodes {
			nodeAddr = n
			delete(ns.idleNodes, nodeAddr)
			ns.workingNodes[nodeAddr] = task
			return
		}
	}
	return
}

func (ns *lcNodeStatus) removeNode(nodeAddr string) (task interface{}) {
	ns.Lock()
	defer ns.Unlock()
	task = ns.workingNodes[nodeAddr]
	delete(ns.idleNodes, nodeAddr)
	delete(ns.workingNodes, nodeAddr)
	return
}

func (ns *lcNodeStatus) releaseNode(nodeAddr string) (task interface{}) {
	ns.Lock()
	defer ns.Unlock()
	task = ns.workingNodes[nodeAddr]
	delete(ns.workingNodes, nodeAddr)
	ns.idleNodes[nodeAddr] = nodeAddr
	return task
}

func (ns *lcNodeStatus) getWorkingTask(nodeAddr string) (task interface{}) {
	ns.RLock()
	defer ns.RUnlock()
	return ns.workingNodes[nodeAddr]
}

//----------------------------------------------

type LcNodeStatInfo struct {
	Addr   string
	TaskId string
}

type LcNodeInfoResponse struct {
	Infos            []*LcNodeStatInfo
	LcConfigurations map[string]*proto.LcConfiguration
	LcRuleTaskStatus *lcRuleTaskStatus
	SnapshotInfos    *VolVerInfos
}

//-----------------------------------------------
