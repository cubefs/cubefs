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

func newLifecycleManager() *lifecycleManager {
	log.LogInfof("action[newLifecycleManager] construct")
	lcMgr := &lifecycleManager{
		lcConfigurations: make(map[string]*proto.LcConfiguration),
		lcNodeStatus:     NewLcNodeStatus(),
		lcRuleTaskStatus: NewLcRuleTaskStatus(),
		idleLcNodeCh:     make(chan struct{}),
		exitCh:           make(chan struct{}),
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
	lcMgr.lcNodeStatus.workingNodes = make(map[string]string)
	lcMgr.lcRuleTaskStatus = NewLcRuleTaskStatus()
	for _, r := range tasks {
		lcMgr.lcRuleTaskStatus.ToBeScanned[r.Id] = r
	}

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
				if task.Id == t {
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
			log.LogInfo("exitCh notified, lifecycleManager process exit")
			return
		case <-lcMgr.idleLcNodeCh:
			log.LogDebug("idleLcNodeCh notified")

			// ToBeScanned -> Scanning
			taskId := lcMgr.lcRuleTaskStatus.GetOneTask()
			if taskId == "" {
				log.LogWarn("lcRuleTaskStatus.GetOneTask, no task")
				continue
			}

			// idleNodes -> workingNodes
			nodeAddr := lcMgr.lcNodeStatus.GetIdleNode(taskId)
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode, redo task")
				lcMgr.lcRuleTaskStatus.RedoTask(taskId)
				continue
			}

			val, ok := lcMgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("lcNodes.Load, nodeAddr(%v) is not available, redo task", nodeAddr)
				lcMgr.lcRuleTaskStatus.RedoTask(lcMgr.lcNodeStatus.RemoveNode(nodeAddr))
				continue
			}

			node := val.(*LcNode)
			task := lcMgr.lcRuleTaskStatus.GetTaskFromScanning(taskId) // task can not be nil
			if task == nil {
				log.LogErrorf("task is nil, release node(%v)", nodeAddr)
				lcMgr.lcNodeStatus.ReleaseNode(nodeAddr)
				continue
			}
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

//-----------------------------------------------

type OpLcNode interface {
	GetIdleNode(taskId string) (nodeAddr string)
	RemoveNode(nodeAddr string) (taskId string)
	ReleaseNode(nodeAddr string) (taskId string)
}

type lcNodeStatus struct {
	sync.RWMutex
	idleNodes    map[string]string //ip:ip
	workingNodes map[string]string //ip:taskId
}

func NewLcNodeStatus() *lcNodeStatus {
	return &lcNodeStatus{
		idleNodes:    make(map[string]string),
		workingNodes: make(map[string]string),
	}
}

func (ns *lcNodeStatus) GetIdleNode(taskId string) (nodeAddr string) {
	ns.Lock()
	defer ns.Unlock()
	if len(ns.idleNodes) == 0 {
		return
	}

	for n := range ns.idleNodes {
		nodeAddr = n
		delete(ns.idleNodes, nodeAddr)
		ns.workingNodes[nodeAddr] = taskId
		return
	}
	return
}

func (ns *lcNodeStatus) RemoveNode(nodeAddr string) (taskId string) {
	ns.Lock()
	defer ns.Unlock()
	taskId = ns.workingNodes[nodeAddr]
	delete(ns.idleNodes, nodeAddr)
	delete(ns.workingNodes, nodeAddr)
	return
}

func (ns *lcNodeStatus) ReleaseNode(nodeAddr string) (taskId string) {
	ns.Lock()
	defer ns.Unlock()
	taskId = ns.workingNodes[nodeAddr]
	delete(ns.workingNodes, nodeAddr)
	ns.idleNodes[nodeAddr] = nodeAddr
	return
}

//-----------------------------------------------

type OpLcTask interface {
	GetOneTask() (taskId string)
	RedoTask(taskId string)
	DeleteScanningTask(taskId string)
}

type lcRuleTaskStatus struct {
	sync.RWMutex
	ToBeScanned map[string]*proto.RuleTask
	Scanning    map[string]*proto.RuleTask
	Results     map[string]*proto.LcNodeRuleTaskResponse
	StartTime   *time.Time
	EndTime     *time.Time
}

func NewLcRuleTaskStatus() *lcRuleTaskStatus {
	return &lcRuleTaskStatus{
		ToBeScanned: make(map[string]*proto.RuleTask),
		Scanning:    make(map[string]*proto.RuleTask),
		Results:     make(map[string]*proto.LcNodeRuleTaskResponse),
	}
}

func (rs *lcRuleTaskStatus) GetOneTask() (taskId string) {
	rs.Lock()
	defer rs.Unlock()
	if len(rs.ToBeScanned) == 0 {
		return
	}

	for k, v := range rs.ToBeScanned {
		taskId = k
		rs.Scanning[k] = v
		delete(rs.ToBeScanned, k)
		return
	}
	return
}

func (rs *lcRuleTaskStatus) RedoTask(taskId string) {
	rs.Lock()
	defer rs.Unlock()
	if taskId == "" {
		return
	}

	if task, ok := rs.Scanning[taskId]; ok {
		rs.ToBeScanned[taskId] = task
		delete(rs.Scanning, taskId)
	}
}

func (rs *lcRuleTaskStatus) DeleteScanningTask(taskId string) {
	rs.Lock()
	defer rs.Unlock()
	if taskId == "" {
		return
	}

	delete(rs.Scanning, taskId)
}

func (rs *lcRuleTaskStatus) AddResult(resp *proto.LcNodeRuleTaskResponse) {
	rs.Lock()
	defer rs.Unlock()
	rs.Results[resp.ID] = resp
}

func (rs *lcRuleTaskStatus) GetTaskFromScanning(taskId string) *proto.RuleTask {
	rs.Lock()
	defer rs.Unlock()
	return rs.Scanning[taskId]
}
