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
	"math"
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
	idleLcNodeCh     chan string
	exitCh           chan struct{}
}

func newLifecycleManager() *lifecycleManager {
	log.LogInfof("action[newLifecycleManager] construct")
	lcMgr := &lifecycleManager{
		lcConfigurations: make(map[string]*proto.LcConfiguration),
		lcNodeStatus:     newLcNodeStatus(),
		lcRuleTaskStatus: newLcRuleTaskStatus(),
		idleLcNodeCh:     make(chan string),
		exitCh:           make(chan struct{}),
	}
	return lcMgr
}

func (lcMgr *lifecycleManager) startLcScan() (success bool, msg string) {
	// stop if already scanning
	if lcMgr.scanning() {
		success = false
		msg = "startLcScan failed: scanning is not completed"
		log.LogWarn(msg)
		return
	}

	tasks := lcMgr.genEnabledRuleTasks()
	if len(tasks) <= 0 {
		success = false
		msg = "startLcScan: no enabled lifecycle rule task to schedule!"
		log.LogDebug(msg)
		return
	} else {
		log.LogDebugf("startLcScan: %v lifecycle rule tasks to schedule!", len(tasks))
	}

	// start scan init
	lcMgr.lcRuleTaskStatus = newLcRuleTaskStatus()
	for _, r := range tasks {
		lcMgr.lcRuleTaskStatus.ToBeScanned[r.Id] = r
	}

	go lcMgr.process()
	success = true
	return
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
	if len(lcMgr.lcRuleTaskStatus.ToBeScanned) > 0 {
		return true
	}

	lcMgr.lcRuleTaskStatus.RLock()
	for _, v := range lcMgr.lcRuleTaskStatus.Results {
		if v.Done != true && time.Now().Before(v.UpdateTime.Add(time.Minute*10)) {
			lcMgr.lcRuleTaskStatus.RUnlock()
			return true
		}
	}
	lcMgr.lcRuleTaskStatus.RUnlock()

	lcMgr.lcNodeStatus.RLock()
	for _, c := range lcMgr.lcNodeStatus.WorkingCount {
		if c > 0 {
			lcMgr.lcNodeStatus.RUnlock()
			return true
		}
	}
	lcMgr.lcNodeStatus.RUnlock()

	log.LogInfo("decide scanning: scanning finished!")
	return false
}

func (lcMgr *lifecycleManager) process() {
	log.LogInfof("lifecycleManager process start, rule num(%v)", len(lcMgr.lcRuleTaskStatus.ToBeScanned))
	now := time.Now()
	lcMgr.lcRuleTaskStatus.StartTime = &now
	for lcMgr.scanning() {
		log.LogDebugf("wait idleLcNodeCh... ToBeScanned num(%v)", len(lcMgr.lcRuleTaskStatus.ToBeScanned))
		select {
		case <-lcMgr.exitCh:
			log.LogInfo("exitCh notified, lifecycleManager process exit")
			return
		case idleNode := <-lcMgr.idleLcNodeCh:
			log.LogDebugf("idleLcNodeCh notified: %v", idleNode)

			// ToBeScanned -> Scanning
			task := lcMgr.lcRuleTaskStatus.GetOneTask()
			if task == nil {
				log.LogDebugf("lcRuleTaskStatus.GetOneTask, no task")
				continue
			}

			nodeAddr := lcMgr.lcNodeStatus.GetIdleNode(idleNode)
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode, redo task")
				lcMgr.lcRuleTaskStatus.RedoTask(task)
				continue
			}

			val, ok := lcMgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("lcNodes.Load, nodeAddr(%v) is not available, redo task", nodeAddr)
				lcMgr.lcNodeStatus.RemoveNode(nodeAddr)
				lcMgr.lcRuleTaskStatus.RedoTask(task)
				continue
			}

			node := val.(*LcNode)
			adminTask := node.createLcScanTask(lcMgr.cluster.masterAddr(), task)
			lcMgr.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add lifecycle scan task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
	end := time.Now()
	lcMgr.lcRuleTaskStatus.EndTime = &end
	log.LogInfof("lifecycleManager process finish, lcRuleTaskStatus results(%v)", lcMgr.lcRuleTaskStatus.Results)
}

func (lcMgr *lifecycleManager) notifyIdleLcNode(nodeAddr string) {
	select {
	case lcMgr.idleLcNodeCh <- nodeAddr:
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
	GetIdleNode(idleNode string) (nodeAddr string)
	RemoveNode(nodeAddr string)
	UpdateNode(nodeAddr string, count int)
}

// update status by heartbeat
type lcNodeStatus struct {
	sync.RWMutex
	WorkingCount map[string]int //ip:count, number of tasks being processed on this node
}

func newLcNodeStatus() *lcNodeStatus {
	return &lcNodeStatus{
		WorkingCount: make(map[string]int),
	}
}

func (ns *lcNodeStatus) GetIdleNode(idleNode string) (nodeAddr string) {
	ns.Lock()
	defer ns.Unlock()
	if len(ns.WorkingCount) == 0 {
		return
	}

	if idleNode != "" {
		nodeAddr = idleNode
		ns.WorkingCount[nodeAddr]++
		return
	}

	min := math.MaxInt
	for n, c := range ns.WorkingCount {
		if c < min {
			nodeAddr = n
			min = c
		}
		if c == 0 {
			break
		}
	}
	ns.WorkingCount[nodeAddr]++
	return
}

func (ns *lcNodeStatus) RemoveNode(nodeAddr string) {
	ns.Lock()
	defer ns.Unlock()
	delete(ns.WorkingCount, nodeAddr)
	return
}

func (ns *lcNodeStatus) UpdateNode(nodeAddr string, count int) {
	ns.Lock()
	defer ns.Unlock()
	ns.WorkingCount[nodeAddr] = count
	return
}

// -----------------------------------------------
type lcRuleTaskStatus struct {
	sync.RWMutex
	ToBeScanned map[string]*proto.RuleTask
	Results     map[string]*proto.LcNodeRuleTaskResponse
	StartTime   *time.Time
	EndTime     *time.Time
}

func newLcRuleTaskStatus() *lcRuleTaskStatus {
	return &lcRuleTaskStatus{
		ToBeScanned: make(map[string]*proto.RuleTask),
		Results:     make(map[string]*proto.LcNodeRuleTaskResponse),
	}
}

func (rs *lcRuleTaskStatus) GetOneTask() (task *proto.RuleTask) {
	rs.Lock()
	defer rs.Unlock()
	if len(rs.ToBeScanned) == 0 {
		return
	}

	for _, t := range rs.ToBeScanned {
		task = t
		break
	}

	delete(rs.ToBeScanned, task.Id)
	return
}

func (rs *lcRuleTaskStatus) RedoTask(task *proto.RuleTask) {
	rs.Lock()
	defer rs.Unlock()
	if task == nil {
		return
	}

	rs.ToBeScanned[task.Id] = task
}

func (rs *lcRuleTaskStatus) AddResult(resp *proto.LcNodeRuleTaskResponse) {
	rs.Lock()
	defer rs.Unlock()
	rs.Results[resp.ID] = resp
}
