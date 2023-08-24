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

type snapshotDelManager struct {
	cluster              *Cluster
	lcSnapshotTaskStatus *lcSnapshotVerStatus
	lcNodeStatus         *lcNodeStatus
	idleNodeCh           chan struct{}
	exitCh               chan struct{}
}

func newSnapshotManager() *snapshotDelManager {
	log.LogInfof("action[newSnapshotManager] construct")
	snapshotMgr := &snapshotDelManager{
		lcSnapshotTaskStatus: NewLcSnapshotVerStatus(),
		lcNodeStatus:         NewLcNodeStatus(),
		idleNodeCh:           make(chan struct{}),
		exitCh:               make(chan struct{}),
	}
	return snapshotMgr
}

func (m *snapshotDelManager) process() {
	for {
		select {
		case <-m.exitCh:
			log.LogInfo("exitCh notified, snapshotDelManager process exit")
			return
		case <-m.idleNodeCh:
			log.LogDebug("idleLcNodeCh notified")

			taskId := m.lcSnapshotTaskStatus.GetOneTask()
			if taskId == "" {
				log.LogDebugf("no snapshot ver del task available")
				continue
			}

			nodeAddr := m.lcNodeStatus.GetIdleNode(taskId)
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode, redo task")
				m.lcSnapshotTaskStatus.RedoTask(taskId)
				continue
			}

			val, ok := m.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("lcNodes.Load, nodeAddr(%v) is not available, redo task", nodeAddr)
				m.lcSnapshotTaskStatus.RedoTask(m.lcNodeStatus.RemoveNode(nodeAddr))
				continue
			}

			node := val.(*LcNode)
			task := m.lcSnapshotTaskStatus.GetTaskFromScanning(taskId)
			if task == nil {
				log.LogErrorf("task is nil, release node(%v)", nodeAddr)
				m.lcNodeStatus.ReleaseNode(nodeAddr)
				continue
			}
			adminTask := node.createSnapshotVerDelTask(m.cluster.masterAddr(), task)
			m.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add snapshot version del task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
}

func (m *snapshotDelManager) notifyIdleLcNode() {
	m.lcSnapshotTaskStatus.RLock()
	defer m.lcSnapshotTaskStatus.RUnlock()

	if len(m.lcSnapshotTaskStatus.VerInfos) > 0 {
		select {
		case m.idleNodeCh <- struct{}{}:
			log.LogDebug("action[handleLcNodeHeartbeatResp], snapshotDelManager scan routine notified!")
		default:
			log.LogDebug("action[handleLcNodeHeartbeatResp], snapshotDelManager skipping notify!")
		}
	}
}

//----------------------------------------------

type lcSnapshotVerStatus struct {
	sync.RWMutex
	VerInfos           map[string]*proto.SnapshotVerDelTask
	ProcessingVerInfos map[string]*proto.SnapshotVerDelTask
	TaskResults        map[string]*proto.SnapshotVerDelTaskResponse
}

func NewLcSnapshotVerStatus() *lcSnapshotVerStatus {
	return &lcSnapshotVerStatus{
		VerInfos:           make(map[string]*proto.SnapshotVerDelTask, 0),
		ProcessingVerInfos: make(map[string]*proto.SnapshotVerDelTask, 0),
		TaskResults:        make(map[string]*proto.SnapshotVerDelTaskResponse, 0),
	}
}

func (vs *lcSnapshotVerStatus) GetOneTask() (taskId string) {
	var min int64 = math.MaxInt64

	vs.Lock()
	defer vs.Unlock()
	if len(vs.VerInfos) == 0 {
		return
	}

	for _, i := range vs.VerInfos {
		if i.VolVersionInfo.DelTime < min {
			min = i.VolVersionInfo.DelTime
			taskId = i.Id
		}
	}

	info := vs.VerInfos[taskId]
	delete(vs.VerInfos, taskId)
	info.UpdateTime = time.Now().UnixMicro()
	vs.ProcessingVerInfos[taskId] = info
	return
}

func (vs *lcSnapshotVerStatus) RedoTask(taskId string) {
	vs.Lock()
	defer vs.Unlock()
	if taskId == "" {
		return
	}

	if pInfo, ok := vs.ProcessingVerInfos[taskId]; ok {
		vs.VerInfos[taskId] = pInfo
		delete(vs.ProcessingVerInfos, taskId)
	}
}

func (vs *lcSnapshotVerStatus) DeleteScanningTask(taskId string) {
	vs.Lock()
	defer vs.Unlock()
	if taskId == "" {
		return
	}

	delete(vs.ProcessingVerInfos, taskId)
}

func (vs *lcSnapshotVerStatus) AddVerInfo(task *proto.SnapshotVerDelTask) {
	vs.Lock()
	defer vs.Unlock()
	if pInfo, ok := vs.ProcessingVerInfos[task.Id]; ok {
		if time.Since(time.UnixMicro(pInfo.UpdateTime)) < time.Second*time.Duration(5*defaultIntervalToCheck) {
			log.LogDebugf("VerInfo: %v is already in processing", task)
			return
		} else {
			log.LogWarnf("VerInfo: %v is in processing, but expired", task)
		}
	}

	vs.VerInfos[task.Id] = task
	log.LogDebugf("Add VerInfo task: %v", task)
}

func (vs *lcSnapshotVerStatus) AddResult(resp *proto.SnapshotVerDelTaskResponse) {
	vs.Lock()
	defer vs.Unlock()
	vs.TaskResults[resp.ID] = resp
}

func (vs *lcSnapshotVerStatus) DeleteOldResult() {
	vs.Lock()
	defer vs.Unlock()
	for k, v := range vs.TaskResults {
		if v.Done == true && time.Now().After(v.EndTime.Add(time.Hour*1)) {
			delete(vs.TaskResults, k)
			log.LogDebugf("delete old result: %v", v)
		}
	}
}

func (vs *lcSnapshotVerStatus) GetTaskFromScanning(taskId string) *proto.SnapshotVerDelTask {
	vs.Lock()
	defer vs.Unlock()
	return vs.ProcessingVerInfos[taskId]
}
