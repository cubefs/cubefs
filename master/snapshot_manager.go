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

type LcVerInfo struct {
	proto.VerInfo
	dTime time.Time
}

type ProcessingVerInfo struct {
	LcVerInfo
	updateTime time.Time
}

type VolVerInfos struct {
	sync.RWMutex
	VerInfos           map[string]*LcVerInfo
	ProcessingVerInfos map[string]*ProcessingVerInfo
	TaskResults        map[string]*proto.SnapshotVerDelTaskResponse
}

func (vvi *VolVerInfos) AddVerInfo(vi *LcVerInfo) {
	vvi.Lock()
	defer vvi.Unlock()
	if pInfo, ok := vvi.ProcessingVerInfos[vi.Key()]; ok {
		if time.Since(pInfo.updateTime) < time.Second*time.Duration(3*defaultIntervalToCheck) {
			log.LogDebugf("VerInfo: %v is already in processing", vi)
			return
		} else {
			log.LogWarnf("VerInfo: %v is expired", vi)
		}
	}

	vvi.VerInfos[vi.Key()] = vi
	log.LogDebugf("Adding VerInfo: %v", vi)
}

func (vvi *VolVerInfos) RemoveProcessingVerInfo(verInfoKey string) (pInfo *ProcessingVerInfo) {
	vvi.Lock()
	defer vvi.Unlock()
	var ok bool
	if pInfo, ok = vvi.ProcessingVerInfos[verInfoKey]; !ok {
		return
	} else {
		delete(vvi.ProcessingVerInfos, verInfoKey)
	}
	return
}

func (vvi *VolVerInfos) RemoveVerInfo(verInfoKey string) {
	vvi.Lock()
	defer vvi.Unlock()
	delete(vvi.VerInfos, verInfoKey)
	delete(vvi.ProcessingVerInfos, verInfoKey)
}

func (vvi *VolVerInfos) FetchOldestVerInfo() (info *LcVerInfo) {
	var min int64 = math.MaxInt64
	minKey := ""

	vvi.Lock()
	defer vvi.Unlock()

	if len(vvi.VerInfos) == 0 {
		return nil
	}

	for _, i := range vvi.VerInfos {
		if i.dTime.Unix() < min {
			min = i.dTime.Unix()
			minKey = i.Key()
		}
	}
	info = vvi.VerInfos[minKey]
	delete(vvi.VerInfos, minKey)

	pInfo := &ProcessingVerInfo{
		LcVerInfo:  *info,
		updateTime: time.Now(),
	}
	vvi.ProcessingVerInfos[minKey] = pInfo
	return
}

func (vvi *VolVerInfos) RedoProcessingVerInfo(verInfoKey string) {
	vvi.Lock()
	defer vvi.Unlock()

	log.LogInfof("RedoProcessingVerInfo, verinfo: %v", verInfoKey)
	if pInfo, ok := vvi.ProcessingVerInfos[verInfoKey]; ok {
		vvi.VerInfos[verInfoKey] = &pInfo.LcVerInfo
		delete(vvi.ProcessingVerInfos, verInfoKey)
	}
}

type snapshotDelManager struct {
	cluster      *Cluster
	volVerInfos  *VolVerInfos
	lcNodeStatus *lcNodeStatus
	idleNodeCh   chan struct{}
	exitCh       chan struct{}
}

func newSnapshotManager() *snapshotDelManager {
	log.LogInfof("action[newSnapshotManager] construct")
	snapshotMgr := &snapshotDelManager{
		volVerInfos: &VolVerInfos{
			VerInfos:           make(map[string]*LcVerInfo, 0),
			ProcessingVerInfos: make(map[string]*ProcessingVerInfo, 0),
			TaskResults:        make(map[string]*proto.SnapshotVerDelTaskResponse, 0),
		},
		lcNodeStatus: &lcNodeStatus{
			workingNodes: make(map[string]interface{}),
			idleNodes:    make(map[string]string),
		},
		idleNodeCh: make(chan struct{}),
		exitCh:     make(chan struct{}),
	}
	return snapshotMgr
}

func (m *snapshotDelManager) Start() {
	go m.process()
}

func (m *snapshotDelManager) process() {
	for {
		select {
		case <-m.exitCh:
			log.LogInfo("snapshotDelManager process exit")
			return
		case <-m.idleNodeCh:
			vi := m.volVerInfos.FetchOldestVerInfo()
			if vi == nil {
				log.LogDebugf("no snapshot ver del task available")
				continue
			}

			nodeAddr := m.lcNodeStatus.getOneIdleNode(vi.Key())
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode")
				continue
			}

			val, ok := m.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("nodeAddr(%v) is not available for scanning!", nodeAddr)
				t := m.lcNodeStatus.removeNode(nodeAddr)
				m.volVerInfos.RedoProcessingVerInfo(t.(string))
				continue
			}
			task := &proto.SnapshotVerDelTask{
				VerInfo: proto.VerInfo{
					VolName: vi.VolName,
					VerSeq:  vi.VerSeq,
				},
			}
			node := val.(*LcNode)
			adminTask := node.createSnapshotVerDelTask(m.cluster.masterAddr(), task)
			m.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add snapshot version del task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
}

func (m *snapshotDelManager) notifyIdleLcNode() {
	m.volVerInfos.RLock()
	defer m.volVerInfos.RUnlock()

	if len(m.volVerInfos.VerInfos) > 0 {
		select {
		case m.idleNodeCh <- struct{}{}:
			log.LogDebug("action[handleLcNodeHeartbeatResp], snapshotDelManager scan routine notified!")
		default:
			log.LogDebug("action[handleLcNodeHeartbeatResp], snapshotDelManager skipping notify!")
		}
	}
}
