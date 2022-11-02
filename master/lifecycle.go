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
	"github.com/cubefs/cubefs/util/errors"
	"math"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

//type scanTaskInfo struct {
//	Id        string
//	routineId int64
//}

type lcnodeTaskInfo interface{}

type nodesState struct {
	sync.RWMutex
	//workingNodes map[string]map[string]*proto.S3ScanTaskInfo
	workingNodes map[string]map[string]lcnodeTaskInfo
	idleNodes    map[string]string
}

func (ns *nodesState) getOneIdleNode() (nodeAddr string, err error) {
	if len(ns.idleNodes) > 0 {
		for n := range ns.idleNodes {
			nodeAddr = n
			break
		}
	} else {
		return "", errors.New("no idle nodes available")
	}
	return nodeAddr, nil
}

//func (ns *nodesState) addTaskToNode(info *proto.S3ScanTaskInfo, nodeAddr string) {
func (ns *nodesState) addTaskToNode(info lcnodeTaskInfo, nodeAddr string) {

	switch t := info.(type) {
	case *proto.S3ScanTaskInfo:
		if taskMap, ok := ns.workingNodes[nodeAddr]; !ok {
			taskMap = make(map[string]lcnodeTaskInfo, 0)
			taskMap[t.Id] = info
			ns.workingNodes[nodeAddr] = taskMap
		} else {
			if _, ok = taskMap[t.Id]; !ok {
				taskMap[t.Id] = info
			} else {
				log.LogWarnf("s3 scan task(%v) in routine(%v) is already running in nodeAddr(%v)", t.Id, t.RoutineId, nodeAddr)
			}
		}
	case *proto.DelVerTaskInfo:
		if taskMap, ok := ns.workingNodes[nodeAddr]; !ok {
			taskMap = make(map[string]lcnodeTaskInfo, 0)
			taskMap[t.Id] = info
			ns.workingNodes[nodeAddr] = taskMap
		} else {
			if _, ok = taskMap[t.Id]; !ok {
				taskMap[t.Id] = info
			} else {
				log.LogWarnf("delete vol version task(%v) is already running in nodeAddr(%v)", t.Id, nodeAddr)
			}
		}
	default:
		log.LogError("Unknown task type: %T", t)
	}

}

func (ns *nodesState) IsNodeBusy(nodeAddr string) bool {
	ns.RLock()
	defer ns.RUnlock()
	var working, idle bool
	_, working = ns.workingNodes[nodeAddr]
	_, idle = ns.idleNodes[nodeAddr]
	return working && !idle
}

//func (ns *nodesState) AddTaskToNode(info *proto.S3ScanTaskInfo, maxConcurrentLcNodes uint64) (nodeAddr string, err error) {
func (ns *nodesState) AddTaskToNode(info lcnodeTaskInfo, maxConcurrentLcNodes uint64) (nodeAddr string, err error) {
	ns.Lock()
	defer ns.Unlock()

	nodeAddr, err = ns.getOneIdleNode()
	if err != nil {
		return nodeAddr, err
	}
	if _, ok := info.(*proto.S3ScanTaskInfo); ok && maxConcurrentLcNodes > 0 {
		if len(ns.workingNodes) >= int(maxConcurrentLcNodes) {
			return "", errors.New("max concurrent lcnodes reached")
		}
	}

	delete(ns.idleNodes, nodeAddr)
	ns.addTaskToNode(info, nodeAddr)
	return
}

//func (ns *nodesState) UpdateNodeTask(nodeAddr string, infos []*proto.S3ScanTaskInfo) {
func (ns *nodesState) UpdateNodeTask(nodeAddr string, infos []lcnodeTaskInfo) {
	if len(infos) == 0 {
		ns.Lock()
		delete(ns.workingNodes, nodeAddr)
		ns.idleNodes[nodeAddr] = nodeAddr
		ns.Unlock()
		return
	}
	ns.Lock()
	defer ns.Unlock()

	if _, ok := ns.workingNodes[nodeAddr]; !ok {
		for _, info := range infos {
			ns.addTaskToNode(info, nodeAddr)
		}
		delete(ns.idleNodes, nodeAddr)
	} else {
		delete(ns.workingNodes, nodeAddr)
		for _, info := range infos {
			ns.addTaskToNode(info, nodeAddr)
		}

	}

}

//func (ns *nodesState) ReleaseTask(taskId string, RoutineID int64) {
//	ns.Lock()
//	defer ns.Unlock()
//	for nodeAddr, taskMap := range ns.workingNodes {
//		if info, ok := taskMap[taskId]; ok {
//			if RoutineID == info.RoutineId {
//				delete(taskMap, taskId)
//				log.LogInfof("task(%v) in routine(%v) is released", taskId, RoutineID)
//				if len(taskMap) == 0 {
//					delete(ns.workingNodes, nodeAddr)
//					ns.idleNodes[nodeAddr] = nodeAddr
//					log.LogInfof("lcnode(%v) is set idle", nodeAddr)
//				}
//			}
//
//		}
//	}
//}

func (ns *nodesState) ReleaseTask(info lcnodeTaskInfo) {
	ns.Lock()
	defer ns.Unlock()
	for nodeAddr, taskMap := range ns.workingNodes {
		switch t := info.(type) {
		case *proto.S3ScanTaskInfo:
			if i, ok := taskMap[t.Id]; ok {
				if it, ok := i.(*proto.S3ScanTaskInfo); ok {
					if t.RoutineId == it.RoutineId {
						delete(taskMap, t.Id)
						log.LogInfof("s3 task(%v) in routine(%v) is released", t.Id, t.RoutineId)
						if len(taskMap) == 0 {
							delete(ns.workingNodes, nodeAddr)
							ns.idleNodes[nodeAddr] = nodeAddr
							log.LogInfof("lcnode(%v) is set idle", nodeAddr)
						}
					}
				}
			}
		case *proto.DelVerTaskInfo:
			if i, ok := taskMap[t.Id]; ok {
				if _, ok := i.(*proto.DelVerTaskInfo); ok {
					delete(taskMap, t.Id)
					log.LogInfof("snap task(%v) is released", t.Id)
					if len(taskMap) == 0 {
						delete(ns.workingNodes, nodeAddr)
						ns.idleNodes[nodeAddr] = nodeAddr
						log.LogInfof("lcnode(%v) is set idle", nodeAddr)
					}
				}
			}
		default:
			log.LogError("Unknown task type: %T", t)
		}
	}
}

func (ns *nodesState) RemoveNode(nodeAddr string) (taskInfos []lcnodeTaskInfo) {
	ns.Lock()
	defer ns.Unlock()

	if taskMap, ok := ns.workingNodes[nodeAddr]; ok {
		for _, info := range taskMap {
			taskInfos = append(taskInfos, info)
		}
	}
	log.LogInfof("RemoveNode, %v tasks [%v] are still in processing on %v", len(taskInfos), taskInfos, nodeAddr)
	delete(ns.idleNodes, nodeAddr)
	delete(ns.workingNodes, nodeAddr)
	return
}

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
	VerInfos map[string]*LcVerInfo
	//ProcessingVerInfos map[string]*LcVerInfo
	ProcessingVerInfos map[string]*ProcessingVerInfo
	TaskResults        map[string]*proto.SnapshotVerDelTaskResponse
}

func (vvi *VolVerInfos) AddVerInfo(vi *LcVerInfo) {
	vvi.Lock()
	defer vvi.Unlock()
	if pInfo, ok := vvi.ProcessingVerInfos[vi.Key()]; ok {
		if time.Since(pInfo.updateTime) < time.Second*time.Duration(defaultNodeTimeOutSec) {
			log.LogDebugf("VerInfo: %v is already in processing", vi)
			return
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

func (vvi *VolVerInfos) ReturnProcessingVerInfo(keys []string) {
	vvi.Lock()
	defer vvi.Unlock()

	log.LogInfof("ReturnProcessingVerInfo, verinfo: %v", keys)
	for _, k := range keys {
		if pInfo, ok := vvi.ProcessingVerInfos[k]; ok {
			vvi.VerInfos[k] = &pInfo.LcVerInfo
			delete(vvi.ProcessingVerInfos, k)
			log.LogInfof("ReturnProcessingVerInfo, remove %v from processing", vvi.VerInfos[k])
		}
	}

}

/*func (vvi *VolVerInfos) ReturnProcessingVerInfo(info *LcVerInfo) {
	vvi.Lock()
	defer vvi.Unlock()
	if _, ok := vvi.VerInfos[info.Key()]; !ok {
		vvi.VerInfos[info.Key()] = info
	}
	delete(vvi.ProcessingVerInfos, info.Key())
}*/

type snapshotDelManager struct {
	volVerInfos VolVerInfos
	idleNodeCh  chan struct{}
	exitCh      chan struct{}
	lcMgr       *lifecycleManager
}

//func (m *snapshotDelManager) AddVerInfo(vi *LcVerInfo) {
//	m.lcMgr.snapshotMgr.volVerInfos.Lock()
//	defer m.lcMgr.snapshotMgr.volVerInfos.Unlock()
//	if _, ok := m.lcMgr.snapshotMgr.volVerInfos.ProcessingVerInfos[vi.Key()]; ok {
//		if m.isVerInfoInProcessing(vi) {
//			log.LogDebugf("VerInfo: %v is already in processing", vi)
//			return
//		} else {
//			log.LogWarnf("VerInfo: %v is not in processing", vi)
//		}
//
//	}
//	m.lcMgr.snapshotMgr.volVerInfos.VerInfos[vi.Key()] = vi
//	log.LogDebugf("Adding VerInfo: %v", vi)
//}

//func (m *snapshotDelManager) isVerInfoInProcessing(vi *LcVerInfo) bool {
//	m.lcMgr.lnStates.RLock()
//	defer m.lcMgr.lnStates.RUnlock()
//	for nodeAddr, taskMap := range m.lcMgr.lnStates.workingNodes {
//		if _, ok := taskMap[vi.Key()]; ok {
//			log.LogDebugf("task %v is already in processing on %v", vi, nodeAddr)
//			return true
//		}
//	}
//	return false
//}

func (m *snapshotDelManager) NotifyIdleLcNode() {
	m.volVerInfos.RLock()
	defer m.volVerInfos.RUnlock()

	if len(m.volVerInfos.VerInfos) > 0 {
		select {
		case m.idleNodeCh <- struct{}{}:
			log.LogDebugf("snapshotDelManager NotifyIdleLcNode, scan routine notified!")
		default:
			log.LogDebugf("snapshotDelManager NotifyIdleLcNode, skipping notify!")
		}
	}

}

func (m *snapshotDelManager) Start() {
	go m.process()
}

func (m *snapshotDelManager) Stop() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("snapshot process Stop,err:%v", r)
		}
	}()

	close(m.exitCh)
}

func (m *snapshotDelManager) process() {
	defer func() {
		log.LogDebugf("snapshot process exit")
	}()
	for {
		select {
		case <-m.exitCh:
			return
		case <-m.idleNodeCh:
			aTasks := make([]*proto.AdminTask, 0)
			vi := m.volVerInfos.FetchOldestVerInfo()
			if vi == nil {
				log.LogDebugf("no snapshot ver del task available")
				break
			}

			info := &proto.DelVerTaskInfo{Id: vi.Key()}

			nodeAddr, err := m.lcMgr.lnStates.AddTaskToNode(info, 0)
			if err != nil {
				log.LogWarnf("AddTaskToNode failed: err(%v)", err)
				rtTasks := make([]string, 0)
				rtTasks = append(rtTasks, vi.Key())
				m.volVerInfos.ReturnProcessingVerInfo(rtTasks)
				break
			}

			val, ok := m.lcMgr.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("nodeAddr(%v) is not available for scanning!", nodeAddr)
				m.lcMgr.lnStates.RemoveNode(nodeAddr)
				break
			}

			task := &proto.SnapshotVerDelTask{
				VerInfo: proto.VerInfo{
					VolName: vi.VolName,
					VerSeq:  vi.VerSeq,
				},
			}
			node := val.(*LcNode)
			aTask := node.createSnapshotVerDelTask(task, m.lcMgr.cluster.masterAddr())
			aTasks = append(aTasks, aTask)
			m.lcMgr.cluster.addLcNodeTasks(aTasks)
			log.LogDebugf("add snapshot version del task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
}

type lifecycleManager struct {
	cluster *Cluster
	//lcnodes
	lnMutex sync.RWMutex
	lcNodes sync.Map
	//lcnode working state
	lnStates *nodesState
	//s3 lifecycle configuration
	s3ConfMutex sync.RWMutex
	s3LcConfs   map[string]*proto.LcConfiguration
	//s3 scan routine
	scanMutex sync.RWMutex
	lcScan    *scanRoutine
	//snapshot
	snapshotMgr *snapshotDelManager
}

//type RuleTask struct {
//	Id   string
//	Rule *proto.Rule
//}

type lcRuleStatus struct {
	Scanned     []*proto.RuleTask
	Scanning    []*proto.RuleTask
	ToBeScanned []*proto.RuleTask
	//Results     []*proto.RuleTaskResponse
	Results map[string]*proto.RuleTaskResponse
}

type scanRoutine struct {
	sync.RWMutex
	s3LcMgr    *lifecycleManager
	Id         int64
	stopped    bool
	StartTime  *time.Time
	EndTime    *time.Time
	RuleStatus lcRuleStatus
	exitCh     chan struct{}
	idleNodeCh chan struct{}
}

func newLifecycleManager() *lifecycleManager {
	log.LogInfof("action[newLifecycleManager] construct")
	lcMgr := &lifecycleManager{
		s3LcConfs: make(map[string]*proto.LcConfiguration),
		lnStates: &nodesState{
			workingNodes: make(map[string]map[string]lcnodeTaskInfo),
			idleNodes:    make(map[string]string),
		},
	}

	lcMgr.snapshotMgr = &snapshotDelManager{
		volVerInfos: VolVerInfos{
			VerInfos:           make(map[string]*LcVerInfo, 0),
			ProcessingVerInfos: make(map[string]*ProcessingVerInfo, 0),
			TaskResults:        make(map[string]*proto.SnapshotVerDelTaskResponse, 0),
		},
		exitCh:     make(chan struct{}),
		idleNodeCh: make(chan struct{}),
		lcMgr:      lcMgr,
	}

	lcMgr.snapshotMgr.Start()

	return lcMgr
}

func newScanRoutine(s3LcMgr *lifecycleManager) *scanRoutine {
	lcScan := &scanRoutine{
		stopped: true,
		s3LcMgr: s3LcMgr,
		RuleStatus: lcRuleStatus{
			Scanned:     make([]*proto.RuleTask, 0),
			Scanning:    make([]*proto.RuleTask, 0),
			ToBeScanned: make([]*proto.RuleTask, 0),
			Results:     make(map[string]*proto.RuleTaskResponse, 0),
		},
		exitCh:     make(chan struct{}),
		idleNodeCh: make(chan struct{}),
	}
	return lcScan
}

func (s *scanRoutine) Start(tasks []*proto.RuleTask) {
	t := time.Now()
	s.StartTime = &t
	s.Id = t.Unix()

	s.RuleStatus.ToBeScanned = append(s.RuleStatus.ToBeScanned, tasks...)
	s.stopped = false
	ruleNum := len(s.RuleStatus.ToBeScanned)
	log.LogInfof("Start scan, rule num(%v)", ruleNum)
	go s.process()
}

func (s *scanRoutine) redoTasks(taskInfos []*proto.S3ScanTaskInfo) {
	s.Lock()
	defer s.Unlock()
	for _, info := range taskInfos {
		if info.RoutineId == s.Id {
			var task *proto.RuleTask
			for i := 0; i < len(s.RuleStatus.Scanning); i++ {
				if s.RuleStatus.Scanning[i].Id == info.Id {
					task = s.RuleStatus.Scanning[i]
					s.RuleStatus.Scanning = append(s.RuleStatus.Scanning[:i], s.RuleStatus.Scanning[i+1:]...)
					break
				}
			}
			s.RuleStatus.ToBeScanned = append(s.RuleStatus.ToBeScanned, task)
		} else {
			log.LogWarnf("redoTasks: skip expired task(%v) in routine(%v), current scan routine(%v)",
				info.Id, info.RoutineId, s.Id)
			continue
		}
	}
}

func (s *scanRoutine) process() {
	defer func() {
		log.LogDebugf("scan routine(%v) process exit", s.Id)
	}()
	for {
		select {
		case <-s.exitCh:
			return
		case <-s.idleNodeCh:
			log.LogDebugf("scan routine notified")
			s.Lock()
			for len(s.RuleStatus.ToBeScanned) > 0 {
				aTasks := make([]*proto.AdminTask, 0)

				var task = s.RuleStatus.ToBeScanned[0]

				info := &proto.S3ScanTaskInfo{
					Id:        task.Id,
					RoutineId: s.Id,
				}
				maxConcurrentLcNodes := s.s3LcMgr.cluster.cfg.MaxConcurrentLcNodes
				nodeAddr, err := s.s3LcMgr.lnStates.AddTaskToNode(info, maxConcurrentLcNodes)
				if err != nil {
					log.LogWarnf("scan routine: err(%v)", err)
					break
				}

				val, ok := s.s3LcMgr.lcNodes.Load(nodeAddr)
				if !ok {
					log.LogErrorf("nodeAddr(%v) is not available for scanning!", nodeAddr)
					s.s3LcMgr.lnStates.RemoveNode(nodeAddr)
					break
				}

				s.RuleStatus.ToBeScanned = s.RuleStatus.ToBeScanned[1:]
				s.RuleStatus.Scanning = append(s.RuleStatus.Scanning, task)

				node := val.(*LcNode)
				aTask := node.createLcScanTask(s.s3LcMgr.lcScan.Id, task, s.s3LcMgr.cluster.masterAddr())
				aTasks = append(aTasks, aTask)

				s.s3LcMgr.cluster.addLcNodeTasks(aTasks)
				log.LogDebugf("scan routine add lifecycle scan task(%v) to lcnode(%v) in routine(%v)", *task, nodeAddr, s.Id)

				if len(s.RuleStatus.ToBeScanned) == 0 {
					s.Unlock()
					log.LogDebugf("scan routine all rule task is scheduled!")
					return
				}
			}
			s.Unlock()
		}
	}
}

func (s *scanRoutine) Scanning() bool {
	s.RLock()
	defer s.RUnlock()
	return s.scanning() && !s.stopped
}

func (s *scanRoutine) scanning() bool {
	return len(s.RuleStatus.ToBeScanned) != 0 || len(s.RuleStatus.Scanning) != 0
}

func (s *scanRoutine) printStatistics(tobeScanned, scanning []string) {
	var totalInodeScannedNum int64
	var FileScannedNum int64
	var DirScannedNum int64
	var ExpiredNum int64
	var ErrorSkippedNum int64
	var AbortedIncompleteMultipartNum int64
	for _, r := range s.RuleStatus.Results {
		totalInodeScannedNum += r.TotalInodeScannedNum
		FileScannedNum += r.FileScannedNum
		DirScannedNum += r.DirScannedNum
		ExpiredNum += r.ExpiredNum
		ErrorSkippedNum += r.ErrorSkippedNum
		AbortedIncompleteMultipartNum += r.AbortedIncompleteMultipartNum
	}

	if len(tobeScanned) == 0 && len(scanning) == 0 {
		log.LogInfof("Expiration scan for routine(%v) is completed, startTime(%v), endTime(%v), totalScanned(%v), ExpiredNum(%v), AbortedIncompleteMultipartNum(%v)",
			s.Id, s.StartTime.String(), s.EndTime.String(), totalInodeScannedNum, ExpiredNum, AbortedIncompleteMultipartNum)
	} else {
		log.LogWarnf("Expiration scan for routine(%v) is not completed, startTime(%v), endTime(%v), (%v) tasks are not started, (%v) tasks are scanning, totalScanned(%v), ExpiredNum(%v), AbortedIncompleteMultipartNum(%v))",
			s.Id, s.StartTime.String(), s.EndTime.String(), len(tobeScanned), len(scanning), totalInodeScannedNum, ExpiredNum, AbortedIncompleteMultipartNum)
	}

}

func (s *scanRoutine) Stop() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("scanRoutine Stop,err:%v", r)
		}
	}()

	s.Lock()
	defer s.Unlock()

	tobeScanned := make([]string, 0)
	scanning := make([]string, 0)
	if !s.stopped {
		t := time.Now()
		s.EndTime = &t
		if s.scanning() {
			for _, t := range s.RuleStatus.ToBeScanned {
				tobeScanned = append(tobeScanned, t.Id)
			}
			for _, t := range s.RuleStatus.Scanning {
				scanning = append(scanning, t.Id)
			}
			s.RuleStatus.ToBeScanned = nil
			s.RuleStatus.Scanning = nil
		}
		close(s.exitCh)
		close(s.idleNodeCh)
		s.stopped = true
		s.printStatistics(tobeScanned, scanning)
	}

	log.LogInfof("Scan routine(%v) stopped", s.Id)
}

func (lcMgr *lifecycleManager) genRuleTasks() []*proto.RuleTask {
	lcMgr.s3ConfMutex.RLock()
	defer lcMgr.s3ConfMutex.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range lcMgr.s3LcConfs {
		ts := v.GenRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (lcMgr *lifecycleManager) setNewScanRoutine(scan *scanRoutine) {
	lcMgr.scanMutex.RLock()
	oldScan := lcMgr.lcScan
	lcMgr.scanMutex.RUnlock()
	if oldScan != nil {
		oldScan.Stop()
		log.LogInfof("setNewScanRoutine: old routine(%v) is stopped!", oldScan.Id)
	}

	lcMgr.scanMutex.Lock()
	lcMgr.lcScan = scan
	lcMgr.scanMutex.Unlock()
	log.LogInfof("setNewScanRoutine: new routine(%v) is started!", scan.Id)
}

func (lcMgr *lifecycleManager) getScanRoutine() *scanRoutine {
	lcMgr.scanMutex.RLock()
	defer lcMgr.scanMutex.RUnlock()
	return lcMgr.lcScan
}

func (lcMgr *lifecycleManager) NotifyIdleLcNode() {
	//snapshot version delete first
	lcMgr.snapshotMgr.NotifyIdleLcNode()

	lcScan := lcMgr.getScanRoutine()
	if lcScan != nil {
		select {
		case lcScan.idleNodeCh <- struct{}{}:
			log.LogDebugf("action[handleLcNodeHeartbeatResp], scan routine notified!")
		default:
			log.LogDebugf("action[handleLcNodeHeartbeatResp], skipping notify looper")
		}
	}
}

func (lcMgr *lifecycleManager) genEnabledRuleTasks() []*proto.RuleTask {
	lcMgr.s3ConfMutex.RLock()
	defer lcMgr.s3ConfMutex.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range lcMgr.s3LcConfs {
		ts := v.GenEnabledRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (lcMgr *lifecycleManager) rescheduleScanRoutine() {
	tasks := lcMgr.genEnabledRuleTasks()

	if len(tasks) <= 0 {
		log.LogDebugf("rescheduleScanRoutine: no enabled lifecycle rule task to schedule!")
		return
	} else {
		log.LogDebugf("rescheduleScanRoutine: %v lifecycle rule tasks to schedule!", len(tasks))
	}

	lcScan := lcMgr.getScanRoutine()

	if lcScan != nil && lcScan.Scanning() {
		lcScan.RLock()
		log.LogWarnf("rescheduleScanRoutine: scanning is not completed, lcScan(%v)", lcScan)
		toBeScannedMap := make(map[string]*proto.RuleTask, 0)
		ScannedMap := make(map[string]*proto.RuleTask, 0)
		ScanningMap := make(map[string]*proto.RuleTask, 0)
		for _, ruleTask := range lcScan.RuleStatus.ToBeScanned {
			toBeScannedMap[ruleTask.Id] = ruleTask
		}
		for _, ruleTask := range lcScan.RuleStatus.Scanned {
			ScannedMap[ruleTask.Id] = ruleTask
		}
		for _, ruleTask := range lcScan.RuleStatus.Scanning {
			ScanningMap[ruleTask.Id] = ruleTask
		}

		toBeScannedSlice := make([]*proto.RuleTask, 0)
		ScannedSlice := make([]*proto.RuleTask, 0)
		ScanningSlice := make([]*proto.RuleTask, 0)
		NewTasksSlice := make([]*proto.RuleTask, 0)

		for _, r := range tasks {
			if _, ok := toBeScannedMap[r.Id]; ok {
				toBeScannedSlice = append(toBeScannedSlice, r)
			} else if _, ok := ScannedMap[r.Id]; ok {
				ScannedSlice = append(ScannedSlice, r)
			} else if _, ok := ScanningMap[r.Id]; ok {
				ScanningSlice = append(ScanningSlice, r)
			} else {
				NewTasksSlice = append(NewTasksSlice, r)
			}
		}
		tasks = tasks[:0]
		tasks = append(tasks, toBeScannedSlice...)
		tasks = append(tasks, NewTasksSlice...)
		tasks = append(tasks, ScannedSlice...)
		tasks = append(tasks, ScanningSlice...)
		lcScan.RUnlock()
		log.LogWarnf("rescheduleScanRoutine: tasks are rearranged")
	}

	newLcScan := newScanRoutine(lcMgr)
	newLcScan.Start(tasks)

	lcMgr.setNewScanRoutine(newLcScan)
}

func (lcMgr *lifecycleManager) SetS3BucketLifecycle(lcConf *proto.LcConfiguration) error {
	lcMgr.s3ConfMutex.Lock()
	defer lcMgr.s3ConfMutex.Unlock()

	lcMgr.s3LcConfs[lcConf.VolName] = lcConf

	return nil
}

func (lcMgr *lifecycleManager) GetS3BucketLifecycle(VolName string) (lcConf *proto.LcConfiguration) {
	lcMgr.s3ConfMutex.RLock()
	defer lcMgr.s3ConfMutex.RUnlock()

	var ok bool
	lcConf, ok = lcMgr.s3LcConfs[VolName]
	if !ok {
		return nil
	}

	return lcConf
}

func (lcMgr *lifecycleManager) DelS3BucketLifecycle(VolName string) {
	lcMgr.s3ConfMutex.Lock()
	defer lcMgr.s3ConfMutex.Unlock()

	delete(lcMgr.s3LcConfs, VolName)
}
