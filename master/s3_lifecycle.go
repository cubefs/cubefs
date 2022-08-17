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
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

//type scanTaskInfo struct {
//	Id        string
//	routineId int64
//}

type nodesState struct {
	sync.RWMutex
	workingNodes map[string]map[string]*proto.ScanTaskInfo
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

func (ns *nodesState) addTaskToNode(info *proto.ScanTaskInfo, nodeAddr string) {
	if taskMap, ok := ns.workingNodes[nodeAddr]; !ok {
		taskMap = make(map[string]*proto.ScanTaskInfo, 0)
		taskMap[info.Id] = info
		ns.workingNodes[nodeAddr] = taskMap
	} else {
		if _, ok := taskMap[info.Id]; !ok {
			taskMap[info.Id] = info
		} else {
			log.LogWarnf("task(%v) in routine(%v) is already running in nodeAddr(%v)", info.Id, info.RoutineId, nodeAddr)
		}
	}
}

func (ns *nodesState) AddTaskToNode(info *proto.ScanTaskInfo) (nodeAddr string, err error) {
	ns.Lock()
	defer ns.Unlock()

	nodeAddr, err = ns.getOneIdleNode()
	if err != nil {
		return nodeAddr, err
	}
	delete(ns.idleNodes, nodeAddr)

	ns.addTaskToNode(info, nodeAddr)
	return
}

func (ns *nodesState) UpdateNodeTask(nodeAddr string, infos []*proto.ScanTaskInfo) {
	if len(infos) == 0 {
		ns.Lock()
		delete(ns.workingNodes, nodeAddr)
		ns.idleNodes[nodeAddr] = nodeAddr
		ns.Unlock()
		return
	}
	ns.Lock()
	defer ns.Unlock()

	if taskMap, ok := ns.workingNodes[nodeAddr]; !ok {
		for _, info := range infos {
			ns.addTaskToNode(info, nodeAddr)
		}
		delete(ns.idleNodes, nodeAddr)
	} else {
		if len(infos) != len(taskMap) {
			delete(ns.workingNodes, nodeAddr)
			for _, info := range infos {
				ns.addTaskToNode(info, nodeAddr)
			}
		} else {
			equal := true
			for _, info := range infos {
				if val, ok := taskMap[info.Id]; !ok {
					equal = false
					break
				} else {
					if val.RoutineId != info.RoutineId {
						equal = false
					}
				}
			}
			if !equal {
				delete(ns.workingNodes, nodeAddr)
				for _, info := range infos {
					ns.addTaskToNode(info, nodeAddr)
				}
			}
		}
	}

}

func (ns *nodesState) ReleaseTask(taskId string, RoutineID int64) {
	ns.Lock()
	defer ns.Unlock()
	for nodeAddr, taskMap := range ns.workingNodes {
		if info, ok := taskMap[taskId]; ok {
			if RoutineID == info.RoutineId {
				delete(taskMap, taskId)
				log.LogInfof("task(%v) in routine(%v) is released", taskId, RoutineID)
				if len(taskMap) == 0 {
					delete(ns.workingNodes, nodeAddr)
					ns.idleNodes[nodeAddr] = nodeAddr
					log.LogInfof("lcnode(%v) is set idle", nodeAddr)
				}
			}

		}
	}
}

func (ns *nodesState) RemoveNode(nodeAddr string) (taskInfos []*proto.ScanTaskInfo) {
	ns.Lock()
	defer ns.Unlock()

	if taskMap, ok := ns.workingNodes[nodeAddr]; ok {
		for _, info := range taskMap {
			taskInfos = append(taskInfos, info)
		}
	}
	delete(ns.idleNodes, nodeAddr)
	delete(ns.workingNodes, nodeAddr)
	return
}

type s3LifecycleManager struct {
	cluster   *Cluster
	lcNodes   sync.Map
	lnMutex   sync.RWMutex
	lcConfs   map[string]*proto.LcConfiguration
	scanMutex sync.RWMutex
	lcScan    *scanRoutine
	//working state
	lnStates  *nodesState
	confMutex sync.RWMutex
}

//type RuleTask struct {
//	Id   string
//	Rule *proto.Rule
//}

type lcRuleStatus struct {
	Scanned     []*proto.RuleTask
	Scanning    []*proto.RuleTask
	ToBeScanned []*proto.RuleTask
	Results     []*proto.RuleTaskResponse
}

type scanRoutine struct {
	sync.RWMutex
	s3LcMgr    *s3LifecycleManager
	Id         int64
	stopped    bool
	StartTime  *time.Time
	EndTime    *time.Time
	RuleStatus lcRuleStatus
	exitCh     chan struct{}
	idleNodeCh chan struct{}
}

func newS3LifecycleManager() *s3LifecycleManager {
	log.LogInfof("action[newS3LifecycleManager] construct")
	s3LcMgr := &s3LifecycleManager{
		lcConfs: make(map[string]*proto.LcConfiguration),
		lnStates: &nodesState{
			workingNodes: make(map[string]map[string]*proto.ScanTaskInfo),
			idleNodes:    make(map[string]string),
		},
	}

	return s3LcMgr
}

func newScanRoutine(s3LcMgr *s3LifecycleManager) *scanRoutine {
	lcScan := &scanRoutine{
		stopped: true,
		s3LcMgr: s3LcMgr,
		RuleStatus: lcRuleStatus{
			Scanned:     make([]*proto.RuleTask, 0),
			Scanning:    make([]*proto.RuleTask, 0),
			ToBeScanned: make([]*proto.RuleTask, 0),
			Results:     make([]*proto.RuleTaskResponse, 0),
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

func (s *scanRoutine) redoTasks(taskInfos []*proto.ScanTaskInfo) {
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

				info := &proto.ScanTaskInfo{
					Id:        task.Id,
					RoutineId: s.Id,
				}
				nodeAddr, err := s.s3LcMgr.lnStates.AddTaskToNode(info)
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

func (s3LcMgr *s3LifecycleManager) genRuleTasks() []*proto.RuleTask {
	s3LcMgr.confMutex.RLock()
	defer s3LcMgr.confMutex.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range s3LcMgr.lcConfs {
		ts := v.GenRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (s3LcMgr *s3LifecycleManager) setNewScanRoutine(scan *scanRoutine) {
	s3LcMgr.scanMutex.RLock()
	oldScan := s3LcMgr.lcScan
	s3LcMgr.scanMutex.RUnlock()
	if oldScan != nil {
		oldScan.Stop()
		log.LogInfof("setNewScanRoutine: old routine(%v) is stopped!", oldScan.Id)
	}

	s3LcMgr.scanMutex.Lock()
	s3LcMgr.lcScan = scan
	s3LcMgr.scanMutex.Unlock()
	log.LogInfof("setNewScanRoutine: new routine(%v) is started!", scan.Id)
}

func (s3LcMgr *s3LifecycleManager) getScanRoutine() *scanRoutine {
	s3LcMgr.scanMutex.RLock()
	defer s3LcMgr.scanMutex.RUnlock()
	return s3LcMgr.lcScan
}

func (s3LcMgr *s3LifecycleManager) genEnabledRuleTasks() []*proto.RuleTask {
	s3LcMgr.confMutex.RLock()
	defer s3LcMgr.confMutex.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range s3LcMgr.lcConfs {
		ts := v.GenEnabledRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (s3LcMgr *s3LifecycleManager) rescheduleScanRoutine() {
	tasks := s3LcMgr.genEnabledRuleTasks()

	if len(tasks) <= 0 {
		log.LogDebugf("rescheduleScanRoutine: no enabled lifecycle rule task to schedule!")
		return
	} else {
		log.LogDebugf("rescheduleScanRoutine: %v lifecycle rule tasks to schedule!", len(tasks))
	}

	lcScan := s3LcMgr.getScanRoutine()

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

	newLcScan := newScanRoutine(s3LcMgr)
	newLcScan.Start(tasks)

	s3LcMgr.setNewScanRoutine(newLcScan)
}

func (s3LcMgr *s3LifecycleManager) SetBucketLifecycle(lcConf *proto.LcConfiguration) error {
	s3LcMgr.confMutex.Lock()
	defer s3LcMgr.confMutex.Unlock()

	s3LcMgr.lcConfs[lcConf.VolName] = lcConf

	return nil
}

func (s3LcMgr *s3LifecycleManager) GetBucketLifecycle(VolName string) (lcConf *proto.LcConfiguration) {
	s3LcMgr.confMutex.RLock()
	defer s3LcMgr.confMutex.RUnlock()

	var ok bool
	lcConf, ok = s3LcMgr.lcConfs[VolName]
	if !ok {
		return nil
	}

	return lcConf
}

func (s3LcMgr *s3LifecycleManager) DelBucketLifecycle(VolName string) {
	s3LcMgr.confMutex.Lock()
	defer s3LcMgr.confMutex.Unlock()

	delete(s3LcMgr.lcConfs, VolName)
}
