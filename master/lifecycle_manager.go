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
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
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

func exist(task *proto.RuleTask, doing []*proto.LcNodeRuleTaskResponse, todo []*proto.RuleTask) bool {
	for _, d := range doing {
		if task.Id == d.ID {
			log.LogInfof("startLcScan: exist doing task: %v, skip this task: %v", d, task)
			return true
		}
		if task.VolName == d.Volume && task.Rule.GetPrefix() == d.Rule.GetPrefix() {
			log.LogInfof("startLcScan: exist doing task: %v, skip this task: %v", d, task)
			return true
		}
		if task.VolName == d.Volume && strings.HasPrefix(task.Rule.GetPrefix(), d.Rule.GetPrefix()) {
			log.LogInfof("startLcScan: exist doing task: %v, skip this task: %v", d, task)
			return true
		}
		if task.VolName == d.Volume && strings.HasPrefix(d.Rule.GetPrefix(), task.Rule.GetPrefix()) {
			log.LogInfof("startLcScan: exist doing task: %v, skip this task: %v", d, task)
			return true
		}
	}
	for _, t := range todo {
		if task.Id == t.Id {
			log.LogInfof("startLcScan: exist todo task: %v, skip this task: %v", t, task)
			return true
		}
		if task.VolName == t.VolName && task.Rule.GetPrefix() == t.Rule.GetPrefix() {
			log.LogInfof("startLcScan: exist todo task: %v, skip this task: %v", t, task)
			return true
		}
		if task.VolName == t.VolName && strings.HasPrefix(task.Rule.GetPrefix(), t.Rule.GetPrefix()) {
			log.LogInfof("startLcScan: exist todo task: %v, skip this task: %v", t, task)
			return true
		}
		if task.VolName == t.VolName && strings.HasPrefix(t.Rule.GetPrefix(), task.Rule.GetPrefix()) {
			log.LogInfof("startLcScan: exist todo task: %v, skip this task: %v", t, task)
			return true
		}
	}
	return false
}

func (lcMgr *lifecycleManager) startLcScan(vol string) (success bool, msg string) {
	start := time.Now()
	lcMgr.lcRuleTaskStatus.StartTime = &start
	lcMgr.lcRuleTaskStatus.EndTime = nil

	var doing []*proto.LcNodeRuleTaskResponse
	var todo []*proto.RuleTask

	if vol == "" {
		lcMgr.lcRuleTaskStatus.Lock()
		for id, result := range lcMgr.lcRuleTaskStatus.Results {
			if !result.Done {
				doing = append(doing, result)
			} else {
				if err := lcMgr.cluster.syncDeleteLcResult(result); err != nil {
					success = false
					msg = fmt.Sprintf("startLcScan failed: syncDeleteLcResult: %v err: %v, need retry", id, err)
					log.LogError(msg)
					lcMgr.lcRuleTaskStatus.Unlock()
					return
				}
				delete(lcMgr.lcRuleTaskStatus.Results, id)
			}
		}
		for id, task := range lcMgr.lcRuleTaskStatus.ToBeScanned {
			if err := lcMgr.cluster.syncDeleteLcTask(task); err != nil {
				success = false
				msg = fmt.Sprintf("startLcScan failed: syncDeleteLcTask: %v err: %v, need retry", id, err)
				log.LogError(msg)
				lcMgr.lcRuleTaskStatus.Unlock()
				return
			}
			delete(lcMgr.lcRuleTaskStatus.ToBeScanned, id)
		}
		lcMgr.lcRuleTaskStatus.Unlock()
	}

	if vol != "" {
		lcMgr.lcRuleTaskStatus.Lock()
		for id, result := range lcMgr.lcRuleTaskStatus.Results {
			if !result.Done {
				doing = append(doing, result)
			} else {
				if result.Volume == vol {
					if err := lcMgr.cluster.syncDeleteLcResult(result); err != nil {
						success = false
						msg = fmt.Sprintf("startLcScan failed: syncDeleteLcResult: %v err: %v, need retry", id, err)
						log.LogError(msg)
						lcMgr.lcRuleTaskStatus.Unlock()
						return
					}
					delete(lcMgr.lcRuleTaskStatus.Results, id)
				}
			}
		}
		for _, task := range lcMgr.lcRuleTaskStatus.ToBeScanned {
			todo = append(todo, task)
		}
		lcMgr.lcRuleTaskStatus.Unlock()
	}

	tasks := lcMgr.genEnabledRuleTasks(vol)

	// decide which task should be started
	var taskTodo []*proto.RuleTask
	for _, task := range tasks {
		if !exist(task, doing, todo) {
			taskTodo = append(taskTodo, task)
		}
	}
	log.LogInfof("startLcScan: all tasks: %v, todo tasks: %v", len(tasks), len(taskTodo))

	if len(taskTodo) <= 0 {
		success = true
		msg = "startLcScan success: no lifecycle task to do"
		log.LogInfo(msg)
		end := time.Now()
		lcMgr.lcRuleTaskStatus.EndTime = &end
		return
	}

	// start scan init
	for _, task := range taskTodo {
		lcMgr.lcRuleTaskStatus.RedoTask(task)
		if err := lcMgr.cluster.syncAddLcTask(task); err != nil {
			log.LogWarnf("startLcScan syncAddLcTask: %v err: %v", task.Id, err)
		}
	}

	success = true
	msg = fmt.Sprintf("startLcScan success: add %v tasks", len(taskTodo))
	log.LogInfo(msg)
	return
}

// generate all tasks or vol tasks
func (lcMgr *lifecycleManager) genEnabledRuleTasks(vol string) []*proto.RuleTask {
	lcMgr.RLock()
	defer lcMgr.RUnlock()
	tasks := make([]*proto.RuleTask, 0)
	for _, v := range lcMgr.lcConfigurations {
		if vol != "" && v.VolName != vol {
			continue
		}
		ts := v.GenEnabledRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

// generate task by vol and rule id
func (lcMgr *lifecycleManager) genRuleTask(vol, taskId string) *proto.RuleTask {
	lcMgr.RLock()
	defer lcMgr.RUnlock()
	conf := lcMgr.lcConfigurations[vol]
	if conf != nil {
		tasks := conf.GenEnabledRuleTasks()
		for _, task := range tasks {
			if task.Id == taskId {
				return task
			}
		}
	}
	return nil
}

func (lcMgr *lifecycleManager) stopLcScan(vol string) (success bool, msg string) {
	if vol == "" {
		success = false
		msg = "stopLcScan failed: invalid vol name"
		log.LogInfo(msg)
		return
	}

	var doing []*proto.LcNodeRuleTaskResponse
	var stopTodo []string
	var stopDoing []string
	var hasTasks bool

	lcMgr.lcRuleTaskStatus.Lock()
	for id, result := range lcMgr.lcRuleTaskStatus.Results {
		if !result.Done && vol == result.Volume {
			doing = append(doing, result)
			hasTasks = true
			delete(lcMgr.lcRuleTaskStatus.Results, id)
		}
	}
	for id, task := range lcMgr.lcRuleTaskStatus.ToBeScanned {
		if vol == task.VolName {
			if err := lcMgr.cluster.syncDeleteLcTask(task); err != nil {
				success = false
				msg = fmt.Sprintf("stopLcScan failed: syncDeleteLcTask: %v err: %v, need retry", id, err)
				log.LogError(msg)
				lcMgr.lcRuleTaskStatus.Unlock()
				return
			}
			delete(lcMgr.lcRuleTaskStatus.ToBeScanned, id)
			stopTodo = append(stopTodo, id)
			hasTasks = true
			log.LogInfof("stopLcScan success: task todo already delete: %v", id)
		}
	}
	lcMgr.lcRuleTaskStatus.Unlock()

	if !hasTasks {
		success = true
		msg = fmt.Sprintf("stopLcScan success: no tasks to stop in vol: %v", vol)
		log.LogInfo(msg)
		return
	}

	client := &http.Client{
		Timeout: time.Second * 5,
	}
	for _, d := range doing {
		resp, err := client.Get(getLcStopUrl(d.LcNode, d.ID))
		if err != nil {
			success = false
			msg = fmt.Sprintf("stopLcScan failed: id: %v err: %v, already success stopTodo: %v, already success stopDoing: %v, need retry", d.ID, err, stopTodo, stopDoing)
			log.LogError(msg)
			return
		}
		if resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			stopDoing = append(stopDoing, d.ID)
			log.LogInfof("stopLcScan success: task doing already notify lcnode to stop: %v", d.ID)
		} else {
			b, e := io.ReadAll(resp.Body)
			if e != nil {
				log.LogErrorf("stopLcScan failed: read resp.Body err: %v", e)
			}
			_ = resp.Body.Close()
			success = false
			msg = fmt.Sprintf("stopLcScan failed: id: %v err: %v, already success stopTodo: %v, already success stopDoing: %v, need retry", d.ID, string(b), stopTodo, stopDoing)
			log.LogError(msg)
			return
		}
	}

	success = true
	msg = fmt.Sprintf("stopLcScan success: task %v (todo) already delete, task %v (doing) already notify lcnode to stop, please check later", stopTodo, stopDoing)
	log.LogInfo(msg)
	return
}

func getLcStopUrl(node, id string) (url string) {
	s := strings.Split(node, ":")
	if len(s) != 2 {
		log.LogErrorf("getLcStopUrl id: %v invalid LcNode addr: %v", id, node)
		return
	}
	ip := s[0]
	port := s[1]
	portInt, err := strconv.Atoi(port)
	if err != nil {
		log.LogErrorf("getLcStopUrl id: %v port: %v err: %v", id, port, err)
		return
	}
	url = fmt.Sprintf("http://%v:%v/stopScanner?id=%v", ip, portInt+1, id)
	log.LogInfof("getLcStopUrl: %v", url)
	return
}

func (lcMgr *lifecycleManager) checkLcRuleTaskResults() {
	log.LogInfo("lifecycleManager checkLcRuleTaskResults start")
	if lcMgr.lcRuleTaskStatus.StartTime == nil {
		start := time.Now()
		lcMgr.lcRuleTaskStatus.StartTime = &start
	}
	timer := time.NewTimer(time.Minute)
	for {
		select {
		case <-lcMgr.exitCh:
			log.LogInfo("exitCh notified, lifecycleManager checkLcRuleTaskResults exit")
			return
		case <-timer.C:
			log.LogInfo("checkLcRuleTaskResults start")
			if lcMgr.lcRuleTaskStatus.CheckResultsDone() {
				if lcMgr.lcRuleTaskStatus.EndTime == nil {
					end := time.Now()
					lcMgr.lcRuleTaskStatus.EndTime = &end
				}
				log.LogInfo("checkLcRuleTaskResults all done")
				timer.Reset(time.Minute * 10)
				continue
			}
			lcMgr.lcRuleTaskStatus.Lock()
			for k, v := range lcMgr.lcRuleTaskStatus.Results {
				if v.Done != true && time.Now().After(v.UpdateTime.Add(time.Minute*20)) {
					task := lcMgr.genRuleTask(v.Volume, k)
					if task != nil {
						delete(lcMgr.lcRuleTaskStatus.Results, k)
						lcMgr.lcRuleTaskStatus.ToBeScanned[task.Id] = task
						log.LogInfof("checkLcRuleTaskResults delete result and redo this task: %v", k)
					} else {
						delete(lcMgr.lcRuleTaskStatus.Results, k)
						log.LogWarnf("checkLcRuleTaskResults delete result and no this task: %v", k)
					}
				}
			}
			lcMgr.lcRuleTaskStatus.Unlock()
			log.LogInfo("checkLcRuleTaskResults finish")
			timer.Reset(time.Minute)
		}
	}
}

func (lcMgr *lifecycleManager) startLcScanHandleLeaderChange() {
	go func() {
		log.LogInfof("startLcScanHandleLeaderChange start, wait 10 min, lcRuleTaskStatus ToBeScanned len: %v", len(lcMgr.lcRuleTaskStatus.ToBeScanned))
		time.Sleep(time.Minute * 10)
		go lcMgr.process()
		go lcMgr.checkLcRuleTaskResults()
	}()
}

func (lcMgr *lifecycleManager) process() {
	log.LogInfo("lifecycleManager process start")
	for {
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
				log.LogDebug("lcRuleTaskStatus.GetOneTask, no task")
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

			if err := lcMgr.cluster.syncDeleteLcTask(task); err != nil {
				log.LogErrorf("syncDeleteLcTask: %v err: %v, redo task, ensure syncDeleteLcTask success", task.Id, err)
				lcMgr.lcNodeStatus.RemoveNode(nodeAddr)
				lcMgr.lcRuleTaskStatus.RedoTask(task)
				continue
			}

			node := val.(*LcNode)
			adminTask := node.createLcScanTask(lcMgr.cluster.masterAddr(), task)
			lcMgr.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			t := time.Now()
			lcMgr.lcRuleTaskStatus.AddResult(&proto.LcNodeRuleTaskResponse{ID: task.Id, LcNode: nodeAddr, UpdateTime: &t, Volume: task.VolName, Rule: task.Rule})
			log.LogInfof("add lifecycle scan task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
}

func (lcMgr *lifecycleManager) notifyIdleLcNode(nodeAddr string) {
	if len(lcMgr.lcRuleTaskStatus.ToBeScanned) > 0 {
		select {
		case lcMgr.idleLcNodeCh <- nodeAddr:
			log.LogDebugf("action[handleLcNodeHeartbeatResp], notifyIdleLcNode success: %v", nodeAddr)
		default:
			log.LogDebug("action[handleLcNodeHeartbeatResp], notifyIdleLcNode skip")
		}
	} else {
		log.LogDebug("action[handleLcNodeHeartbeatResp], notifyIdleLcNode skip no ToBeScanned")
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

func (rs *lcRuleTaskStatus) CheckResultsDone() bool {
	if len(rs.ToBeScanned) > 0 {
		return false
	}
	rs.RLock()
	defer rs.RUnlock()
	for _, v := range rs.Results {
		if v.Done == false {
			return false
		}
	}
	return true
}
