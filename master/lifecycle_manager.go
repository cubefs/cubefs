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
	startTime        *time.Time // start or stop scan must wait 12s when master leader change
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

func (lcMgr *lifecycleManager) startLcScan(vol, rid string) (success bool, msg string) {
	now := time.Now()
	if lcMgr.startTime != nil && now.Before(lcMgr.startTime.Add(time.Second*12)) {
		success = false
		msg = fmt.Sprintf("startLcScan failed: master restart or leader change just now, wait %v", lcMgr.startTime.Add(time.Second*12).Sub(now))
		log.LogInfo(msg)
		return
	}

	log.LogInfof("startLcScan received, vol: %v, ruleid: %v", vol, rid)
	if vol == "" && rid != "" {
		success = false
		msg = "startLcScan failed: ruleid must be used with vol"
		log.LogInfo(msg)
		return
	}

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

	var tid string
	if vol != "" {
		if rid != "" {
			tid = fmt.Sprintf("%s:%s", vol, rid)
		}
		lcMgr.lcRuleTaskStatus.Lock()
		for id, result := range lcMgr.lcRuleTaskStatus.Results {
			if !result.Done {
				doing = append(doing, result)
			} else {
				if result.Volume == vol && (tid == "" || tid == id) {
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
	var taskSkip []string
	var taskTodo []*proto.RuleTask
	for _, task := range tasks {
		if tid != "" && task.Id != tid {
			continue
		}
		if lcMgr.cluster.volDelete(task.VolName) {
			log.LogWarnf("startLcScan: vol already deleted, not start: %v", task.Id)
			continue
		}
		if !exist(task, doing, todo) {
			taskTodo = append(taskTodo, task)
		} else {
			taskSkip = append(taskSkip, task.Id)
		}
	}
	log.LogInfof("startLcScan: all tasks: %v, todo tasks: %v", len(tasks), len(taskTodo))

	if len(taskTodo) <= 0 {
		success = true
		msg = fmt.Sprintf("startLcScan success: no lifecycle task to start, task(%v) now todo or doing", taskSkip)
		log.LogInfo(msg)
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

func (lcMgr *lifecycleManager) stopLcScan(vol, rid string) (success bool, msg string) {
	now := time.Now()
	if lcMgr.startTime != nil && now.Before(lcMgr.startTime.Add(time.Second*12)) {
		success = false
		msg = fmt.Sprintf("stopLcScan failed: master restart or leader change just now, wait %v", lcMgr.startTime.Add(time.Second*12).Sub(now))
		log.LogInfo(msg)
		return
	}

	log.LogInfof("stopLcScan received, vol: %v, ruleid: %v", vol, rid)
	if vol == "" {
		success = false
		msg = "stopLcScan failed: invalid vol name"
		log.LogInfo(msg)
		return
	}
	var tid string
	if rid != "" {
		tid = fmt.Sprintf("%s:%s", vol, rid)
	}

	var doing []*proto.LcNodeRuleTaskResponse
	var stopTodo []string
	var stopDoing []string
	var hasTasks bool

	lcMgr.lcRuleTaskStatus.Lock()
	for id, result := range lcMgr.lcRuleTaskStatus.Results {
		if !result.Done && vol == result.Volume && (tid == "" || tid == id) {
			doing = append(doing, result)
			hasTasks = true
		}
	}
	for id, task := range lcMgr.lcRuleTaskStatus.ToBeScanned {
		if vol == task.VolName && (tid == "" || tid == id) {
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
			log.LogInfof("stopLcScan: task todo already delete: %v", id)
		}
	}
	lcMgr.lcRuleTaskStatus.Unlock()

	if !hasTasks {
		success = true
		msg = fmt.Sprintf("stopLcScan success: no tasks to stop in vol(%v), ruleid(%v)", vol, rid)
		log.LogInfo(msg)
		return
	}

	client := &http.Client{
		Timeout: time.Second * 5,
	}
	rets := make(chan Rets, len(doing))
	for _, d := range doing {
		if time.Now().After(d.UpdateTime.Add(time.Second * 18)) {
			lcMgr.lcRuleTaskStatus.Lock()
			delete(lcMgr.lcRuleTaskStatus.Results, d.ID)
			lcMgr.lcRuleTaskStatus.Unlock()
			log.LogInfof("stopLcScan: task(%v) doing in lcnode(%v) miss heartbeat over 18s, already delete", d.ID, d.LcNode)
			rets <- Rets{Id: d.ID, Err: nil}
			continue
		}
		go func(cli *http.Client, d *proto.LcNodeRuleTaskResponse) {
			err := doRequestStopLcScan(cli, d)
			rets <- Rets{Id: d.ID, Err: err}
		}(client, d)
	}

	var StopErrs []error
	for i := 0; i < len(doing); i++ {
		ret := <-rets
		if ret.Err != nil {
			StopErrs = append(StopErrs, ret.Err)
		} else {
			stopDoing = append(stopDoing, ret.Id)
		}
	}

	if len(StopErrs) > 0 {
		success = false
		msg = fmt.Sprintf("stopLcScan failed: %v, already success stopTodo: %v, already success stopDoing: %v, please retry", StopErrs, stopTodo, stopDoing)
		log.LogWarn(msg)
		return
	}

	success = true
	msg = fmt.Sprintf("stopLcScan success: task %v (todo) already delete, task %v (doing) already notify lcnode to stop, please check results later", stopTodo, stopDoing)
	log.LogInfo(msg)
	return
}

type Rets struct {
	Id  string
	Err error
}

func doRequestStopLcScan(cli *http.Client, d *proto.LcNodeRuleTaskResponse) error {
	for i := 0; i <= 3; i++ {
		resp, err := cli.Get(getLcStopUrl(d.LcNode, d.ID))
		if err != nil {
			err = fmt.Errorf("stopLcScan failed: task(%v) doing in lcnode(%v) stop err: %v", d.ID, d.LcNode, err)
			log.LogWarn(err)
			return err
		}
		if resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			log.LogInfof("stopLcScan: task(%v) doing in lcnode(%v) already notify to stop", d.ID, d.LcNode)
			return nil
		} else if resp.StatusCode == http.StatusNotFound {
			// maybe lcnode has not received the task, or lcnode restart
			_ = resp.Body.Close()
			log.LogInfof("stopLcScan: task(%v) doing in lcnode(%v) not exist, retry: %v", d.ID, d.LcNode, i)
			if i == 3 {
				break
			}
			time.Sleep(time.Second)
			continue
		} else {
			b, e := io.ReadAll(resp.Body)
			if e != nil {
				log.LogWarnf("stopLcScan: task(%v) doing in lcnode(%v) read resp.Body err: %v", d.ID, d.LcNode, e)
			}
			_ = resp.Body.Close()
			err = fmt.Errorf("stopLcScan: task(%v) doing in lcnode(%v) stop err: %v", d.ID, d.LcNode, string(b))
			log.LogWarn(err)
			return err
		}
	}
	err := fmt.Errorf("stopLcScan: task(%v) doing in lcnode(%v) not exist, please retry", d.ID, d.LcNode)
	log.LogWarn(err)
	return err
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
			var volDeleted []string
			lcMgr.lcRuleTaskStatus.Lock()
			for k, v := range lcMgr.lcRuleTaskStatus.ToBeScanned {
				if lcMgr.cluster.volDelete(v.VolName) {
					log.LogWarnf("checkLcRuleTaskResults vol already deleted, stop todo task later: %v", k)
					volDeleted = append(volDeleted, v.VolName)
					continue
				}
			}
			for k, v := range lcMgr.lcRuleTaskStatus.Results {
				if v.Done == false && v.RcvStop == false {
					if lcMgr.cluster.volDelete(v.Volume) {
						log.LogWarnf("checkLcRuleTaskResults vol already deleted, stop doing task later: %v", k)
						volDeleted = append(volDeleted, v.Volume)
						continue
					}
				}
				if v.Done == false && time.Now().After(v.UpdateTime.Add(time.Minute*20)) {
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
			for _, vol := range uniqueStrings(volDeleted) {
				_, msg := lcMgr.stopLcScan(vol, "")
				log.LogWarnf("checkLcRuleTaskResults %v", msg)
			}
			log.LogInfo("checkLcRuleTaskResults finish")
			timer.Reset(time.Minute)
		}
	}
}

func uniqueStrings(input []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, s := range input {
		if _, exists := seen[s]; !exists {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}
	return result
}

func (lcMgr *lifecycleManager) startLcScanHandleLeaderChange() {
	now := time.Now()
	lcMgr.startTime = &now
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
		log.LogInfof("wait idleLcNodeCh... ToBeScanned num(%v)", len(lcMgr.lcRuleTaskStatus.ToBeScanned))
		select {
		case <-lcMgr.exitCh:
			log.LogInfo("exitCh notified, lifecycleManager process exit")
			return
		case idleNode := <-lcMgr.idleLcNodeCh:
			log.LogInfof("process idleLcNodeCh notified: %v", idleNode)

			// ToBeScanned -> Scanning
			task := lcMgr.lcRuleTaskStatus.GetOneTask()
			if task == nil {
				log.LogInfof("process(%v), lcRuleTaskStatus.GetOneTask, no task", idleNode)
				continue
			}

			nodeAddr := lcMgr.lcNodeStatus.GetIdleNode(idleNode)
			if nodeAddr == "" {
				log.LogWarnf("process(%v), no idle lcnode, redo task", idleNode)
				lcMgr.lcRuleTaskStatus.RedoTask(task)
				continue
			}

			val, ok := lcMgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("process(%v), lcNodes.Load, nodeAddr is not available, redo task", nodeAddr)
				lcMgr.lcNodeStatus.RemoveNode(nodeAddr)
				lcMgr.lcRuleTaskStatus.RedoTask(task)
				continue
			}

			if err := lcMgr.cluster.syncDeleteLcTask(task); err != nil {
				log.LogErrorf("process(%v), syncDeleteLcTask: %v err: %v, redo task, ensure syncDeleteLcTask success", nodeAddr, task.Id, err)
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
