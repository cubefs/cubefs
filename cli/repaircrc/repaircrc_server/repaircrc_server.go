package repaircrc_server

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	atomic2 "go.uber.org/atomic"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RepairServer struct {
	repairTaskMap  map[int64]*RepairCrcTask
	taskReceiverCh chan *RepairCrcTask
	taskDir        string
	maxTaskId      atomic2.Int64
	lock           sync.RWMutex
	state          uint32
	stopC          chan bool
	wg             sync.WaitGroup
}

type TaskMeta struct {
	TaskList   []*RepairCrcTask  `json:"task_list"`
}

func NewServer() *RepairServer {
	return &RepairServer{}
}
const (
	StateStandby uint32 = iota
	StateStart
	StateRunning
	StateShutdown
	StateStopped
)

const (
	repairTaskFile  = "task.json"
	maxTaskIDFile  = "MAX_TASK_ID"
)
// Shutdown shuts down the current data node.
func (s *RepairServer) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, StateRunning, StateShutdown) {
		close(s.stopC)
		close(s.taskReceiverCh)
		s.wg.Done()
		atomic.StoreUint32(&s.state, StateStopped)
	}
	return
}

// Sync keeps data node in sync.
func (s *RepairServer) Sync() {
	if atomic.LoadUint32(&s.state) == StateRunning {
		s.wg.Wait()
	}
}
const (
	checkTypeExtentReplica = 0
	checkTypeExtentLength  = 1
	checkTypeExtentCrc     = 2
	checkTypeInodeEk       = 3
	checkTypeInodeNlink    = 4
)
// Workflow of starting up a data node.
func (s *RepairServer) DoStart(cfg *config.Config) (err error) {
	if atomic.CompareAndSwapUint32(&s.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&s.state, newState)
		}()
		s.repairTaskMap = make(map[int64]*RepairCrcTask, 0)
		s.stopC = make(chan bool, 0)
		s.taskReceiverCh = make(chan *RepairCrcTask, 1024)
		if err = s.parseConfig(cfg); err != nil {
			return
		}
		go s.initRepairReceiver()
		go s.registerHandler()
		go s.scheduleToRepairCrc()
		s.wg.Add(1)
	}
	return
}

func (s *RepairServer) initRepairReceiver() {
	for _, t := range s.repairTaskMap {
		s.taskReceiverCh <- t
	}
}

func (s *RepairServer) parseConfig(cfg *config.Config) (err error) {
	var idFile *os.File
	var maxTaskID uint64
	var taskFile *os.File
	s.taskDir = cfg.GetString("taskDir")

	//read max task id
	taskIDFile := fmt.Sprintf("%v/%v", s.taskDir, maxTaskIDFile)
	idFile, err = os.Open(taskIDFile)
	if err != nil && os.IsNotExist(err) {
		idFile, err = os.Create(taskIDFile)
	}
	if err != nil {
		return
	}
	defer idFile.Close()

	idData := make([]byte, 0)
	idData, err = ioutil.ReadFile(taskIDFile)
	if len(idData) == 0 {
		maxTaskID = 0
	} else {
		maxTaskID, err = strconv.ParseUint(string(idData), 10, 64)
		if err != nil {
			return
		}
	}
	s.maxTaskId.Store(int64(maxTaskID))

	//load tasks
	taskJsonFile := fmt.Sprintf("%v/%v", s.taskDir, repairTaskFile)
	taskFile, err = os.Open(taskJsonFile)
	if err != nil && os.IsNotExist(err) {
		taskFile, err = os.Create(taskJsonFile)
	}
	if err != nil {
		return
	}
	defer taskFile.Close()
	data := make([]byte, 0)
	data, err = ioutil.ReadFile(taskJsonFile)
	if err != nil {
		return
	}
	if len(data) == 0 {
		return
	}
	taskMeta := new(TaskMeta)
	err = json.Unmarshal(data, taskMeta)
	if err != nil {
		return
	}
	for _, t := range taskMeta.TaskList {
		bytes, _ := json.Marshal(t)
		task := NewRepairTask()
		json.Unmarshal(bytes, task)
		if err = task.validTask(); err != nil {
			log.LogError("illegal task: %v, err: %v", task.TaskId, err)
			continue
		}
		s.repairTaskMap[task.TaskId] = task
		if task.TaskId > s.maxTaskId.Load() {
			s.maxTaskId.Store(task.TaskId)
		}
	}
	s.PersistTaskID()
	return
}

func (s *RepairServer) registerHandler() (err error) {
	http.HandleFunc("/addTask", s.handleAddTask)
	http.HandleFunc("/delTask", s.handleDelTask)
	http.HandleFunc("/taskList", s.handleTaskList)
	return
}

func getRequestBody(r *http.Request) (body []byte, err error) {
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return nil, fmt.Errorf(" Read Body Error:%v ", err.Error())
	}
	return
}

func (s *RepairServer) handleTaskList(w http.ResponseWriter, r *http.Request) {
	buildSuccessResp(w, s.repairTaskMap)
	return
}

func (s *RepairServer) handleAddTask(w http.ResponseWriter, r *http.Request) {
	body, err := getRequestBody(r)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	task := NewRepairTask()
	if err = json.Unmarshal(body, &task); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	err = task.validTask()
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.lock.RLock()
	for _, t := range s.repairTaskMap {
		if t.ClusterInfo.Master == task.ClusterInfo.Master && t.RepairType == task.RepairType {
			switch t.RepairType {
			case RepairVolume:
				if t.Filter.VolFilter == task.Filter.VolFilter {
					buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("already has duplicate task: %v", t.TaskId))
					return
				}
			case RepairDataNode:
				if t.NodeAddress == task.NodeAddress {
					buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("already has duplicate task: %v", t.TaskId))
					return
				}
			}
		}
	}
	s.lock.RUnlock()
	s.addTask(task)
	s.PersistMetadata()
	s.PersistTaskID()
	s.taskReceiverCh <- task
	buildSuccessResp(w, fmt.Sprintf("add task %v success", task.TaskId))
}

func (s *RepairServer) handleDelTask(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		taskID    uint64
	)
	taskStr := r.FormValue("task")
	if taskID, err = strconv.ParseUint(taskStr, 10, 64); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.stopTask(int64(taskID))
	s.PersistMetadata()
	buildSuccessResp(w, fmt.Sprintf("delete task %v success", taskID))
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func (s *RepairServer) scheduleToRepairCrc() {
	for {
		select {
		case task := <- s.taskReceiverCh:
			log.LogInfof("scheduleToRepairCrc, task:%v", task.TaskId)
			go s.executeTask(task)
		case <- s.stopC:
			return
		}
	}
}

type RepairTaskFunc func() (err error)

func (s *RepairServer) executeTask(t *RepairCrcTask) {
	var err error
	defer func() {
		if err != nil {
			log.LogError("executeTask: failed, err:%v", err)
		}
		log.LogInfof("executeTask stop, taskID:%v", t.TaskId)
	}()
	t.mc = master.NewMasterClientWithoutTimeout([]string{t.ClusterInfo.Master}, false)
	t.mc.DataNodeProfPort = t.ClusterInfo.DnProf
	t.mc.MetaNodeProfPort = t.ClusterInfo.MnProf
	if t.Frequency.Interval < 1 {
		t.Frequency.Interval = defaultIntervalHour
	}
	timer := time.NewTimer(time.Duration(t.Frequency.Interval) * time.Hour)
	defer timer.Stop()
	timer.Reset(time.Second)
	var count uint32
	for {
		select {
		case <- timer.C:
			log.LogInfof("executeTask begin, taskID:%v", t.TaskId)
			switch t.RepairType {
			case RepairVolume:
				t.executeVolumeTask()
			case RepairDataNode:
				t.executeDataNodeTask()
			}
			count++
			if t.Frequency.ExecuteCount > 0 && count >= t.Frequency.ExecuteCount {
				return
			}
			timer.Reset(time.Duration(t.Frequency.Interval) * time.Hour)
		case <- t.stopC:
			return
		}
	}

}

func (s *RepairServer) addTask(task *RepairCrcTask) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.maxTaskId.Add(1)
	task.TaskId = s.maxTaskId.Load()
	s.repairTaskMap[task.TaskId] = task
}
func (s *RepairServer) stopTask(id int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	task := s.repairTaskMap[id]
	close(task.stopC)
	delete(s.repairTaskMap, id)
}

func (s *RepairServer) PersistMetadata() (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var metadata = new(TaskMeta)
	tmpFile := s.taskDir + "/." + repairTaskFile
	taskFile := s.taskDir + "/" + repairTaskFile
	metadata.TaskList = make([]*RepairCrcTask, 0)
	for _, t := range s.repairTaskMap {
		metadata.TaskList = append(metadata.TaskList, t)
	}
	var newData []byte
	if newData, err = json.Marshal(metadata); err != nil {
		return
	}
	var tempFile *os.File
	if tempFile, err = os.OpenFile(tmpFile, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		if err != nil {
			_ = os.Remove(tmpFile)
		}
	}()
	if _, err = tempFile.Write(newData); err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	if err = os.Rename(tmpFile, taskFile); err != nil {
		return
	}
	log.LogInfof("PersistMetadata data(%v)", string(newData))
	return
}

func (s *RepairServer) PersistTaskID() (err error){
	s.lock.Lock()
	defer s.lock.Unlock()
	tmpFile := s.taskDir + "/." + maxTaskIDFile
	taskFile := s.taskDir + "/" + maxTaskIDFile
	var tempFile *os.File
	if tempFile, err = os.OpenFile(tmpFile, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		if err != nil {
			_ = os.Remove(tmpFile)
		}
	}()
	if _, err = tempFile.Write([]byte(strconv.FormatUint(uint64(s.maxTaskId.Load()), 10))); err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	if err = os.Rename(tmpFile, taskFile); err != nil {
		return
	}
	log.LogInfof("PersistMetadata max task id(%v)", strconv.FormatUint(uint64(s.maxTaskId.Load()), 10))
	return
}