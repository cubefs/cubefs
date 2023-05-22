package checkcrc

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cli/cmd/data_check"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultLimitLevel = 2
)

var dataPortMap = map[string]string{
	"6000":  "6001",
	"17030": "17031",
	"17310": "17320",
}
var metaPortMap = map[string]string{
	"9021":  "9092",
	"17020": "17021",
	"17210": "17220",
}

type CrcWorker struct {
	worker.BaseWorker
	masterAddr map[string][]string
	mcw        map[string]*master.MasterClient
	svv        map[string]*proto.SmartVolumeView
	cv         map[string]*proto.ClusterView // TODO
	lock       sync.RWMutex
	state      uint32
	stopC      chan bool
	//todo stop a specified task
	wg sync.WaitGroup
}

func NewCrcWorker() *CrcWorker {
	return &CrcWorker{}
}

func NewCrcWorkerForScheduler() (cw *CrcWorker, err error) {
	return &CrcWorker{}, nil
}

const (
	StateStandby uint32 = iota
	StateStart
	StateRunning
	StateShutdown
	StateStopped
)

const (
	DataNodeProf = "dnProf"
	MetaNodeProf = "mnProf"
)

// Shutdown shuts down the current data node.
func (s *CrcWorker) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, StateRunning, StateShutdown) {
		close(s.stopC)
		s.wg.Done()
		atomic.StoreUint32(&s.state, StateStopped)
	}
	return
}

// Sync keeps data node in sync.
func (s *CrcWorker) Sync() {
	if atomic.LoadUint32(&s.state) == StateRunning {
		s.wg.Wait()
	}
}
func (s *CrcWorker) Start(cfg *config.Config) (err error) {
	return s.Control.Start(s, cfg, doStart)
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*CrcWorker)
	if !ok {
		return errors.New("invalid node type")
	}
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
		s.stopC = make(chan bool, 0)
		if err = s.parseConfig(cfg); err != nil {
			return
		}
		go s.registerHandler()
		s.wg.Add(1)
	}
	return
}
func (s *CrcWorker) initWorker() (err error) {
	s.WorkerType = proto.WorkerTypeSmartVolume
	s.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	// init master client
	masterClient := make(map[string]*master.MasterClient)
	for cluster, addresses := range s.masterAddr {
		mc := master.NewMasterClient(addresses, false)
		masterClient[cluster] = mc
	}
	s.mcw = masterClient
	s.svv = make(map[string]*proto.SmartVolumeView)
	s.cv = make(map[string]*proto.ClusterView)
	return
}
func (s *CrcWorker) GetCreatorDuration() int {
	return 3600
}

// CreateTask for scheduler node to produce single task
func (s *CrcWorker) CreateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	newTasks = make([]*proto.Task, 0)
	var taskAddFunc = func(task *proto.Task) {
		if int64(len(newTasks)) >= taskNum {
			return
		}
		if !isDuplicateTask(runningTasks, task) {
			newTasks = append(newTasks, task)
		}
	}
	switch clusterId {
	case "spark":
		task1 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			ZoneFilter: "ssd",
		})
		taskAddFunc(task1)
		task2 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			ZoneExcludeFilter: "ssd",
		})
		taskAddFunc(task2)
	case "mysql":
		task1 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			VolFilter: "orderdb-his",
		})
		taskAddFunc(task1)
		task2 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			VolExcludeFilter: "orderdb-his",
		})
		taskAddFunc(task2)
	default:
		task := newCheckVolumeCrcTask(clusterId, proto.Filter{})
		taskAddFunc(task)
	}
	return
}

func newCheckVolumeCrcTask(cluster string, filter proto.Filter) (task *proto.Task) {
	task = new(proto.Task)
	task.Cluster = cluster
	task.TaskType = proto.WorkerTypeCheckCrc
	crcTask := &proto.CheckCrcTaskInfo{
		CheckTiny:     false,
		Concurrency:   DefaultLimitLevel,
		ModifyTimeMin: "",
		ModifyTimeMax: "",
		RepairType:    proto.RepairVolume,
		NodeAddress:   "",
	}
	crcTask.Filter = filter
	crcTaskBytes, err := json.Marshal(crcTask)
	if err != nil {
		return
	}
	task.TaskInfo = string(crcTaskBytes)

	return
}

func (s *CrcWorker) parseConfig(cfg *config.Config) (err error) {
	return
}

func (s *CrcWorker) registerHandler() (err error) {
	//	http.HandleFunc("/addTask", s.handleAddTask)
	return
}

// ConsumeTask
// for worker node to consume single task
// if err is not empty, it candidate current task was failed, and will not retry it.
// if err is empty but restore is true, it candidate this task does not meet the processing conditions
// and task will be restored to queue to consume again
func (s *CrcWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	defer func() {
		if err != nil {
			log.LogError("ConsumeTask: failed, err:%v", err)
		}
		log.LogInfof("ConsumeTask stop, taskID:%v", task.TaskId)
	}()
	mc := s.mcw[task.Cluster]
	cluster, err := mc.AdminAPI().GetCluster()
	if err != nil {
		return true, err
	}
	dnProf := dataPortMap[strings.Split(cluster.DataNodes[0].Addr, ":")[1]]
	if dnProf == "" {
		return true, fmt.Errorf("wrong data prof")
	}
	mnProf := metaPortMap[strings.Split(cluster.MetaNodes[0].Addr, ":")[1]]
	if mnProf == "" {
		return true, fmt.Errorf("wrong meta prof")
	}
	dnPortNum, err := strconv.Atoi(dnProf)
	if err != nil {
		return
	}
	mc.DataNodeProfPort = uint16(dnPortNum)
	mnPortNum, err := strconv.Atoi(mnProf)
	if err != nil {
		return
	}
	mc.MetaNodeProfPort = uint16(mnPortNum)

	crcTaskInfo := proto.CheckCrcTaskInfo{}
	err = json.Unmarshal([]byte(task.ExceptionInfo), &crcTaskInfo)
	if err != nil {
		return
	}
	if err = validTask(crcTaskInfo); err != nil {
		return
	}
	switch crcTaskInfo.RepairType {
	case proto.RepairVolume:
		_ = data_check.ExecuteVolumeTask(int64(task.TaskId), crcTaskInfo.Concurrency, crcTaskInfo.Filter, mc, crcTaskInfo.ModifyTimeMin, crcTaskInfo.ModifyTimeMax, func() bool {
			select {
			case <-s.stopC:
				return true
			default:
				return false
			}
		})
	case proto.RepairDataNode:
		_ = data_check.ExecuteDataNodeTask(int64(task.TaskId), crcTaskInfo.Concurrency, crcTaskInfo.NodeAddress, mc, crcTaskInfo.ModifyTimeMin, crcTaskInfo.CheckTiny)
	}
	return
}

func validTask(t proto.CheckCrcTaskInfo) (err error) {
	re := regexp.MustCompile(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)+([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	if t.RepairType > proto.RepairVolume {
		err = fmt.Errorf("repair type illegal")
		return
	}
	if t.ModifyTimeMin != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ModifyTimeMin); err != nil {
			err = fmt.Errorf("modifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.ModifyTimeMax != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ModifyTimeMax); err != nil {
			err = fmt.Errorf("modifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.RepairType == proto.RepairDataNode && t.NodeAddress == "" {
		err = fmt.Errorf("nodeAddress can not be empty when repair datanode")
		return
	}
	if t.NodeAddress != "" && !re.MatchString(strings.Split(t.NodeAddress, ":")[0]) {
		err = fmt.Errorf("nodeAddress illegal")
		return
	}
	return
}

func isDuplicateTask(runningTasks []*proto.Task, task *proto.Task) bool {
	for _, t := range runningTasks {
		if t.Cluster == task.Cluster && t.TaskType == task.TaskType {
			if t.TaskInfo == task.TaskInfo {
				return true
			}
		}
	}
	return false
}
