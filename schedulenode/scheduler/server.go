package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/schedulenode/compact"
	"github.com/chubaofs/chubaofs/schedulenode/smart"
	"github.com/chubaofs/chubaofs/sdk/hbase"
	"github.com/chubaofs/chubaofs/sdk/mysql"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/iputil"
	"github.com/chubaofs/chubaofs/util/log"
	stringutil "github.com/chubaofs/chubaofs/util/string"
	"github.com/chubaofs/chubaofs/util/ump"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultQueryLimit          = 100
	DefaultNotModifiedForHours = 24 // unit: hour
)

const (
	DefaultHeartbeat      = 5 // unit: second
	DefaultLeaderPeriod   = 3
	DefaultFollowerPeriod = 3
)

const (
	DefaultLoadWorkerPeriod      = 120 // unit: second
	DefaultLoadTaskPeriod        = 120 // unit: second
	DefaultLoadFlowControlPeriod = 120 // unit: second
)

const (
	DefaultWorkerDuration = 3600 // unit: second
)

const (
	DefaultExceptionTaskHandlePeriod = 3600 // unit: second
)

const (
	DefaultFlowControlSmartVolume = 100
	DefaultFlowControlCompact     = 1000
)

const (
	DefaultWorkerMaxTaskNumSmartVolume = 100
	DefaultWorkerMaxTaskNumCompact     = 100
)

type ScheduleNode struct {
	localIp     string
	port        string
	role        string
	workers     sync.Map
	workerNodes sync.Map
	flowControl sync.Map
	candidate   *Candidate
	masterAddr  map[string][]string
	tasks       map[string][]*proto.Task
	taskLock    sync.RWMutex
	workerTypes []proto.WorkerType
	mc          *config.MysqlConfig
	ec          *config.ElectConfig
	wc          *config.WorkerConfig
	hc          *config.HBaseConfig
	hBaseClient *hbase.HBaseClient
	StopC       chan struct{}
	control     common.Control
}

func NewScheduleNode() *ScheduleNode {
	return &ScheduleNode{}
}

func (s *ScheduleNode) Start(cfg *config.Config) (err error) {
	return s.control.Start(s, cfg, doStart)
}

func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*ScheduleNode)
	if !ok {
		return errors.New("invalid node type")
	}
	s.StopC = make(chan struct{}, 0)
	s.tasks = make(map[string][]*proto.Task)

	if err = s.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	// register all running worker to schedule node
	if err = s.registerWorker(cfg); err != nil {
		log.LogErrorf("[doStart] register worker to schedule node failed, error(%v)", err)
		return
	}

	// init mysql client
	if err = mysql.InitMysqlClient(s.mc); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	// init hBase client
	s.hBaseClient = hbase.NewHBaseClient(s.hc)

	// register api handler
	s.registerHandler()

	// init ump monitor and alarm module
	exporter.Init(proto.RoleScheduleNode, proto.RoleScheduleNode, cfg)
	exporter.RegistConsul(cfg)

	// start campaign goroutine
	c := NewCandidate(fmt.Sprintf("%s:%s", s.localIp, s.port), s.ec)
	if err = c.StartCampaign(); err != nil {
		log.LogErrorf("[doStart] start schedule node failed, error(%v)", err)
		return
	}
	s.candidate = c

	s.startTaskCreator()
	go s.workerManager()
	go s.taskManager()
	go s.exceptionTaskManager()
	go s.identityMonitor()
	go s.flowControlManager()
	return
}

func (s *ScheduleNode) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("ScheduleNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc(ScheduleNodeAPIStatus, s.getScheduleStatus)
	http.HandleFunc(ScheduleNodeAPIGetLeader, s.getScheduleNodeLeader)
	http.HandleFunc(ScheduleNodeAPIListTasks, s.getTasks)
	http.HandleFunc(ScheduleNodeAPIListHisTasks, s.getTaskHistory)
	http.HandleFunc(ScheduleNodeAPIListWorkers, s.getWorkers)
	http.HandleFunc(ScheduleNodeAPIListRunningWorkers, s.getWorkersInMemory)
	http.HandleFunc(ScheduleNodeAPIListRunningTasks, s.getTasksInMemory)
	http.HandleFunc(ScheduleNodeAPICleanTask, s.cleanTask)
	http.HandleFunc(ScheduleNodeAPIFlowAdd, s.addNewFlowControl)
	http.HandleFunc(ScheduleNodeAPIFlowModify, s.modifyFlowControl)
	http.HandleFunc(ScheduleNodeAPIFlowDelete, s.deleteFlowControl)
	http.HandleFunc(ScheduleNodeAPIFlowList, s.listFlowControls)
	http.HandleFunc(ScheduleNodeAPIFlowGet, s.getFlowControlsInMemory)
	http.HandleFunc(ScheduleNodeAPIConfigAdd, s.addScheduleConfig)
	http.HandleFunc(ScheduleNodeAPIConfigUpdate, s.updateScheduleConfig)
	http.HandleFunc(ScheduleNodeAPIConfigDelete, s.deleteScheduleConfig)
	http.HandleFunc(ScheduleNodeAPIConfigSelect, s.selectScheduleConfig)
	http.HandleFunc(ScheduleNodeAPIMigrateUsing, s.selectMigrateThresholdUsing)
}

func (s *ScheduleNode) identityMonitor() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[identityMonitor] schedule node identity monitor is running...")
		select {
		case <-timer.C:
			monitorIdentity := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSchedulerIdentityMonitor)
				defer metrics.Set(err)
				// schedule node load new all worker nodes if was elected as leader again
				log.LogDebugf("[identityMonitor] IdentityChanged(%v), IsLeader(%v)", s.candidate.IdentityChanged, s.candidate.IsLeader)
				if !s.candidate.IdentityChanged {
					timer.Reset(time.Second * DefaultLoadWorkerPeriod)
					return
				}
				if s.candidate.IsLeader {
					s.loadWorkers()
				} else {
					s.cleanTaskAndWorker()
				}
				s.candidate.ResetIdentityChange()
				return
			}
			if err := monitorIdentity(); err != nil {
				log.LogErrorf("[identityMonitor] identity monitor has exception, err(%v)", err)
				return
			}
			timer.Reset(time.Second * time.Duration(s.candidate.HeartBeat))
		case <-s.StopC:
			log.LogInfof("[identityMonitor] stop schedule node identity monitor")
			return
		}
	}
}

func (s *ScheduleNode) flowControlManager() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[flowControlManager] schedule node flow control manager is running...")
		select {
		case <-timer.C:
			loadFlowControls := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSchedulerFlowControlManager)
				defer metrics.Set(err)

				keys := make(map[string]*proto.FlowControl)
				for _, wt := range s.workerTypes {
					flows, err := mysql.SelectFlowControlsViaType(wt)
					if err != nil {
						log.LogErrorf("[flowControlManager] select flow controls failed, err(%v)", err)
						continue
					}
					for _, flow := range flows {
						key := flowControlKey(wt, flow.FlowType, flow.FlowValue)
						keys[key] = flow
						s.flowControl.Store(key, flow)
					}
				}

				// clean flow controls in memory before load all new flow controls
				s.flowControl.Range(func(key, value interface{}) bool {
					k := key.(string)
					if _, ok := keys[k]; !ok {
						s.flowControl.Delete(key)
					}
					return true
				})
				return
			}

			if err := loadFlowControls(); err != nil {
				log.LogErrorf("[flowControlManager] flow control manager has exception, err(%v)", err)
				return
			}
			timer.Reset(time.Second * time.Duration(DefaultLoadFlowControlPeriod))
		case <-s.StopC:
			log.LogInfof("[flowControlManager] stop schedule node flow control manager")
			return
		}
	}
}

func (s *ScheduleNode) isLeader() bool {
	return s.candidate.IsLeader
}

func (s *ScheduleNode) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

func doShutdown(server common.Server) {
	log.LogInfof("shutdown schedule node")
	s, ok := server.(*ScheduleNode)
	if !ok {
		return
	}
	close(s.StopC)
	mysql.Close()
	s.candidate.Stop()
}

// register worker to schedule node
func (s *ScheduleNode) registerWorker(cfg *config.Config) (err error) {
	// load all running worker types
	wt := make([]proto.WorkerType, 0)
	wt = append(wt, proto.WorkerTypeSmartVolume)
	wt = append(wt, proto.WorkerTypeCompact)
	s.workerTypes = wt

	var smartVolumeWorker *smart.SmartVolumeWorker
	if smartVolumeWorker, err = smart.NewSmartVolumeWorkerForScheduler(cfg); err != nil {
		log.LogErrorf("[registerWorker] create smart volume worker failed, err(%v)", err)
		return
	}
	var compactWorker *compact.CompactWorker
	if compactWorker, err = compact.NewCompactWorkerForScheduler(cfg); err != nil {
		log.LogErrorf("[registerWorker] create compact worker failed, err(%v)", err)
		return
	}
	s.workers.Store(proto.WorkerTypeSmartVolume, smartVolumeWorker)
	s.workers.Store(proto.WorkerTypeCompact, compactWorker)
	return
}

func (s *ScheduleNode) Sync() {
	s.control.Sync()
}

// when schedule node start, load all allocated tasks once to memory
func (s *ScheduleNode) loadWorkers() {
	log.LogDebugf("[loadWorkers] schedule node start to load worker nodes")
	var failedCounter int
	for index := 0; index < len(s.workerTypes); index++ {
		if failedCounter >= 10 {
			panic("[loadWorkers] load worker nodes failed too many times.")
		}
		wt := s.workerTypes[index]
		if err := s.removeExceptionWorkerNodes(wt); err != nil {
			log.LogErrorf("[workerManager] remove exception worker nodes failed, workerType(%v), err(%v)",
				proto.WorkerTypeToName(wt), err)
			index--
			failedCounter++
			time.Sleep(time.Second)
			continue
		}

		workerNodes, err := mysql.SelectAllWorkers(int(wt))
		if err != nil {
			log.LogErrorf("[loadWorkers] select worker nodes of specified with type failed, workerType(%v), err(%v)", proto.WorkerTypeToName(wt), err)
			index--
			failedCounter++
			time.Sleep(time.Second)
			continue
		}
		// select specified type tasks that has been allocated and not finished
		tasks, err := mysql.SelectRunningTasks(int(wt))
		if err != nil {
			log.LogErrorf("[loadWorkers] select workerNodes with type failed, taskType(%v), err(%v)", wt, err)
			index--
			failedCounter++
			time.Sleep(time.Second)
			continue
		}
		// store tasks to worker
		for _, task := range tasks {
			if task.WorkerAddr == "" {
				continue
			}
			workerNode := s.getWorkerNode(task.WorkerAddr, workerNodes)
			if workerNode == nil {
				log.LogWarnf("[loadWorkers] task allocated worker not exist, taskId(%v), taskAllocatedAddr(%v), ", task.TaskId, task.WorkerAddr)
				continue
			}
			workerNode.AddTasks([]*proto.Task{task})
		}
		failedCounter = 0
		s.workerNodes.Store(wt, workerNodes)
		s.storeTasks(wt, tasks)
	}
}

func (s *ScheduleNode) storeTasks(wt proto.WorkerType, tasks []*proto.Task) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	for _, task := range tasks {
		key := flowControlKey(wt, proto.FlowTypeCluster, task.Cluster)
		ets := s.tasks[key]
		var existed bool
		for _, et := range ets {
			if et.TaskId == task.TaskId {
				existed = true
				break
			}
		}
		if !existed {
			s.tasks[key] = append(s.tasks[key], task)
		}
	}
}

func (s *ScheduleNode) storeClusterTasks(wt proto.WorkerType, cluster string, newTasks []*proto.Task) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	key := flowControlKey(wt, proto.FlowTypeCluster, cluster)
	s.tasks[key] = append(s.tasks[key], newTasks...)
}

func (s *ScheduleNode) getRunningTasks(wt proto.WorkerType, cluster string) []*proto.Task {
	s.taskLock.RLock()
	defer s.taskLock.RUnlock()
	key := flowControlKey(wt, proto.FlowTypeCluster, cluster)
	return s.tasks[key]
}

func (s *ScheduleNode) removeTask(wt proto.WorkerType, task *proto.Task) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	key := flowControlKey(wt, proto.FlowTypeCluster, task.Cluster)
	tasks, ok := s.tasks[key]
	if ok {
		for index := 0; index < len(tasks); {
			t := tasks[index]
			if task.TaskId == t.TaskId && task.TaskType == t.TaskType && task.Cluster == t.Cluster {
				tasks = append(tasks[:index], tasks[index+1:]...)
				s.tasks[key] = tasks
				log.LogDebugf("[removeTask] remove task from schedule node tasks, workerType(%v), taskId(%v), cluster(%v), volName(%v), dpId(%v), mpId(%v)",
					proto.WorkerTypeToName(wt), task.TaskId, task.Cluster, task.VolName, task.DpId, task.MpId)
				continue
			} else {
				index++
			}
		}
	}
}

func (s *ScheduleNode) cleanTaskAndWorker() {
	log.LogDebugf("[cleanTaskAndWorker] clean tasks and worker nodes in schedule node")
	s.tasks = make(map[string][]*proto.Task)
	s.workerNodes.Range(func(key, value interface{}) bool {
		s.workerNodes.Delete(key)
		return true
	})
}

func (s *ScheduleNode) getWorkerNode(workerAddr string, workers []*proto.WorkerNode) (workerNode *proto.WorkerNode) {
	for _, wn := range workers {
		if wn.WorkerAddr == workerAddr {
			return wn
		}
	}
	return
}

// manage worker node
// 1. remove exception worker nodes from database
// 2. select all active worker nodes
// 3. update worker node in schedule node, add new added worker node, remove abnormal worker node from memory
// 4. redistribute tasks of abnormal worker node
func (s *ScheduleNode) workerManager() {
	log.LogInfof("[workerManager] start worker manager.")
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[workerManager] worker manager is running")
		select {
		case <-timer.C:
			manageWorker := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSchedulerWorkerManager)
				defer metrics.Set(err)

				for _, wt := range s.workerTypes {
					if err := s.removeExceptionWorkerNodes(wt); err != nil {
						log.LogErrorf("[workerManager] remove exception worker nodes failed, workerType(%v), err(%v)",
							proto.WorkerTypeToName(wt), err)
						continue
					}

					// new worker nodes
					workerNodesNew, err := mysql.SelectAllWorkers(int(wt))
					if err != nil {
						log.LogErrorf("[workerManager] load workerNodes of specified type failed, taskType(%v), err(%v)",
							proto.WorkerTypeToName(wt), err)
						time.Sleep(time.Second)
						continue
					}

					value, ok := s.workerNodes.Load(wt)
					if !ok {
						s.workerNodes.Store(wt, workerNodesNew)
						continue
					}
					workerNodes := value.([]*proto.WorkerNode)
					// added new worker nodes
					workerNodesAdd := getWorkerNodesDiff(workerNodesNew, workerNodes)
					if len(workerNodesAdd) > 0 {
						workerNodes = append(workerNodes, workerNodesAdd...)
					}
					// remove crashed worker node from scheduleNode's worker node list
					workerNodesRemoved := getWorkerNodesDiff(workerNodes, workerNodesNew)
					log.LogDebugf("[workerManager] worker removed, workerType(%v), workerList(%v)", proto.WorkerTypeToName(wt), workerNodesRemoved)
					for _, workerNodeRemoveSingle := range workerNodesRemoved {
						for index, wn := range workerNodes {
							if wn.WorkerAddr == workerNodeRemoveSingle.WorkerAddr && wn.WorkerType == workerNodeRemoveSingle.WorkerType {
								workerNodes = append(workerNodes[:index], workerNodes[index+1:]...)
							}
						}
					}
					s.workerNodes.Store(wt, workerNodes)
					// reassign tasks of crashed worker nodes to other worker node
					if len(workerNodesRemoved) > 0 {
						for _, workerNode := range workerNodesRemoved {
							s.dispatchTaskToWorker(wt, workerNode.Tasks)
						}
					}
				}
				return
			}

			if !s.candidate.IsLeader {
				log.LogDebugf("[workerManager] current node is follower. localIp(%v)", s.localIp)
				timer.Reset(time.Second * DefaultLoadWorkerPeriod)
				continue
			}
			if err := manageWorker(); err != nil {
				log.LogErrorf("[workerManager] worker manager has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultLoadWorkerPeriod)
		case <-s.StopC:
			log.LogInfof("[workerManager] stop loading worker node list")
			return
		}
	}
}

// delete abnormal worker node, and move to history table
func (s *ScheduleNode) removeExceptionWorkerNodes(wt proto.WorkerType) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorSchedulerMoveExceptionWorkerToHistory)
	defer metrics.Set(err)

	var workerNodesException []*proto.WorkerNode
	workerNodesException, err = mysql.SelectExceptionWorkers(int(wt), s.wc.WorkerHeartbeat*s.wc.WorkerPeriod)
	if err != nil {
		log.LogErrorf("[workerManager] select exception worker nodes of specified type failed, taskType(%v), err(%v)",
			proto.WorkerTypeToName(wt), err)
		return
	}
	if workerNodesException != nil && len(workerNodesException) > 0 {
		if err = mysql.AddExceptionWorkersToHistory(workerNodesException); err != nil {
			log.LogErrorf("[workerManager] add exception worker nodes to history failed, taskType(%v), err(%v)",
				proto.WorkerTypeToName(wt), err)
			return
		}
		if err = mysql.DeleteExceptionWorkers(workerNodesException); err != nil {
			log.LogErrorf("[workerManager] delete exception worker nodes failed, taskType(%v), err(%v)",
				proto.WorkerTypeToName(wt), err)
			return
		}
	}
	return
}

// get worker nodes in set1 but not in set2
func getWorkerNodesDiff(set1, set2 []*proto.WorkerNode) (wns []*proto.WorkerNode) {
	for _, wn1 := range set1 {
		exist := false
		for _, wn2 := range set2 {
			if wn1.WorkerAddr == wn2.WorkerAddr {
				exist = true
				break
			}
		}
		if !exist {
			wns = append(wns, wn1)
		}
	}
	return
}

func (s *ScheduleNode) startTaskCreator() {
	for _, wt := range s.workerTypes {
		if value, ok := s.workers.Load(wt); ok {
			switch wt {
			case proto.WorkerTypeSmartVolume:
				sv := value.(*smart.SmartVolumeWorker)
				dr := sv.GetCreatorDuration()
				if dr <= 0 {
					log.LogWarnf("[startTaskCreator] worker duration is invalid, use default value, workerType(%v)", proto.WorkerTypeToName(wt))
					dr = DefaultWorkerDuration
				}
				go s.taskCreator(proto.WorkerTypeSmartVolume, dr, sv.CreateTask)

			case proto.WorkerTypeCompact:
				cw := value.(*compact.CompactWorker)
				dr := cw.GetCreatorDuration()
				if dr <= 0 {
					log.LogWarnf("[startTaskCreator] worker duration is invalid, use default value, workerType(%v)", proto.WorkerTypeToName(wt))
					dr = DefaultWorkerDuration
				}
				go s.taskCreator(proto.WorkerTypeCompact, dr, cw.CreateTask)

			default:
				log.LogWarnf("[startTaskCreator] unknown worker type, workerType(%v)", wt)
			}
		} else {
			log.LogErrorf("[startTaskCreator] worker not registered of type %v", wt)
			panic(fmt.Sprintf("can not find specified worker, workerType(%v)", proto.WorkerTypeToName(wt)))
		}
	}
}

// flow control when task creating.
func (s *ScheduleNode) taskCreator(wt proto.WorkerType, duration int, createTask func(cluster string, taskNum int64, rt []*proto.Task, wn []*proto.WorkerNode) (nt []*proto.Task, err error)) {
	log.LogInfof("[taskCreator] start task creator, workerType(%v), duration(%v)", proto.WorkerTypeToName(wt), duration)
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[taskCreator] task creator is running. taskType(%v)", proto.WorkerTypeToName(wt))
		select {
		case <-timer.C:
			if !s.candidate.IsLeader {
				log.LogDebugf("[taskCreator] current node is follower. taskType(%v), localIp(%v)", proto.WorkerTypeToName(wt), s.localIp)
				timer.Reset(time.Second * time.Duration(duration))
				continue
			}
			for cluster := range s.masterAddr {
				var (
					newTasks, runningTasks []*proto.Task
					flowControl            *proto.FlowControl
					taskNum                int64
					err                    error
				)
				// get cluster running tasks
				runningTasks = s.getRunningTasks(wt, cluster)
				log.LogDebugf("[taskCreator] workerType(%v), cluster(%v), runningTasksLength(%v)", proto.WorkerTypeToName(wt), cluster, len(runningTasks))

				// get cluster flow control info
				value, flowExisted := s.flowControl.Load(flowControlKey(wt, proto.FlowTypeCluster, cluster))
				if !flowExisted {
					taskNum = getDefaultFlowControlValue(wt) - int64(len(runningTasks))
					log.LogDebugf("[taskCreator] flow control not existed, workerType(%v), cluster(%v), taskNum(%v)", proto.WorkerTypeToName(wt), cluster, taskNum)
				} else {
					flowControl = value.(*proto.FlowControl)
					taskNum = flowControl.MaxNums - int64(len(runningTasks))
					log.LogDebugf("[taskCreator] flow control exist, workerType(%v), cluster(%v), taskNum(%v), flowControl.MaxNums(%v)", proto.WorkerTypeToName(wt), cluster, taskNum, flowControl.MaxNums)
				}
				if taskNum <= 0 {
					log.LogWarnf("[taskCreator] cluster has been limited to create new task, workerType(%v), cluster(%v), runningTasks(%v), taskNum(%v)",
						proto.WorkerTypeToName(wt), cluster, len(runningTasks), taskNum)
					continue
				}
				var workerNodes []*proto.WorkerNode
				v, ok := s.workerNodes.Load(wt)
				if ok {
					workerNodes = v.([]*proto.WorkerNode)
				}
				if newTasks, err = createTask(cluster, taskNum, runningTasks, workerNodes); err != nil {
					log.LogErrorf("[taskCreator] execute task creator failed, workerType(%v), cluster(%v), taskNum(%v), err(%v)",
						proto.WorkerTypeToName(wt), cluster, taskNum, err)
				}
				if len(newTasks) > 0 {
					s.storeClusterTasks(wt, cluster, newTasks)
				}
			}
			timer.Reset(time.Second * time.Duration(duration))
		case <-s.StopC:
			log.LogInfof("[taskCreator] stop task creator via stop signal")
			timer.Stop()
			return
		}
	}
}

func (s *ScheduleNode) taskManager() {
	log.LogInfof("[taskManager] start task manager.")
	timer := time.NewTimer(0)
	// ensure current goroutine listen the same channel, or not the new channel in every select
	for {
		log.LogDebugf("[taskManager] task manager is running")
		select {
		case <-timer.C:
			taskManager := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSchedulerTaskManager)
				defer metrics.Set(err)
				// select unallocated tasks from database, and allocate them to the lowest payload worker node
				for _, wt := range s.workerTypes {
					var offset = 0
					for {
						tasks, err := mysql.SelectUnallocatedTasks(int(wt), DefaultQueryLimit, offset)
						if err != nil {
							log.LogErrorf("[taskManager] select unallocated tasks failed, taskType(%v), err(%v)", proto.WorkerTypeToName(wt), err)
							break
						}
						if len(tasks) == 0 {
							break
						}
						s.dispatchTaskToWorker(wt, tasks)
						offset += len(tasks)
					}
				}
				// move succeed tasks from task table to history table
				for _, wt := range s.workerTypes {
					var offset = 0
					for {
						tasks, err := mysql.SelectSucceedTasks(int(wt), DefaultQueryLimit, offset)
						if err != nil {
							log.LogErrorf("[taskManager] select success finished tasks failed, taskType(%v), err(%v)", proto.WorkerTypeToName(wt), err)
							break
						}
						if len(tasks) == 0 {
							break
						}
						// move tasks to history
						if err = s.moveTasksToHistory(tasks, false); err != nil {
							log.LogErrorf("[taskManager] move succeed tasks to history failed, taskType(%v), tasks(%v), err(%v)", proto.WorkerTypeToName(wt), tasks, err)
							continue
						}
						// remove successful tasks from worker node who dispatched this work
						s.removeTaskFromScheduleNode(wt, tasks)
						offset += len(tasks)
					}
				}
				return
			}

			if !s.candidate.IsLeader {
				log.LogDebugf("[taskManager] current node is follower. localIp(%v)", s.localIp)
				timer.Reset(time.Second * DefaultLoadTaskPeriod)
				continue
			}
			if err := taskManager(); err != nil {
				log.LogErrorf("[taskManager] task manager has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultLoadTaskPeriod)
		case <-s.StopC:
			log.LogInfof("[taskManager] stop task manager via stop signal")
			timer.Stop()
			return
		}
	}
}

func (s *ScheduleNode) exceptionTaskManager() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[exceptionTaskManager] exception task manager is running")
		select {
		case <-timer.C:
			manageExceptionTask := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSchedulerExceptionTaskManager)
				defer metrics.Set(err)

				for _, wt := range s.workerTypes {
					var offset = 0
					for {
						tasks, err := mysql.SelectExceptionTasks(int(wt), DefaultQueryLimit, offset)
						log.LogDebugf("[exceptionTaskManager] taskType(%v), tasks(%v)", proto.WorkerTypeToName(wt), tasks)
						if err != nil {
							log.LogErrorf("[exceptionTaskManager] select failed tasks failed, taskType(%v), err(%v)", proto.WorkerTypeToName(wt), err)
							break
						}
						if len(tasks) == 0 {
							break
						}
						var longExcTasks []*proto.Task
						for _, task := range tasks {
							if time.Now().Sub(task.UpdateTime).Seconds() > DefaultExceptionTaskHandlePeriod {
								longExcTasks = append(longExcTasks, task)
							}
						}
						if len(longExcTasks) == 0 {
							break
						}
						log.LogDebugf("[exceptionTaskManager] taskType(%v), tasks(%v), longExcTasks(%v)", proto.WorkerTypeToName(wt), tasks, longExcTasks)
						// remove exceptional tasks from worker node who dispatched this work
						s.removeTaskFromScheduleNode(wt, longExcTasks)
						s.dispatchTaskToWorker(wt, longExcTasks)
						s.storeTasks(wt, longExcTasks)
						offset += len(tasks)

						// move failed tasks to history
						if err = s.moveTasksToHistory(longExcTasks, true); err != nil {
							log.LogErrorf("[exceptionTaskManager] move tasks to history failed, taskType(%v), longExcTasks(%v), err(%v)", proto.WorkerTypeToName(wt), longExcTasks, err)
							continue
						}
					}

					// select not modified tasks for a long time, task is regarded as failed if not be modified in 24 hours
					offset = 0
					for {
						tasks, err := mysql.SelectNotModifiedForLongTime(int(wt), DefaultNotModifiedForHours, DefaultQueryLimit, offset)
						if err != nil {
							log.LogErrorf("[exceptionTaskManager] select tasks that not modified for a long time, taskType(%v), err(%v)", proto.WorkerTypeToName(wt), err)
							break
						}
						if len(tasks) == 0 {
							break
						}

						// have tasks that not modified for a long time
						var taskIds []string
						for _, task := range tasks {
							taskIds = append(taskIds, strconv.FormatUint(task.TaskId, 10))
						}
						alarmKey := "have tasks that not modified for a long time"
						alarmInfo := fmt.Sprintf("have tasks that not modified for a long time, taskIds(%v)", strings.Join(taskIds, ","))
						log.LogErrorf("[exceptionTaskManager] %v", alarmInfo)
						ump.Alarm(alarmKey, alarmInfo)

						// remove exceptional tasks from worker node who dispatched this work
						s.removeTaskFromScheduleNode(wt, tasks)
						// dispatch exceptional tasks to new worker node
						s.dispatchTaskToWorker(wt, tasks)
						offset += len(tasks)

						// move failed tasks to history
						if err = s.moveTasksToHistory(tasks, true); err != nil {
							log.LogErrorf("[exceptionTaskManager] move tasks to history failed, taskType(%v), tasks(%v), err(%v)", proto.WorkerTypeToName(wt), tasks, err)
							continue
						}
					}
				}
				return
			}

			if !s.candidate.IsLeader {
				log.LogDebugf("[exceptionTaskManager] current node is follower. localIp(%v)", s.localIp)
				timer.Reset(time.Second * DefaultLoadTaskPeriod)
				continue
			}
			if err := manageExceptionTask(); err != nil {
				log.LogErrorf("[exceptionTaskManager] exception task manager has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultLoadTaskPeriod)
		case <-s.StopC:
			log.LogInfof("[exceptionTaskManager] stop task exception manager via stop signal")
			timer.Stop()
			return
		}
	}
}

func (s *ScheduleNode) removeTaskFromScheduleNode(wt proto.WorkerType, tasks []*proto.Task) {
	log.LogDebugf("[removeTaskFromScheduleNode] wt(%v), tasks(%v)", proto.WorkerTypeToName(wt), mysql.JoinTaskIds(tasks))
	value, ok := s.workerNodes.Load(wt)
	if !ok {
		return
	}
	wns := value.([]*proto.WorkerNode)

	// remove task from worker node and schedule node tasks list
	for _, task := range tasks {
		for _, workerNode := range wns {
			if workerNode.ContainTaskByTaskId(task) {
				workerNode.RemoveTask(task)
			}
		}
	}
	for _, task := range tasks {
		s.removeTask(wt, task)
	}
}

func (s *ScheduleNode) dispatchTaskToWorker(wt proto.WorkerType, tasks []*proto.Task) {
	metrics := exporter.NewTPCnt(proto.MonitorSchedulerDispatchTask)
	defer metrics.Set(nil)

	if len(tasks) == 0 {
		return
	}
	var worker *proto.WorkerNode
	for _, task := range tasks {
		if worker = s.getLowestPayloadWorker(wt); worker == nil {
			log.LogErrorf("[dispatchTaskToWorker] get lowest payload worker is empty, workerType(%v), cluster(%v), volName(%v), dpId(%v), taskId(%v)",
				proto.WorkerTypeToName(wt), task.Cluster, task.VolName, task.DpId, task.TaskId)
			return
		}
		// modified tasks status and worker address
		if err := mysql.AllocateTask(task, worker.WorkerAddr); err != nil {
			log.LogErrorf("[dispatchTaskToWorker] dispatch task to worker in database failed, workerType(%v), workerAddr(%v), cluster(%v), volName(%v), dpId(%v), taskId(%v), err(%v)",
				proto.WorkerTypeToName(wt), worker.WorkerAddr, task.Cluster, task.VolName, task.DpId, task.TaskId, err.Error())
			continue
		}
		// dispatch tasks to selected lowest load worker
		log.LogDebugf("[dispatchTaskToWorker] dispatch task to worker, taskId(%v), cluster(%v), volName(%v), dpId(%v), workerAddr(%v)",
			task.TaskId, task.Cluster, task.VolName, task.DpId, worker.WorkerAddr)
		task.WorkerAddr = worker.WorkerAddr
		worker.AddTask(task)
	}
}

func (s *ScheduleNode) getLowestPayloadWorker(wt proto.WorkerType) (worker *proto.WorkerNode) {
	value, ok := s.workerNodes.Load(wt)
	if !ok {
		return nil
	}
	wns := value.([]*proto.WorkerNode)

	var least = math.MaxInt32
	for _, wn := range wns {
		// check worker is whether can accept new task
		taskNum := s.getWorkerMaxTaskNums(wt, wn.WorkerAddr)
		if int64(len(wn.Tasks)) >= taskNum {
			continue
		}
		if len(wn.Tasks) < least {
			worker = wn
			least = len(wn.Tasks)
		}
	}
	return
}

func (s *ScheduleNode) getWorkerMaxTaskNums(wt proto.WorkerType, workerAddr string) (taskNum int64) {
	flowKey := flowControlKey(wt, proto.FlowTypeWorker, workerAddr)
	v, ok := s.flowControl.Load(flowKey)
	if ok {
		flow := v.(*proto.FlowControl)
		taskNum = flow.MaxNums
	} else {
		switch wt {
		case proto.WorkerTypeSmartVolume:
			taskNum = DefaultWorkerMaxTaskNumSmartVolume
		case proto.WorkerTypeCompact:
			taskNum = DefaultWorkerMaxTaskNumCompact
		}
	}
	return
}

func (s *ScheduleNode) moveTasksToHistory(tasks []*proto.Task, saveOld bool) (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorSchedulerMoveTaskToHistory)
	defer metrics.Set(nil)

	// add to
	if err = mysql.AddTasksToHistory(tasks); err != nil {
		log.LogErrorf("[moveTasksToHistory] add tasks to hBase failed, tasks(%v), err(%v)", tasks, err)
		return
	}
	if !saveOld {
		if err = mysql.DeleteTasks(tasks); err != nil {
			log.LogErrorf("[moveTasksToHistory] move tasks to history failed, tasks(%v), err(%v)", tasks, err)
			return
		}
	}
	return nil
}

func (s *ScheduleNode) parseConfig(cfg *config.Config) (err error) {
	var regexpPort *regexp.Regexp
	localIP := cfg.GetString(config.ConfigKeyLocalIP)
	if stringutil.IsStrEmpty(localIP) {
		if localIP, err = iputil.GetLocalIPByDial(); err != nil {
			return
		}
	}
	port := cfg.GetString(config.ConfigKeyProfPort)
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return fmt.Errorf("error: no port")
	}
	if !regexpPort.MatchString(port) {
		return fmt.Errorf("error: port must string")
	}

	s.port = port
	s.localIp = localIP
	// parse cluster master address
	masters := make(map[string][]string)
	masterBaseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	var masterAddr []string
	for clusterName, value := range masterBaseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		if len(masterAddr) == 0 {
			masterAddr = addresses
		}
		masters[clusterName] = addresses
	}
	s.masterAddr = masters
	// used for cmd to report version
	if len(masterAddr) == 0 {
		cfg.SetStringSlice(proto.MasterAddr, masterAddr)
	}
	// parse leader elect config
	hb := cfg.GetInt64(config.ConfigKeyHeartbeat)
	lp := cfg.GetInt64(config.ConfigKeyLeaderPeriod)
	fp := cfg.GetInt64(config.ConfigKeyFollowerPeriod)
	if hb <= 0 {
		hb = DefaultHeartbeat
	}
	if lp <= 0 {
		lp = DefaultLeaderPeriod
	}
	if fp <= 0 {
		fp = DefaultFollowerPeriod
	}
	s.ec = config.NewElectConfig(int(hb), int(lp), int(fp))

	// worker node heartbeat config
	whb := cfg.GetInt64(config.ConfigKeyWorkerHeartbeat)
	wp := cfg.GetInt64(config.ConfigKeyWorkerPeriod)
	if whb <= 0 {
		whb = proto.DefaultWorkerHeartbeat
	}
	if wp <= 0 {
		wp = proto.DefaultWorkerPeriod
	}
	s.wc = config.NewWorkerConfig(int(whb), int(wp), 0)

	// parse mysql config
	var (
		mysqlUrl, mysqlUserName, mysqlPassword, mysqlDatabase string
		maxIdleConns, maxOpenConns                            int
		passFilePath                                          string
		mysqlPort                                             float64
	)
	baseInfo := cfg.GetMap(config.ConfigKeyMysql)
	for clusterName, value := range baseInfo {
		switch clusterName {
		case config.ConfigKeyMysqlUrl:
			if vs, ok := value.(string); ok {
				mysqlUrl = vs
			}
		case config.ConfigKeyUserName:
			if vs, ok := value.(string); ok {
				mysqlUserName = vs
			}
		case config.ConfigKeyPassword:
			if vs, ok := value.(string); ok {
				mysqlPassword = vs
			}
		case config.ConfigKeyPassFile:
			if vi, ok := value.(string); ok {
				passFilePath = vi
			}
		case config.ConfigKeyDatabase:
			if vs, ok := value.(string); ok {
				mysqlDatabase = vs
			}
		case config.ConfigKeyPort:
			if vi, ok := value.(float64); ok {
				mysqlPort = vi
			}
		case config.ConfigKeyIdleConn:
			if vi, ok := value.(int); ok {
				maxIdleConns = vi
			}
		case config.ConfigKeyOpenConn:
			if vi, ok := value.(int); ok {
				maxOpenConns = vi
			}
		}
	}
	if stringutil.IsStrEmpty(mysqlUrl) {
		return fmt.Errorf("error: no mysql url")
	}
	if stringutil.IsStrEmpty(mysqlUserName) {
		return fmt.Errorf("error: no mysql username")
	}
	if stringutil.IsStrEmpty(mysqlDatabase) {
		return fmt.Errorf("error: no mysql database")
	}
	if stringutil.IsStrEmpty(mysqlPassword) && stringutil.IsStrEmpty(passFilePath) {
		return fmt.Errorf("error: mysql password and pass file path can not be both empty")
	}
	if stringutil.IsStrEmpty(mysqlPassword) {
		mysqlPassword, err = mysql.ParsePassword(passFilePath, config.ConfigKeyEncryptPass)
		if err != nil {
			log.LogDebugf("[parseConfig] parse mysql password from encrypted file, err(%v)", err.Error())
			return err
		}
		if stringutil.IsStrEmpty(mysqlPassword) {
			return errors.New("parsed mysql password from encrypted file is empty")
		}
	}
	if mysqlPort <= 0 {
		mysqlPort = proto.DefaultMysqlPort
	}
	if maxIdleConns <= 0 {
		maxIdleConns = proto.DefaultMysqlMaxIdleConns
	}
	if maxOpenConns <= 0 {
		maxOpenConns = proto.DefaultMysqlMaxOpenConns
	}
	s.mc = config.NewMysqlConfig(mysqlUrl, mysqlUserName, mysqlPassword, mysqlDatabase, int(mysqlPort), maxIdleConns, maxOpenConns)

	// parse HBase config for smart volume worker createTask function
	hBaseUrl := cfg.GetString(config.ConfigKeyHBaseUrl)
	if stringutil.IsStrEmpty(hBaseUrl) {
		return fmt.Errorf("error: no hBase url")
	}
	s.hc = config.NewHBaseConfig(hBaseUrl)
	return
}

func flowControlKey(wt proto.WorkerType, flowType, flowValue string) string {
	return fmt.Sprintf("%s_%s_%s", proto.WorkerTypeToName(wt), flowType, flowValue)
}

func getDefaultFlowControlValue(wt proto.WorkerType) int64 {
	switch wt {
	case proto.WorkerTypeSmartVolume:
		return DefaultFlowControlSmartVolume
	case proto.WorkerTypeCompact:
		return DefaultFlowControlCompact
	default:
		log.LogErrorf("[getDefaultFlowControlValue] invalid worker type, workerType(%v)", wt)
		return 0
	}
}
