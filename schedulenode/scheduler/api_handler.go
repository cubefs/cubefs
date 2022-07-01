package scheduler

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/schedulenode/smart"
	"github.com/chubaofs/chubaofs/sdk/mysql"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/buf"
	"net/http"
	"net/http/httputil"
	"strconv"
)

const (
	ParamKeyCluster     = "cluster"
	ParamKeyVolume      = "volume"
	ParamKeyDPId        = "dpId"
	ParamKeyMPid        = "mdId"
	ParamKeyTaskType    = "taskType"
	ParamKeyTaskId      = "taskId"
	ParamKeyFlowType    = "flowType"
	ParamKeyFlowValue   = "flowValue"
	ParamKeyLimit       = "limit"
	ParamKeyOffset      = "offset"
	ParamKeyWorkerType  = "workerType"
	ParamKeyWorkerAddr  = "workerAddr"
	ParamKeyMaxNum      = "maxNum"
	ParamKeyConfigType  = "confType"
	ParamKeyConfigKey   = "key"
	ParamKeyConfigValue = "value"
)

const (
	DefaultLimitValue = 100
)

const (
	ScheduleNodeAPIStatus             = "/scheduleNode/status"
	ScheduleNodeAPIGetLeader          = "/scheduleNode/leader"
	ScheduleNodeAPIListTasks          = "/task/list"
	ScheduleNodeAPIListHisTasks       = "/task/history/list"
	ScheduleNodeAPIListWorkers        = "/worker/list"
	ScheduleNodeAPIListRunningWorkers = "/worker/running/list"
	ScheduleNodeAPIListRunningTasks   = "/task/running/list"
	ScheduleNodeAPICleanTask          = "/task/clean"
	ScheduleNodeAPIFlowAdd            = "/flow/add"
	ScheduleNodeAPIFlowModify         = "/flow/modify"
	ScheduleNodeAPIFlowDelete         = "/flow/delete"
	ScheduleNodeAPIFlowList           = "/flow/list"
	ScheduleNodeAPIFlowGet            = "/flow/get"
	ScheduleNodeAPIConfigAdd          = "/config/add"
	ScheduleNodeAPIConfigUpdate       = "/config/update"
	ScheduleNodeAPIConfigDelete       = "/config/delete"
	ScheduleNodeAPIConfigSelect       = "/config/select"
	ScheduleNodeAPIMigrateUsing       = "/migrate/using"
)

func (s *ScheduleNode) getScheduleStatus(w http.ResponseWriter, r *http.Request) {
	s.buildSuccessResp(w, fmt.Sprintf("scheduleNode(%s:%s) is running", s.localIp, s.port))
}

func (s *ScheduleNode) getScheduleNodeLeader(w http.ResponseWriter, r *http.Request) {
	var le *proto.LeaderElect
	var err error
	if le, err = mysql.GetLeader(s.candidate.HeartBeat * s.candidate.LeaderPeriod); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, le)
}

func (s *ScheduleNode) getTasks(w http.ResponseWriter, r *http.Request) {
	var (
		cluster, volume         string
		dpId, mpId              uint64
		limit, offset, taskType int
		tasks                   []*proto.Task
		err                     error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	cluster = r.FormValue(ParamKeyCluster)
	volume = r.FormValue(ParamKeyVolume)
	if !util.IsStrEmpty(r.FormValue(ParamKeyDPId)) {
		if dpId, err = strconv.ParseUint(r.FormValue(ParamKeyDPId), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyDPId, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyMPid)) {
		if mpId, err = strconv.ParseUint(r.FormValue(ParamKeyMPid), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyMPid, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyTaskType)) {
		if taskType, err = strconv.Atoi(r.FormValue(ParamKeyTaskType)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskType, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyLimit)) {
		if limit, err = strconv.Atoi(r.FormValue(ParamKeyLimit)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyLimit, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyOffset)) {
		if offset, err = strconv.Atoi(r.FormValue(ParamKeyOffset)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit == 0 {
		limit = DefaultLimitValue
	}
	tasks, err = mysql.SelectTasks(cluster, volume, dpId, mpId, taskType, limit, offset)
	if err != nil {
		err = fmt.Errorf("select tasks failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, tasks)
}

func (s *ScheduleNode) getTaskHistory(w http.ResponseWriter, r *http.Request) {
	var (
		cluster, volume         string
		dpId, mpId              uint64
		limit, offset, taskType int
		tasks                   []*proto.TaskHistory
		err                     error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	cluster = r.FormValue(ParamKeyCluster)
	volume = r.FormValue(ParamKeyVolume)
	if !util.IsStrEmpty(r.FormValue(ParamKeyDPId)) {
		if dpId, err = strconv.ParseUint(r.FormValue(ParamKeyDPId), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyDPId, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyMPid)) {
		if mpId, err = strconv.ParseUint(r.FormValue(ParamKeyMPid), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyMPid, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyTaskType)) {
		if taskType, err = strconv.Atoi(r.FormValue(ParamKeyTaskType)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskType, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyLimit)) {
		if limit, err = strconv.Atoi(r.FormValue(ParamKeyLimit)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyLimit, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyOffset)) {
		if offset, err = strconv.Atoi(r.FormValue(ParamKeyOffset)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit == 0 {
		limit = DefaultLimitValue
	}
	tasks, err = mysql.SelectTaskHistory(cluster, volume, dpId, mpId, taskType, limit, offset)
	if err != nil {
		err = fmt.Errorf("select task history failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, tasks)
}

func (s *ScheduleNode) getWorkers(w http.ResponseWriter, r *http.Request) {
	var (
		err                       error
		workerType, limit, offset int
		workerAddr                string
		workers                   []*proto.WorkerNode
	)

	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyWorkerType)) {
		if workerType, err = strconv.Atoi(r.FormValue(ParamKeyWorkerType)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyWorkerType, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	workerAddr = r.FormValue(ParamKeyWorkerAddr)
	if !util.IsStrEmpty(r.FormValue(ParamKeyLimit)) {
		if limit, err = strconv.Atoi(r.FormValue(ParamKeyLimit)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyLimit, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !util.IsStrEmpty(r.FormValue(ParamKeyOffset)) {
		if offset, err = strconv.Atoi(r.FormValue(ParamKeyOffset)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit == 0 {
		limit = DefaultLimitValue
	}
	workers, err = mysql.SelectWorkers(workerType, workerAddr, limit, offset)
	if err != nil {
		err = fmt.Errorf("select workers failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, workers)
}

func (s *ScheduleNode) getWorkersInMemory(w http.ResponseWriter, r *http.Request) {
	if !s.candidate.IsLeader {
		proxy := s.newProxy()
		proxy.ServeHTTP(w, r)
	} else {
		workerNodes := make(map[string][]*proto.WorkerNode)
		s.workerNodes.Range(func(key, value interface{}) bool {
			workerType, _ := key.(proto.WorkerType)
			nodes, _ := value.([]*proto.WorkerNode)
			workerTypeString := proto.WorkerTypeToName(workerType)
			workerNodes[workerTypeString] = nodes
			return true
		})
		s.buildSuccessResp(w, workerNodes)
	}
}

func (s *ScheduleNode) getTasksInMemory(w http.ResponseWriter, r *http.Request) {
	if !s.candidate.IsLeader {
		proxy := s.newProxy()
		proxy.ServeHTTP(w, r)
	} else {
		s.buildSuccessResp(w, s.tasks)
	}
}

func (s *ScheduleNode) cleanTask(w http.ResponseWriter, r *http.Request) {
	if !s.candidate.IsLeader {
		proxy := s.newProxy()
		proxy.ServeHTTP(w, r)
	} else {
		var (
			cluster, volume string
			taskType        int
			taskId          int64
			err             error
		)
		if err = r.ParseForm(); err != nil {
			err = fmt.Errorf("parse form fail: %v", err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if util.IsStrEmpty(r.FormValue(ParamKeyCluster)) {
			err = fmt.Errorf("param %v can not be empty", ParamKeyCluster)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if util.IsStrEmpty(r.FormValue(ParamKeyVolume)) {
			err = fmt.Errorf("param %v can not be empty", ParamKeyVolume)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if util.IsStrEmpty(r.FormValue(ParamKeyTaskType)) {
			err = fmt.Errorf("param %v can not be empty", ParamKeyTaskType)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if util.IsStrEmpty(r.FormValue(ParamKeyTaskId)) {
			err = fmt.Errorf("param %v can not be empty", ParamKeyTaskId)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		cluster = r.FormValue(ParamKeyCluster)
		volume = r.FormValue(ParamKeyVolume)
		if taskType, err = strconv.Atoi(r.FormValue(ParamKeyTaskType)); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskType, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if taskId, err = strconv.ParseInt(r.FormValue(ParamKeyTaskId), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskId, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}

		task := &proto.Task{
			TaskId:   uint64(taskId),
			TaskType: proto.WorkerType(taskType),
			Cluster:  cluster,
			VolName:  volume,
		}
		s.removeTaskFromScheduleNode(proto.WorkerType(taskType), []*proto.Task{task})
		if err = mysql.DeleteTaskByVolumeAndId(cluster, volume, taskType, taskId); err != nil {
			err = fmt.Errorf("delete task failed: %v", err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		s.buildSuccessResp(w, "delete task success")
	}
}

func (s *ScheduleNode) addNewFlowControl(w http.ResponseWriter, r *http.Request) {
	var (
		flow *proto.FlowControl
		err  error
	)
	flow, err = parseParamFlowControl(r)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = mysql.AddFlowControl(flow); err != nil {
		err = fmt.Errorf("add flow control failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "add flow control success")
}

func (s *ScheduleNode) modifyFlowControl(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		flow *proto.FlowControl
	)
	flow, err = parseParamFlowControl(r)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = mysql.UpdateFlowControl(flow); err != nil {
		err = fmt.Errorf("modify flow control failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "modify flow control success")
}

func (s *ScheduleNode) deleteFlowControl(w http.ResponseWriter, r *http.Request) {
	var (
		taskType  int
		flowType  string
		flowValue string
		err       error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyTaskType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyTaskType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if taskType, err = strconv.Atoi(r.FormValue(ParamKeyTaskType)); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskType, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyFlowType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyFlowType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	flowType = r.FormValue(ParamKeyFlowType)
	if flowType != proto.FlowTypeCluster && flowType != proto.FlowTypeWorker {
		err = fmt.Errorf("invalid flow type: %v", flowType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyFlowValue)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyFlowValue)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	flowValue = r.FormValue(ParamKeyFlowValue)

	flow := &proto.FlowControl{
		WorkerType: proto.WorkerType(taskType),
		FlowType:   flowType,
		FlowValue:  flowValue,
	}
	if err = mysql.DeleteFlowControl(flow); err != nil {
		err = fmt.Errorf("delete flow control failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "delete flow control success")
}

func (s *ScheduleNode) listFlowControls(w http.ResponseWriter, r *http.Request) {
	var (
		flows []*proto.FlowControl
		err   error
	)
	if flows, err = mysql.SelectFlowControls(); err != nil {
		err = fmt.Errorf("list all flow controls failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, flows)
}

func (s *ScheduleNode) getFlowControlsInMemory(w http.ResponseWriter, r *http.Request) {
	flows := make(map[string]*proto.FlowControl)
	s.flowControl.Range(func(key, value interface{}) bool {
		flowKey := key.(string)
		flow := value.(*proto.FlowControl)
		flows[flowKey] = flow
		return true
	})
	s.buildSuccessResp(w, flows)
}

func (s *ScheduleNode) addScheduleConfig(w http.ResponseWriter, r *http.Request) {
	var (
		sc  *proto.ScheduleConfig
		err error
	)
	sc, err = parseParamScheduleConfig(r)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = mysql.AddScheduleConfig(sc); err != nil {
		err = fmt.Errorf("add schedule config failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "add schedule config success")
}

func (s *ScheduleNode) updateScheduleConfig(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		sc  *proto.ScheduleConfig
	)
	sc, err = parseParamScheduleConfig(r)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = mysql.UpdateScheduleConfig(sc); err != nil {
		err = fmt.Errorf("modify schedule config failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "modify schedule config success")
}

func (s *ScheduleNode) deleteScheduleConfig(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		sc       *proto.ScheduleConfig
		confType int
		confKey  string
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyConfigType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if confType, err = strconv.Atoi(r.FormValue(ParamKeyConfigType)); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyConfigType, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if confType != int(proto.ScheduleConfigTypeMigrateThreshold) {
		err = fmt.Errorf("invalid config type: %v", confType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	confKey = r.FormValue(ParamKeyConfigKey)
	if util.IsStrEmpty(confKey) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigKey)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	sc = &proto.ScheduleConfig{
		ConfigType: proto.ScheduleConfigType(confType),
		ConfigKey:  confKey,
	}
	if err = mysql.DeleteScheduleConfig(sc); err != nil {
		err = fmt.Errorf("delete schedule config failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, "delete schedule config success")
}

func (s *ScheduleNode) selectScheduleConfig(w http.ResponseWriter, r *http.Request) {
	var (
		scs      []*proto.ScheduleConfig
		err      error
		confType int
	)
	if util.IsStrEmpty(r.FormValue(ParamKeyConfigType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if confType, err = strconv.Atoi(r.FormValue(ParamKeyConfigType)); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyConfigType, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if confType != int(proto.ScheduleConfigTypeMigrateThreshold) {
		err = fmt.Errorf("invalid config type: %v", confType)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if scs, err = mysql.SelectScheduleConfig(proto.ScheduleConfigType(confType)); err != nil {
		err = fmt.Errorf("list scedule config infos failed: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	s.buildSuccessResp(w, scs)
}

func (s *ScheduleNode) selectMigrateThresholdUsing(w http.ResponseWriter, r *http.Request) {
	value, exist := s.workers.Load(proto.WorkerTypeSmartVolume)
	if !exist {
		s.buildFailureResp(w, http.StatusBadRequest, "smart volume worker not found")
		return
	}
	sv, ok := value.(*smart.SmartVolumeWorker)
	if !ok {
		s.buildFailureResp(w, http.StatusBadRequest, "smart volume worker type is invalid")
		return
	}
	s.buildSuccessResp(w, sv.MigrateThreshold())
}

func (s *ScheduleNode) newProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL.Scheme = "http"
			request.URL.Host = s.candidate.LeaderAddr
		},
		BufferPool: buf.NewBytePool(1000, 32*1024),
	}
}

func parseParamScheduleConfig(r *http.Request) (flow *proto.ScheduleConfig, err error) {
	var (
		confType  int
		confKey   string
		confValue string
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyConfigType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigType)
		return
	}
	if confType, err = strconv.Atoi(r.FormValue(ParamKeyConfigType)); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyConfigType, err)
		return
	}
	if confType != int(proto.ScheduleConfigTypeMigrateThreshold) {
		err = fmt.Errorf("invalid config type: %v", confType)
		return
	}
	confKey = r.FormValue(ParamKeyConfigKey)
	if util.IsStrEmpty(confKey) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigKey)
		return
	}
	confValue = r.FormValue(ParamKeyConfigValue)
	if util.IsStrEmpty(confValue) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyConfigValue)
		return
	}
	// if config type is migrate threshold, value must be float64 and less then 1
	var migrateThreshold float64
	if migrateThreshold, err = strconv.ParseFloat(confValue, 64); err != nil {
		err = fmt.Errorf("parse migrate threshold failed: %v", err.Error())
		return
	}
	if migrateThreshold <= 0 || migrateThreshold >= 1 {
		err = fmt.Errorf("migrate threshold value is invalid")
		return
	}
	return proto.NewScheduleConfig(proto.ScheduleConfigType(confType), confKey, confValue), nil
}

func parseParamFlowControl(r *http.Request) (flow *proto.FlowControl, err error) {
	var (
		taskType  int
		flowType  string
		flowValue string
		maxNum    int64
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyTaskType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyTaskType)
		return
	}
	if taskType, err = strconv.Atoi(r.FormValue(ParamKeyTaskType)); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyTaskType, err)
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyFlowType)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyFlowType)
		return
	}
	flowType = r.FormValue(ParamKeyFlowType)
	if flowType != proto.FlowTypeCluster && flowType != proto.FlowTypeWorker {
		err = fmt.Errorf("invalid flow type: %v", flowType)
		return
	}
	if util.IsStrEmpty(r.FormValue(ParamKeyFlowValue)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyFlowValue)
		return
	}
	flowValue = r.FormValue(ParamKeyFlowValue)
	if util.IsStrEmpty(r.FormValue(ParamKeyMaxNum)) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyMaxNum)
		return
	}
	if maxNum, err = strconv.ParseInt(r.FormValue(ParamKeyMaxNum), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", ParamKeyMaxNum, err)
		return
	}
	return proto.NewFlowControl(proto.WorkerType(taskType), flowType, flowValue, maxNum)
}

func (s *ScheduleNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildJSONResp(w, http.StatusOK, data, "")
}

func (s *ScheduleNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func (s *ScheduleNode) buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
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
