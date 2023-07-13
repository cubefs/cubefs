package proto

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	RoleScheduleNode        = "scheduleNode"
	RoleSmartVolumeWorker   = "smartVolume"
	RoleCompactWorker       = "compact"
	RoleDPReBalanceWorker   = "dpReBalance"
	RoleDiskReBalanceWorker = "diskReBalance"
)

const (
	DefaultMysqlPort         = 3306
	DefaultMysqlMaxIdleConns = 0
	DefaultMysqlMaxOpenConns = 50
)

const (
	DefaultWorkerHeartbeat = 5 // unit: second
	DefaultWorkerPeriod    = 10
)

type TaskStatus uint8

type WorkerType uint8

type FlowType uint8

const (
	FlowTypeCluster = "cluster"
	FlowTypeWorker  = "worker"
)

const (
	ModuleTypeMeta = "mp"
	ModuleTypeData = "dp"
)

const (
	ActionMetricsTimeTypeReserved = iota
	ActionMetricsTimeTypeMinute
	ActionMetricsTimeTypeHour
	ActionMetricsTimeTypeDay
	ActionMetricsTimeTypeStringMinute = "minute"
	ActionMetricsTimeTypeStringHour   = "hour"
	ActionMetricsTimeTypeStringDay    = "day"
)

const (
	DPCreateTimeTypeReserved = iota
	DPCreateTimeTypeTimestamp
	DPCreateTimeTypeDays
	DPCreateTimeTypeStringTimestamp = "timestamp"
	DPCreateTimeTypeStringDays      = "days"
)

const (
	DataTypeReserved = iota
	DataTypeCount
	DataTypeSize
	DataTypeStringCount = "count"
	DataTypeStringSize  = "size"
)

const (
	TaskStatusUnallocated TaskStatus = iota
	TaskStatusAllocated
	TaskStatusPending
	TaskStatusProcessing
	TaskStatusSucceed
	TaskStatusFailed
)

const (
	WorkerTypeReserved WorkerType = iota
	WorkerTypeSmartVolume
	WorkerTypeCompact
	WorkerTypeDPReBalance
	WorkerTypeDiskReBalance
)

var workerTypeMap = map[WorkerType]string{
	WorkerTypeSmartVolume:   "smartVolume",
	WorkerTypeCompact:       "compact",
	WorkerTypeDPReBalance:   "dpRebalance",
	WorkerTypeDiskReBalance: "diskRebalance",
}

func WorkerTypeToName(wt WorkerType) string {
	if name, exist := workerTypeMap[wt]; exist {
		return name
	}
	return ""
}

type LeaderElect struct {
	Term       uint64
	LeaderAddr string
	CreateTime time.Time
	UpdateTime time.Time
}

type Task struct {
	TaskId        uint64
	TaskType      WorkerType
	Cluster       string
	VolName       string
	DpId          uint64
	MpId          uint64
	TaskInfo      string
	WorkerAddr    string
	ExceptionInfo string
	Status        TaskStatus
	CreateTime    time.Time
	UpdateTime    time.Time
}

func NewDataTask(wt WorkerType, cluster, volName string, dpId uint64, mpId uint64, taskInfo string) *Task {
	return &Task{
		TaskType: wt,
		Cluster:  cluster,
		VolName:  volName,
		DpId:     dpId,
		MpId:     mpId,
		TaskInfo: taskInfo,
	}
}

func (t *Task) String() string {
	data, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(data)
	//sb := strings.Builder{}
	//sb.WriteString("Cluster:")
	//sb.WriteString(t.Cluster)
	//sb.WriteString("VolName:")
	//sb.WriteString(t.VolName)
	//sb.WriteString("DpId:")
	//sb.WriteString(strconv.FormatUint(t.DpId, 10))
	//sb.WriteString("MpId:")
	//sb.WriteString(strconv.FormatUint(t.MpId, 10))
	//sb.WriteString("TaskId:")
	//sb.WriteString(strconv.FormatUint(t.TaskId, 10))
	//sb.WriteString("TaskType:")
	//sb.WriteString(WorkerTypeToName(t.TaskType))
	//sb.WriteString(t.TaskInfo)
	//sb.WriteString(t.WorkerAddr)
	//sb.WriteString(strconv.Itoa(int(t.Status)))
	//sb.WriteString(t.CreateTime.String())
	//sb.WriteString(t.UpdateTime.String())
	//return sb.String()
}

type TaskHistory struct {
	Task
	TaskCreateTime time.Time
	TaskUpdateTime time.Time
}

func NewMetaTask(tt WorkerType, cluster, volName string, mpId uint64, taskInfo string) *Task {
	return &Task{
		TaskType: tt,
		Cluster:  cluster,
		VolName:  volName,
		MpId:     mpId,
		TaskInfo: taskInfo,
	}
}

type WorkerNode struct {
	WorkerId   uint64
	WorkerType WorkerType
	WorkerAddr string
	CreateTime time.Time
	UpdateTime time.Time
	Tasks      []*Task
	TaskLock   sync.RWMutex `json:"-"`
}

func NewWorkerNode(workerId uint64, wt WorkerType, localIp string) *WorkerNode {
	return &WorkerNode{
		WorkerId:   workerId,
		WorkerType: wt,
		WorkerAddr: localIp,
	}
}

func (wn *WorkerNode) ContainTaskByTaskId(task *Task) bool {
	wn.TaskLock.RLock()
	defer wn.TaskLock.RUnlock()
	for _, t := range wn.Tasks {
		if t.TaskId == task.TaskId {
			return true
		}
	}
	return false
}

func (wn *WorkerNode) ContainTaskByDataPartition(task *Task) (bool, *Task) {
	wn.TaskLock.RLock()
	defer wn.TaskLock.RUnlock()
	for _, t := range wn.Tasks {
		if t.Cluster == task.Cluster && t.VolName == task.VolName && t.DpId == task.DpId {
			return true, t
		}
	}
	return false, nil
}

func (wn *WorkerNode) ContainTaskByMetaPartition(task *Task) (bool, *Task) {
	wn.TaskLock.RLock()
	defer wn.TaskLock.RUnlock()
	for _, t := range wn.Tasks {
		if t.Cluster == task.Cluster && t.VolName == task.VolName && t.MpId == task.MpId {
			return true, t
		}
	}
	return false, nil
}

func (wn *WorkerNode) AddTasks(tasks []*Task) {
	wn.TaskLock.Lock()
	defer wn.TaskLock.Unlock()
	wn.Tasks = append(wn.Tasks, tasks...)
}

func (wn *WorkerNode) AddTask(task *Task) {
	wn.TaskLock.Lock()
	defer wn.TaskLock.Unlock()
	wn.Tasks = append(wn.Tasks, task)
}

func (wn *WorkerNode) RemoveTask(task *Task) {
	wn.TaskLock.Lock()
	defer wn.TaskLock.Unlock()
	for index, t := range wn.Tasks {
		if index == 0 && t.TaskId == task.TaskId {
			wn.Tasks = wn.Tasks[1:]
			log.LogDebugf("[RemoveTask] remove task from worker node, workerType(%v), workerId(%v), localIP(%v), taskId(%v), cluster(%v), volName(%v), dpId(%v), mpId(%v)",
				WorkerTypeToName(wn.WorkerType), wn.WorkerId, wn.WorkerAddr, task.TaskId, task.Cluster, task.VolName, task.DpId, task.MpId)
			break
		}
		if index == len(wn.Tasks)-1 && t.TaskId == task.TaskId {
			wn.Tasks = wn.Tasks[:len(wn.Tasks)-1]
			log.LogDebugf("[RemoveTask] remove task from worker node, workerType(%v), workerId(%v), localIP(%v), taskId(%v), cluster(%v), volName(%v), dpId(%v), mpId(%v)",
				WorkerTypeToName(wn.WorkerType), wn.WorkerId, wn.WorkerAddr, task.TaskId, task.Cluster, task.VolName, task.DpId, task.MpId)
			break
		}
		if t.TaskId == task.TaskId {
			wn.Tasks = append(wn.Tasks[0:index], wn.Tasks[index+1:]...)
			log.LogDebugf("[RemoveTask] remove task from worker node, workerType(%v), workerId(%v), localIP(%v), taskId(%v), cluster(%v), volName(%v), dpId(%v), mpId(%v)",
				WorkerTypeToName(wn.WorkerType), wn.WorkerId, wn.WorkerAddr, task.TaskId, task.Cluster, task.VolName, task.DpId, task.MpId)
			break
		}
	}
}

type FlowControl struct {
	WorkerType WorkerType
	FlowType   string
	FlowValue  string
	MaxNums    int64
	CreateTime time.Time
	UpdateTime time.Time
}

func NewFlowControl(wt WorkerType, flowType, flowValue string, maxNum int64) (flow *FlowControl, err error) {
	if _, ok := workerTypeMap[wt]; !ok {
		return nil, errors.New("invalid worker type")
	}
	flow = &FlowControl{
		WorkerType: wt,
		FlowType:   flowType,
		FlowValue:  flowValue,
		MaxNums:    maxNum,
	}
	return
}

type ScheduleConfigType uint8

const (
	ScheduleConfigTypeReserved ScheduleConfigType = iota
	ScheduleConfigTypeMigrateThreshold
	ScheduleConfigTypeMigrateThresholdString = "migrateThreshold"
)

const (
	ScheduleConfigMigrateThresholdGlobalKey = "*"
)

type ScheduleConfig struct {
	ConfigId    uint64
	ConfigType  ScheduleConfigType
	ConfigKey   string
	ConfigValue string
	CreateTime  time.Time
	UpdateTime  time.Time
}

func NewScheduleConfig(ct ScheduleConfigType, ck, cv string) *ScheduleConfig {
	return &ScheduleConfig{
		ConfigType:  ct,
		ConfigKey:   ck,
		ConfigValue: cv,
	}
}

func (sc ScheduleConfig) Key() string {
	return fmt.Sprintf("%v_%v", sc.ConfigType, sc.ConfigKey)
}
