package worker

import (
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	stringutil "github.com/cubefs/cubefs/util/string"
	"github.com/cubefs/cubefs/util/ump"
	"regexp"
	"time"
)

type Worker interface {
	CreateTask(cluster string, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error)
	ConsumeTask(task *proto.Task) (restore bool, err error)
	GetCreatorDuration() int
}

const (
	DefaultCreateDuration     = 60 // 默认生成任务执行周期, unit: second //TODO, 测试值，以后要调整
	DefaultTaskChanLength     = 1000
	DefaultLoadTaskPeriod     = 60 // 60秒load一次任务到worker
	DefaultQuerySize          = 100
	DefaultTaskSleepNextRetry = 720
)

type BaseWorker struct {
	WorkerType    proto.WorkerType
	WorkerId      uint64
	WorkerAddr    string
	LocalIp       string
	Port          string
	TaskChan      chan *proto.Task
	WorkerConfig  *config.WorkerConfig
	MysqlConfig   *config.MysqlConfig
	StopC         chan struct{}
	Control       common.Control
	NodeException bool // the flag that indicate current node is whether has exception, it can't pull new tasks and consume tasks if true
}

func (b *BaseWorker) Sync() {
	b.Control.Sync()
}

func (b *BaseWorker) ParseBaseConfig(cfg *config.Config) (err error) {
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
	b.Port = port
	b.LocalIp = localIP
	b.WorkerAddr = fmt.Sprintf("%s:%s", b.LocalIp, b.Port)

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
	if stringutil.IsStrEmpty(mysqlDatabase) {
		return fmt.Errorf("error: no mysql database")
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
	b.MysqlConfig = config.NewMysqlConfig(mysqlUrl, mysqlUserName, mysqlPassword, mysqlDatabase, int(mysqlPort), maxIdleConns, maxOpenConns)

	// worker node heartbeat config
	whb := cfg.GetInt(config.ConfigKeyWorkerHeartbeat)
	wp := cfg.GetInt(config.ConfigKeyWorkerPeriod)
	wtp := cfg.GetInt(config.ConfigKeyWorkerTaskPeriod)
	if whb <= 0 {
		whb = proto.DefaultWorkerHeartbeat
	}
	if wp <= 0 {
		wp = proto.DefaultWorkerPeriod
	}
	if wtp <= 0 {
		wtp = DefaultCreateDuration
	}
	b.WorkerConfig = config.NewWorkerConfig(int(whb), int(wp), int(wtp))
	return
}

// register current node to database, and start managers
// 1. load tasks allocated current node
// 2. start worker node heartbeat with database
// 3. start worker node task manager
// 4. start worker node task consumer manager
func (b *BaseWorker) RegisterWorker(wt proto.WorkerType, consumeTask func(task *proto.Task) (bool, error)) (err error) {
	log.LogDebugf("[RegisterWorker] workerAddr(%v), wt(%v)", b.WorkerAddr, proto.WorkerTypeToName(wt))
	b.WorkerType = wt
	if err = b.heartbeat(wt); err != nil {
		log.LogErrorf("[registerWorker] register worker to database failed, workerAddr(%v), workerType(%v), err(%v)",
			b.WorkerAddr, proto.WorkerTypeToName(b.WorkerType), err)
		return
	}
	go b.sendWorkerHeartbeat()
	go b.consumeTaskBase(consumeTask)
	go b.loadWorkerTasks(wt)
	b.loadNotFinishedTasks(wt)
	return
}

// update worker node heartbeat regularly
// Worker node have the function of self degradation.
// It will stop pulling task and consuming tasks if failed heartbeat reached the threshold
func (b *BaseWorker) sendWorkerHeartbeat() {
	timer := time.NewTimer(time.Duration(b.WorkerConfig.WorkerHeartbeat) * time.Second)
	counter := 0
	for {
		log.LogDebugf("[SendWorkerHeartbeat] worker node heartbeat sender is running, workerId(%v), workerType(%v), workerAddr(%v)",
			b.WorkerId, proto.WorkerTypeToName(b.WorkerType), b.WorkerAddr)
		select {
		case <-timer.C:
			heartbeat := func() {
				metrics := exporter.NewTPCnt(proto.MonitorWorkerHeartbeat)
				defer metrics.Set(nil)

				if err := b.heartbeat(b.WorkerType); err != nil {
					log.LogErrorf("[SendWorkerHeartbeat] update worker heartbeat failed, workerId(%v), workerType(%v), workerAddr(%v), err(%v)",
						b.WorkerId, proto.WorkerTypeToName(b.WorkerType), b.WorkerAddr, err)
					counter++
				} else {
					counter = 0
					b.NodeException = false
				}
				if counter > b.WorkerConfig.WorkerPeriod {
					log.LogErrorf("[SendWorkerHeartbeat] update worker heartbeat failed times more then worker period, workerId(%v), workerType(%v), workerAddr(%v), workerPeriod(%v), counter(%v)",
						b.WorkerId, proto.WorkerTypeToName(b.WorkerType), b.WorkerAddr, b.WorkerConfig.WorkerPeriod, counter)
					b.NodeException = true
				}
			}
			heartbeat()
			timer.Reset(time.Duration(b.WorkerConfig.WorkerHeartbeat) * time.Second)
		case <-b.StopC:
			log.LogInfof("[SendWorkerHeartbeat] stop worker heartbeat via stop signal, workerId(%v), workerType(%v), workerAddr(%v)",
				b.WorkerId, proto.WorkerTypeToName(b.WorkerType), b.WorkerAddr)
			timer.Stop()
			return
		}
	}
}

func (b *BaseWorker) heartbeat(wt proto.WorkerType) (err error) {
	var wn *proto.WorkerNode
	var workerId uint64
	if wn, err = mysql.SelectWorkerNode(wt, b.WorkerAddr); err != nil {
		log.LogErrorf("[heartbeat] select worker node failed, workerAddr(%v), workerType(%v), err(%v)",
			b.WorkerAddr, proto.WorkerTypeToName(wt), err.Error())
		return
	}
	if wn != nil {
		if err = mysql.UpdateWorkerHeartbeat(wn); err != nil {
			log.LogErrorf("[heartbeat] update worker node heartbeat, workerAddr(%v), workerType(%v), err(%v)",
				b.WorkerAddr, proto.WorkerTypeToName(wt), err.Error())
			return
		}
		workerId = wn.WorkerId
	} else {
		wn = &proto.WorkerNode{
			WorkerType: wt,
			WorkerAddr: b.WorkerAddr,
		}
		if workerId, err = mysql.AddWorker(wn); err != nil {
			log.LogErrorf("[heartbeat] add worker node to database failed, workerAddr(%v), workerType(%v), err(%v)",
				b.WorkerAddr, proto.WorkerTypeToName(b.WorkerType), err)
			return
		}
	}
	b.WorkerId = workerId
	return
}

// Load not finished tasks when worker start to continue previous tasks.
// The purpose is to fix questions when worker node restart, previous tasks can be executed go on
func (b *BaseWorker) loadNotFinishedTasks(wt proto.WorkerType) {
	var offset = 0
	for {
		tasks, err := mysql.SelectNotFinishedTask(b.WorkerAddr, int(wt), DefaultQuerySize, offset)
		if err != nil {
			log.LogErrorf("[loadNotFinishedTasks] load tasks failed, workerType(%v), workerAddr(%v), err(%v)", proto.WorkerTypeToName(wt), b.WorkerAddr, err)
			break
		}
		if len(tasks) == 0 {
			break
		}
		log.LogDebugf("[loadNotFinishedTasks] select not finished tasks, workerType(%v), taskIds(%v)", proto.WorkerTypeToName(wt), mysql.JoinTaskIds(tasks))
		b.StoreTask(tasks)
		offset += len(tasks)
	}
}

// scheduler for loading tasks allocated current node
func (b *BaseWorker) loadWorkerTasks(wt proto.WorkerType) {
	timer := time.NewTimer(time.Second * DefaultLoadTaskPeriod)
	for {
		log.LogDebugf("[loadWorkerTasks] load tasks dispatched current worker node, workerId(%v), workerType(%v), workerAddr(%v)",
			b.WorkerId, proto.WorkerTypeToName(wt), b.WorkerAddr)
		select {
		case <-timer.C:
			loadTask := func() {
				if b.NodeException {
					log.LogInfof("[loadWorkerTasks] current node is abnormal, workerAddr(%v), workerType(%v)", b.WorkerAddr, proto.WorkerTypeToName(b.WorkerType))
					return
				}
				var offset = 0
				for {
					tasks, err := mysql.SelectAllocatedTask(b.WorkerAddr, int(wt), DefaultQuerySize, offset)
					if err != nil {
						log.LogErrorf("[loadWorkerTasks] load tasks failed, workerType(%v), workerAddr(%v), err(%v)", proto.WorkerTypeToName(wt), b.WorkerAddr, err)
						break
					}
					if len(tasks) == 0 {
						break
					}
					b.StoreTask(tasks)
					offset += len(tasks)
				}
			}
			loadTask()
			timer.Reset(time.Second * DefaultLoadTaskPeriod)
		case <-b.StopC:
			timer.Stop()
			return
		}
	}
}

func (b *BaseWorker) AddTask(task *proto.Task) (taskId uint64, err error) {
	taskId, err = mysql.AddTask(task)
	if err != nil {
		log.LogErrorf("[AddTask] add task to failed, err(%v), task(%v)", err, task.String())
		alarmKey := fmt.Sprintf("Add %s task to database failed", proto.WorkerTypeToName(task.TaskType))
		alarmInfo := fmt.Sprintf("Add %s task to database failed, errInfo: %s", proto.WorkerTypeToName(task.TaskType), err.Error())
		ump.Alarm(alarmKey, alarmInfo)
		return
	}
	return
}

func (b *BaseWorker) UpdateTaskInfo(taskId uint64, taskInfo string) (err error) {
	err = mysql.UpdateTaskInfo(taskId, taskInfo)
	if err != nil {
		log.LogErrorf("[UpdateTaskInfo] update task info failed, taskId(%v), taskInfo(%v), err(%v)", taskId, taskInfo, err)
		return
	}
	return
}

func (b *BaseWorker) UpdateTaskProcessing(taskId uint64, taskType proto.WorkerType) (err error) {
	task := &proto.Task{
		TaskId:   taskId,
		TaskType: taskType,
	}
	err = mysql.UpdateTaskStatus(task)
	if err != nil {
		log.LogErrorf("[UpdateTaskUpdateTime] update task status to be processing failed, taskId(%v), taskType(%v) err(%v)",
			taskId, proto.WorkerTypeToName(taskType), err)
		return
	}
	return
}

func (b *BaseWorker) ContainDPTask(task *proto.Task, runningTasks []*proto.Task) (exist bool, tf *proto.Task, err error) {
	for _, t := range runningTasks {
		if t.Cluster == task.Cluster && t.VolName == task.VolName && t.DpId == task.DpId {
			return true, t, nil
		}
	}
	return mysql.CheckDPTaskExist(task.Cluster, task.VolName, int(task.TaskType), task.DpId)
}

func (b *BaseWorker) ContainMPTask(task *proto.Task, runningTasks []*proto.Task) (exist bool, tf *proto.Task, err error) {
	for _, t := range runningTasks {
		if t.Cluster == task.Cluster && t.VolName == task.VolName && t.MpId == task.MpId {
			return true, t, nil
		}
	}
	return mysql.CheckMPTaskExist(task.Cluster, task.VolName, int(task.TaskType), task.MpId)
}

func (b *BaseWorker) ContainMPTaskByWorkerNodes(task *proto.Task, wn []*proto.WorkerNode) (exist bool, tf *proto.Task, err error) {
	for _, workerNode := range wn {
		if exist, tf = workerNode.ContainTaskByMetaPartition(task); exist {
			log.LogDebugf("[CompactWorker CreateTask] meta partition task is running, cluster(%v), volume(%v), dpId(%v), mpId(%v)",
				task.Cluster, task.VolName, task.DpId, task.MpId)
			return
		}
	}
	return mysql.CheckMPTaskExist(task.Cluster, task.VolName, int(task.TaskType), task.MpId)
}

func (b *BaseWorker) consumeTaskBase(consumeFunc func(task *proto.Task) (bool, error)) {
	for {
		log.LogDebugf("[consumeTaskBase] task consumer is running, workerAddr(%v), workerType(%v)",
			b.WorkerAddr, proto.WorkerTypeToName(b.WorkerType))
		if b.NodeException {
			log.LogWarnf("[consumeTaskBase] current node is abnormal, workerAddr(%v), workerType(%v)",
				b.WorkerAddr, proto.WorkerTypeToName(b.WorkerType))
			time.Sleep(time.Second * DefaultLoadTaskPeriod)
			continue
		}
		select {
		case task := <-b.TaskChan:
			go func() {
				restore, err := consumeFunc(task)
				if err != nil {
					alarmKey := fmt.Sprintf("%s worker consume task failed", proto.WorkerTypeToName(task.TaskType))
					alarmInfo := fmt.Sprintf("consume task failed, taskInfo(%v)", task.String())
					log.LogErrorf("[consumeTaskBase] alarmInfo(%v), error(%v)", alarmInfo, err.Error())
					ump.Alarm(alarmKey, alarmInfo)

					errorInfo := err.Error()
					err = mysql.UpdateTaskFailed(task, errorInfo)
					if err != nil {
						log.LogErrorf("[ConsumeTask] update task status to be failed has exception, cluster(%v), volName(%v), dpId(%v), err(%v)", task.Cluster, task.VolName, task.DpId, err)
					}
					if restore {
						b.RestoreTask(task)
					}
					return
				}
				if restore {
					task.Status = proto.TaskStatusProcessing
					err = mysql.UpdateTaskStatus(task)
					if err != nil {
						log.LogErrorf("[ConsumeTask] update task status to processing failed, cluster(%v), volName(%v), dpId(%v), err(%v)", task.Cluster, task.VolName, task.DpId, err)
					}
					b.RestoreTask(task)
					return
				}
				task.Status = proto.TaskStatusSucceed
				err = mysql.UpdateTaskStatus(task)
				if err != nil {
					b.RestoreTask(task)
					log.LogErrorf("[ConsumeTask] update task status to be success failed, cluster(%v), volName(%v), dpId(%v), err(%v)", task.Cluster, task.VolName, task.DpId, err)
					return
				}
			}()
		case <-b.StopC:
			log.LogInfof("[consumeTaskBase] stop consumer task via stop signal")
			return
		}
	}
}

func (b *BaseWorker) StoreTask(tasks []*proto.Task) {
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		b.TaskChan <- task
	}
	// update task status to pending
	err := mysql.UpdateTasksStatus(tasks, proto.TaskStatusAllocated, proto.TaskStatusPending)
	if err != nil {
		log.LogErrorf("[StoreTask] store task to worker failed, err(%v), taskIds(%v)", err, mysql.JoinTaskIds(tasks))
	}
}

func (b *BaseWorker) RestoreTask(task *proto.Task) {
	time.Sleep(time.Second * DefaultTaskSleepNextRetry)
	b.TaskChan <- task
}
