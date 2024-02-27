package fsck

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

type FSCheckTaskSchedule struct {
	sync.RWMutex
	worker.BaseWorker
	port          string
	masterAddr    map[string][]string
	mcw           map[string]*master.MasterClient
	mcwRWMutex    sync.RWMutex
}

func NewFSCheckTaskSchedule(cfg *config.Config) (fsckTaskSchedule *FSCheckTaskSchedule, err error) {
	fsckTaskSchedule = &FSCheckTaskSchedule{}
	if err = fsckTaskSchedule.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewFSCheckTaskSchedule] parse config info failed, error(%v)", err)
		return
	}
	if err = fsckTaskSchedule.initFSCheckTaskScheduler(); err != nil {
		log.LogErrorf("[NewFSCheckTaskSchedule] init compact worker failed, error(%v)", err)
		return
	}
	return
}

func (fsckTaskSchedule *FSCheckTaskSchedule) parseConfig(cfg *config.Config) (err error) {
	err = fsckTaskSchedule.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	// parse cluster master address
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		masters[clusterName] = addresses
	}
	fsckTaskSchedule.masterAddr = masters
	fsckTaskSchedule.port = fsckTaskSchedule.Port
	return
}

func (fsckTaskSchedule *FSCheckTaskSchedule) initFSCheckTaskScheduler() (err error) {
	fsckTaskSchedule.WorkerType = proto.WorkerTypeFSCheck
	fsckTaskSchedule.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	fsckTaskSchedule.mcw = make(map[string]*master.MasterClient)
	for cluster, addresses := range fsckTaskSchedule.masterAddr {
		fsckTaskSchedule.mcw[cluster] = master.NewMasterClient(addresses, false)
	}

	if err = mysql.InitMysqlClient(fsckTaskSchedule.MysqlConfig); err != nil {
		log.LogErrorf("[initFSCheckTaskScheduler] init mysql client failed, error(%v)", err)
		return
	}
	return
}

func (fsckTaskSchedule *FSCheckTaskSchedule) GetCreatorDuration() int {
	return fsckTaskSchedule.WorkerConfig.TaskCreatePeriod
}

func (fsckTaskSchedule *FSCheckTaskSchedule) CreateTask(clusterID string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	fsckTaskSchedule.RLock()
	defer fsckTaskSchedule.RUnlock()

	_, ok := fsckTaskSchedule.mcw[clusterID]
	if !ok {
		log.LogInfof("FSCheckTaskSchedule CreateTask:cluster %s not exist", clusterID)
		return
	}
	masterClient := fsckTaskSchedule.mcw[clusterID]

	var checkRules []*proto.CheckRule
	checkRules, err = mysql.SelectCheckRule(int(fsckTaskSchedule.WorkerType), clusterID)
	if err != nil {
		return
	}
	ruleMap := make(map[string]string, len(checkRules))
	for _, rule := range checkRules {
		ruleMap[rule.RuleType] = rule.RuleValue
	}

	var needCheckVols []string
	checkAll, checkVolumes, skipVolumes := common.ParseCheckAllRules(ruleMap)
	if checkAll {
		var vols []*proto.VolInfo
		vols, err = masterClient.AdminAPI().ListVols("")
		if err != nil {
			return
		}
		for _, vol := range vols {
			if _, ok = skipVolumes[vol.Name]; ok {
				continue
			}
			needCheckVols = append(needCheckVols, vol.Name)
		}
	} else {
		for volName := range checkVolumes {
			needCheckVols = append(needCheckVols, volName)
		}
	}

	specialOwnerRule := common.ParseSpecialOwnerRules(ruleMap)

	for _, volName := range needCheckVols {
		newTask := proto.NewDataTask(proto.WorkerTypeFSCheck, clusterID, volName, 0, 0, "")
		if alreadyExist, _, _ := fsckTaskSchedule.ContainTask(newTask, runningTasks); alreadyExist {
			log.LogDebugf("fsckTaskSchedule CreateTask %s %s fsck task already exist", clusterID, volName)
			continue
		}

		//get interval
		var (
			checkInterval = DefaultCheckInterval
			volInfo *proto.SimpleVolView
		)

		if volInfo, err = masterClient.AdminAPI().GetVolumeSimpleInfo(volName); err != nil {
			continue
		}
		if specialRule, has := specialOwnerRule[volInfo.Owner]; has {
			checkInterval = time.Duration(specialRule.CheckIntervalMin) * time.Minute
		}

		latestFinishedTime := fsckTaskSchedule.GetLatestFinishedTime(newTask)
		if time.Since(latestFinishedTime) < checkInterval {
			continue
		}

		log.LogDebugf("FSCheckTaskSchedule CreateTask start add task to database, cluster(%v), volume(%v)", clusterID, volName)
		var taskId uint64
		if taskId, err = fsckTaskSchedule.AddTask(newTask); err != nil {
			log.LogErrorf("FSCheckTaskSchedule CreateTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				clusterID, volName, newTask, err)
			continue
		}

		newTask.TaskId = taskId
		newTasks = append(newTasks, newTask)
	}
	return
}
