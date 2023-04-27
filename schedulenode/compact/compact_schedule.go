package compact

import (
	"fmt"
	"sort"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// NewCompactWorkerForScheduler scheduleNode进程调用此方法
func NewCompactWorkerForScheduler(cfg *config.Config) (cw *CompactWorker, err error) {
	cw = &CompactWorker{}
	if err = cw.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewCompactWorkerForScheduler] parse config info failed, error(%v)", err)
		return
	}
	if err = cw.initWorkerForScheduler(); err != nil {
		log.LogErrorf("[NewCompactWorkerForScheduler] init compact worker failed, error(%v)", err)
		return
	}
	go cw.loadCompactVolume()
	return
}

// initWorkerForScheduler scheduleNode进程调用此方法
func (cw *CompactWorker) initWorkerForScheduler() (err error) {
	cw.WorkerType = proto.WorkerTypeCompact
	cw.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	cw.mcw = make(map[string]*master.MasterClient)
	for cluster, addresses := range cw.masterAddr {
		cw.mcw[cluster] = master.NewMasterClient(addresses, false)
	}

	cw.cvv = make(map[string]*proto.CompactVolumeView)
	cw.volumeTaskPos = make(map[string]uint64, 0)
	cw.volumeTaskCnt = make(map[string]uint64, 0)

	if err = mysql.InitMysqlClient(cw.MysqlConfig); err != nil {
		log.LogErrorf("[initWorker] init mysql client failed, error(%v)", err)
		return
	}
	return
}

// CreateTask scheduleNode进程调用此方法进行创建任务
func (cw *CompactWorker) CreateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	metric := exporter.NewModuleTP(proto.MonitorCompactCreateTask)
	defer metric.Set(err)

	if len(cw.cvv) == 0 {
		log.LogInfof("CompactVolumeWorker CreateTask current compact volume view is empty")
		return
	}
	cw.RLock()
	defer cw.RUnlock()
	if cv, ok := cw.cvv[clusterId]; ok {
		var compactOpenVols []*proto.CompactVolume
		var compactCloseVols []*proto.CompactVolume
		for _, vol := range cv.CompactVolumes {
			log.LogDebugf("CreateTaskA cw.cvv: cluster(%v) volName(%v) ForceROW(%v) CompactTag(%v)", clusterId, vol.Name, vol.ForceROW, vol.CompactTag)
			compact := vol.CompactTag.String()
			switch compact {
			case proto.CompactOpenName:
				compactOpenVols = append(compactOpenVols, vol)
			case proto.CompactCloseName:
				compactCloseVols = append(compactCloseVols, vol)
			default:
			}
		}
		cw.CancelMpTask(clusterId, compactCloseVols, wns)
		newTasks, err = cw.CreateMpTask(clusterId, compactOpenVols, taskNum, runningTasks)
	}
	return
}

func (cw *CompactWorker) CreateMpTask(cluster string, vols []*proto.CompactVolume, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	sort.Slice(vols, func(i, j int) bool {
		key1 := clusterVolumeKey(cluster, vols[i].Name)
		key2 := clusterVolumeKey(cluster, vols[j].Name)
		return cw.volumeTaskCnt[key1] < cw.volumeTaskCnt[key2]
	})
	cw.debugMsg(cluster, vols)
	for _, vol := range vols {
		if !vol.ForceROW {
			log.LogWarnf("CompactWorker CreateMpTask verify forceROW cluster(%v) volName(%v) ForceROW(%v) CompactTag(%v)", cluster, vol.Name, vol.ForceROW, vol.CompactTag)
			continue
		}
		log.LogDebugf("CompactWorker CreateTask begin clusterId(%v), volumeName(%v), taskNum(%v), runningTasksLength(%v)", cluster, vol.Name, taskNum, len(runningTasks))
		volumeTasks := make(map[string][]*proto.Task)
		for _, task := range runningTasks {
			volumeTasks[task.VolName] = append(volumeTasks[task.VolName], task)
		}
		volumeTaskNum := len(volumeTasks[vol.Name])

		var mpView []*proto.MetaPartitionView
		mpView, err = cw.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("CompactWorker CreateMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}
		sort.Slice(mpView, func(i, j int) bool {
			return mpView[i].PartitionID < mpView[j].PartitionID
		})
		clusterVolumekey := clusterVolumeKey(cluster, vol.Name)
		// resolve volume delete rebuild
		if len(mpView) > 0 && mpView[len(mpView)-1].PartitionID <= cw.volumeTaskPos[clusterVolumekey] {
			cw.volumeTaskPos[clusterVolumekey] = 0
		}
		for i, mp := range mpView {
			if mp.PartitionID <= cw.volumeTaskPos[clusterVolumekey] {
				continue
			}
			if volumeTaskNum >= DefaultVolumeMaxCompactingMPNums {
				break
			}
			if mp.InodeCount == 0 {
				if i >= len(mpView)-1 {
					cw.volumeTaskPos[clusterVolumekey] = 0
				} else {
					cw.volumeTaskPos[clusterVolumekey] = mp.PartitionID
				}
				continue
			}
			newTask := proto.NewDataTask(proto.WorkerTypeCompact, cluster, vol.Name, 0, mp.PartitionID, vol.CompactTag.String())
			var exist bool
			var oldTask *proto.Task
			if exist, oldTask, err = cw.ContainMPTask(newTask, runningTasks); err != nil {
				log.LogErrorf("CompactWorker CreateMpTask ContainMPTask failed, cluster(%v), volume(%v), err(%v)",
					cluster, vol.Name, err)
				continue
			}
			if exist && oldTask.Status != proto.TaskStatusSucceed {
				continue
			}
			var taskId uint64
			if taskId, err = cw.AddTask(newTask); err != nil {
				log.LogErrorf("CompactWorker CreateMpTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
					cluster, vol.Name, newTask, err)
				continue
			}
			cw.volumeTaskCnt[clusterVolumekey] = cw.volumeTaskCnt[clusterVolumekey] + 1
			if i >= len(mpView)-1 {
				cw.volumeTaskPos[clusterVolumekey] = 0
			} else {
				cw.volumeTaskPos[clusterVolumekey] = mp.PartitionID
			}
			newTask.TaskId = taskId
			volumeTaskNum++
			newTasks = append(newTasks, newTask)
			if int64(len(newTasks)) >= taskNum {
				return
			}
		}
	}
	return
}

func (cw *CompactWorker) debugMsg(cluster string, vols []*proto.CompactVolume) {
	var msg = ""
	for _, vol := range vols {
		volumeKey := clusterVolumeKey(cluster, vol.Name)
		msg += fmt.Sprintf("%v:%v:%v ", vol.Name, cw.volumeTaskCnt[volumeKey], cw.volumeTaskPos[volumeKey])
	}
	log.LogDebugf("order of vols: %v", msg)
}

func (cw *CompactWorker) CancelMpTask(cluster string, vols []*proto.CompactVolume, wns []*proto.WorkerNode) {
	for _, vol := range vols {
		log.LogDebugf("CompactWorker CancelMpTask clusterId(%v), vol(%v) wnsLength(%v)", cluster, vol.Name, len(wns))
		mpView, err := cw.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("CompactWorker CancelMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}
		workerAddrMap := make(map[string]struct{}, 0)
		for _, mp := range mpView {
			newTask := proto.NewDataTask(proto.WorkerTypeCompact, cluster, vol.Name, 0, mp.PartitionID, vol.CompactTag.String())
			var exist bool
			var oldTask *proto.Task
			if exist, oldTask, err = cw.ContainMPTaskByWorkerNodes(newTask, wns); err != nil {
				log.LogErrorf("CompactWorker CancelMpTask ContainMPTaskByWorkerNodes failed, cluster(%v), volume(%v), err(%v)",
					cluster, vol.Name, err)
				continue
			}
			if !exist {
				continue
			}
			log.LogDebugf("CancelMpTask oldTask WorkerAddr(%v), task(%v)", oldTask.WorkerAddr, oldTask)
			if oldTask.WorkerAddr != "" {
				workerAddrMap[oldTask.WorkerAddr] = struct{}{}
			}

			if err = cw.UpdateTaskInfo(oldTask.TaskId, vol.CompactTag.String()); err != nil {
				log.LogErrorf("CompactWorker CancelMpTask UpdateTaskInfo to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
					cluster, vol.Name, newTask, err)
			}
		}
		// call worker node stop compact task
		for addr := range workerAddrMap {
			var res *proto.QueryHTTPResult
			stopCompactUrl := genStopCompactUrl(addr, cluster, vol.Name)
			for i := 0; i < RetryDoGetMaxCnt; i++ {
				if res, err = doGet(stopCompactUrl); err != nil {
					log.LogErrorf("CompactWorker CancelMpTask doGet failed, cluster(%v), volume(%v), stopCompactUrl(%v), err(%v)",
						cluster, vol.Name, stopCompactUrl, err)
					continue
				}
				break
			}
			log.LogDebugf("CompactWorker CancelMpTask doGet stopCompactUrl(%v), result(%v)", stopCompactUrl, res)
		}
	}
}

func (cw *CompactWorker) GetCreatorDuration() int {
	return cw.WorkerConfig.TaskCreatePeriod
}

func (cw *CompactWorker) updateTaskInfo(newTaskInfo string, oldTask *proto.Task) (err error) {
	if oldTask.TaskInfo != newTaskInfo {
		if err = cw.UpdateTaskInfo(oldTask.TaskId, newTaskInfo); err != nil {
			return
		}
	}
	oldTask.TaskInfo = newTaskInfo
	return
}

// loadCompactVolume scheduleNode进程调用此方法
func (cw *CompactWorker) loadCompactVolume() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[loadCompactVolume] compact volume loader is running, workerType(%v), localIp(%v)", cw.WorkerType, cw.LocalIp)
		select {
		case <-timer.C:
			loader := func() (err error) {
				metrics := exporter.NewModuleTP(proto.MonitorCompactLoadCompactVolume)
				defer metrics.Set(err)

				for cluster, mc := range cw.mcw {
					var cmpVolView *proto.CompactVolumeView
					cmpVolView, err = GetCompactVolumes(cluster, mc)
					if err != nil {
						log.LogErrorf("[loadCompactVolume] get cluster compact volumes failed, cluster(%v), err(%v)", cluster, err)
						continue
					}
					if cmpVolView == nil || len(cmpVolView.CompactVolumes) == 0 {
						log.LogWarnf("[loadCompactVolume] got all compact volumes is empty, cluster(%v)", cluster)
						continue
					}
					cw.Lock()
					cw.cvv[cluster] = cmpVolView
					cw.Unlock()
				}
				return
			}
			if err := loader(); err != nil {
				log.LogErrorf("[loadCompactVolume] compact volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultCompactVolumeLoadDuration)
		case <-cw.StopC:
			timer.Stop()
			return
		}
	}
}

func GetCompactVolumes(cluster string, mc *master.MasterClient) (cvv *proto.CompactVolumeView, err error) {
	var volumes []*proto.CompactVolume
	volumes, err = mc.AdminAPI().ListCompactVolumes()
	if err != nil {
		log.LogErrorf("[GetCompactVolumes] cluster(%v) masterAddr(%v) err(%v)", cluster, mc.Nodes(), err)
		return
	}
	if len(volumes) == 0 {
		return
	}
	cvv = proto.NewCompactVolumeView(cluster, volumes)
	return
}

func (cw *CompactWorker) GetMpView(cluster string, volName string) (mpView []*proto.MetaPartitionView, err error) {
	mc := cw.mcw[cluster]
	if mc == nil {
		return nil, fmt.Errorf("GetMpView cluster(%v) does not exist", cluster)
	}
	mpView, err = mc.ClientAPI().GetMetaPartitions(volName)
	if err != nil {
		return
	}
	return
}

func clusterVolumeKey(cluster, volume string) string {
	return fmt.Sprintf("%s,%s", cluster, volume)
}
