package smart

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/schedulenode/worker"
	"github.com/chubaofs/chubaofs/sdk/hbase"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/mysql"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSmartVolumeLoadDuration      = 60  // unit: second
	DefaultClusterLoadDuration          = 60  // unit: second
	DefaultMigrateUsedThresholdDuration = 120 // unit: second
	DefaultDPMigrateUsedThreshold       = 0.5 // dp can be migrated when used size more then this threshold
	DefaultVolumeMaxMigratingDPNums     = 100 // the max dp nums that are migrating in one volume
	DefaultActionMetricMinuteUnit       = 5   // the max dp nums that are migrating in one volume
)

type SmartVolumeWorker struct {
	worker.BaseWorker
	masterAddr         map[string][]string
	mcw                map[string]*master.MasterClient
	svv                map[string]*proto.SmartVolumeView
	cv                 map[string]*proto.ClusterView // TODO
	HBaseConfig        *config.HBaseConfig
	hBaseClient        *hbase.HBaseClient
	svvLock            sync.RWMutex
	dpMigrateThreshold sync.Map
}

func NewSmartVolumeWorker() *SmartVolumeWorker {
	return &SmartVolumeWorker{}
}

func NewSmartVolumeWorkerForScheduler(cfg *config.Config) (sv *SmartVolumeWorker, err error) {
	sv = &SmartVolumeWorker{}
	if err = sv.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}
	// init smart volume worker
	if err = sv.initWorker(); err != nil {
		log.LogErrorf("[doStart] init smart volume worker failed, error(%v)", err)
		return
	}
	if err = sv.loadMigrateThreshold(); err != nil {
		log.LogErrorf("[doStart] load data partition migrate threshold failed, error(%v)", err)
		return
	}
	// start smart volume load scheduler
	go sv.loadSmartVolume()
	// load data partition migrate threshold
	go sv.migrateThresholdManager()
	return
}

func (sv *SmartVolumeWorker) Start(cfg *config.Config) (err error) {
	return sv.Control.Start(sv, cfg, doStart)
}

func doStart(server common.Server, cfg *config.Config) (err error) {
	sv, ok := server.(*SmartVolumeWorker)
	if !ok {
		return errors.New("invalid node type")
	}
	sv.StopC = make(chan struct{}, 0)

	if err = sv.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	// init ump monitor and alarm module
	exporter.Init(proto.RoleScheduleNode, proto.RoleSmartVolumeWorker, cfg)
	exporter.RegistConsul(cfg)

	// init smart volume worker
	if err = sv.initWorker(); err != nil {
		log.LogErrorf("[doStart] init smart volume worker failed, error(%v)", err)
		return
	}
	if err = sv.RegisterWorker(proto.WorkerTypeSmartVolume, sv.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register smart volume worker failed, error(%v)", err)
		return
	}
	go sv.registerHandler()
	go sv.loadSmartVolume()
	//go sv.loadCluster()
	return
}

func (sv *SmartVolumeWorker) initWorker() (err error) {
	sv.WorkerType = proto.WorkerTypeSmartVolume
	sv.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	// init master client
	masterClient := make(map[string]*master.MasterClient)
	for cluster, addresses := range sv.masterAddr {
		mc := master.NewMasterClient(addresses, false)
		masterClient[cluster] = mc
	}
	sv.mcw = masterClient
	sv.svv = make(map[string]*proto.SmartVolumeView)
	sv.cv = make(map[string]*proto.ClusterView)

	// init mysql client
	if err = mysql.InitMysqlClient(sv.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	// init hBase client
	sv.hBaseClient = hbase.NewHBaseClient(sv.HBaseConfig)
	return
}

func (sv *SmartVolumeWorker) loadSmartVolume() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[loadSmartVolume] smart volume loader is running, workerId(%v), workerAddr(%v)",
			sv.WorkerId, sv.WorkerAddr)
		select {
		case <-timer.C:
			loader := func() (err error) {
				metrics := exporter.NewTPCnt(proto.MonitorSmartLoadSmartVolume)
				defer metrics.Set(err)

				for cluster, mc := range sv.mcw {
					smartVolumeView, err := GetSmartVolumes(cluster, mc)
					if err != nil {
						log.LogErrorf("[loadSmartVolume] get cluster smart volumes failed, cluster(%v), err(%v)", cluster, err)
						continue
					}
					if smartVolumeView == nil || len(smartVolumeView.SmartVolumes) == 0 {
						log.LogWarnf("[loadSmartVolume] got all smart volumes is empty, cluster(%v)", cluster)
						continue
					}
					// parse smart volume layer policy
					for _, vv := range smartVolumeView.SmartVolumes {
						sv.parseLayerPolicy(vv)
					}
					sv.svvLock.Lock()
					sv.svv[cluster] = smartVolumeView
					sv.svvLock.Unlock()
				}
				return
			}
			if err := loader(); err != nil {
				log.LogErrorf("[loadSmartVolume] smart volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultSmartVolumeLoadDuration)
		case <-sv.StopC:
			timer.Stop()
			return
		}
	}
}

func (sv *SmartVolumeWorker) loadMigrateThreshold() (err error) {
	metrics := exporter.NewTPCnt(proto.MonitorSchedulerMigrateThresholdManager)
	defer metrics.Set(err)

	keys := make(map[string]*proto.ScheduleConfig)
	scs, err := mysql.SelectScheduleConfig(proto.ScheduleConfigTypeMigrateThreshold)
	if err != nil {
		log.LogErrorf("[migrateThresholdManager] select volume migrate threshold failed, err(%v)", err)
		return
	}
	for _, sc := range scs {
		key := sc.Key()
		keys[key] = sc
		sv.dpMigrateThreshold.Store(key, sc)
	}
	// clean flow controls in memory before load all new flow controls
	sv.dpMigrateThreshold.Range(func(key, value interface{}) bool {
		k := key.(string)
		if _, ok := keys[k]; !ok {
			sv.dpMigrateThreshold.Delete(key)
		}
		return true
	})
	// global default value
	var globalDefaultExisted bool
	globalKey := fmt.Sprintf("%v_%v", proto.ScheduleConfigTypeMigrateThreshold, proto.ScheduleConfigMigrateThresholdGlobalKey)
	sv.dpMigrateThreshold.Range(func(key, value interface{}) bool {
		k := key.(string)
		if k == globalKey {
			globalDefaultExisted = true
		}
		return true
	})
	if !globalDefaultExisted {
		configValue := strconv.FormatFloat(DefaultDPMigrateUsedThreshold, 'f', 2, 64)
		dsc := proto.NewScheduleConfig(proto.ScheduleConfigTypeMigrateThreshold, proto.ScheduleConfigMigrateThresholdGlobalKey, configValue)
		sv.dpMigrateThreshold.Store(globalKey, dsc)
	}
	return
}

func (sv *SmartVolumeWorker) migrateThresholdManager() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("[migrateThresholdManager] smart volume migrate threshold manager is running...")
		select {
		case <-timer.C:
			if err := sv.loadMigrateThreshold(); err != nil {
				log.LogErrorf("[migrateThresholdManager] flow control manager has exception, err(%v)", err)
				return
			}
			timer.Reset(time.Second * time.Duration(DefaultMigrateUsedThresholdDuration))
		case <-sv.StopC:
			log.LogInfof("[migrateThresholdManager] stop smart volume migrate threshold manager")
			return
		}
	}
}

func (sv *SmartVolumeWorker) getVolumeMigrateThreshold(volName string) (threshold float64) {
	var (
		v   interface{}
		ok  bool
		err error
		sc  *proto.ScheduleConfig
	)

	getThresholdByKey := func(key string) (e error) {
		if v, ok = sv.dpMigrateThreshold.Load(key); ok {
			sc, ok = v.(*proto.ScheduleConfig)
			if !ok {
				return errors.New("invalid value type")
			}
			threshold, err = strconv.ParseFloat(sc.ConfigValue, 64)
			if err != nil {
				return err
			}
			return
		}
		return errors.New("not exist")
	}

	volumeKey := fmt.Sprintf("%v_%v", proto.ScheduleConfigTypeMigrateThreshold, volName)
	if err = getThresholdByKey(volumeKey); err == nil && threshold > 0 {
		return threshold
	}
	log.LogInfof("[getVolumeMigrateThreshold] get volume migrate threshold failed, volName(%v), volumeKey(%v), threshold(%v), err(%v)",
		volName, volumeKey, threshold, err)

	globalKey := fmt.Sprintf("%v_%v", proto.ScheduleConfigTypeMigrateThreshold, proto.ScheduleConfigMigrateThresholdGlobalKey)
	if err = getThresholdByKey(globalKey); err == nil && threshold > 0 {
		return threshold
	}
	log.LogErrorf("[getVolumeMigrateThreshold] get global migrate threshold failed, volName(%v), globalKey(%v), threshold(%v), err(%v)",
		volName, globalKey, threshold, err)
	return DefaultDPMigrateUsedThreshold
}

func (sv *SmartVolumeWorker) loadCluster() {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			log.LogDebugf("[loadSmartVolume] smart volume cluster info loader is running, workerId(%v), workerAddr(%v)",
				sv.WorkerId, sv.WorkerAddr)
			for cluster, mc := range sv.mcw {
				clusterMigrationView, err := mc.AdminAPI().GetCluster()
				if err != nil {
					log.LogErrorf("[loadCluster] get cluster migration view info failed, cluster(%v), err(%v)", cluster, err)
					timer.Reset(time.Second * DefaultClusterLoadDuration)
					continue
				}
				if clusterMigrationView == nil {
					log.LogWarnf("[loadCluster] got cluster migration view is empty, cluster(%v)", cluster)
					timer.Reset(time.Second * DefaultClusterLoadDuration)
					continue
				}
				sv.cv[cluster] = clusterMigrationView
			}
		case <-sv.StopC:
			timer.Stop()
			return
		}
		timer.Reset(time.Second * DefaultClusterLoadDuration)
	}
}

func (sv *SmartVolumeWorker) parseLayerPolicy(volume *proto.SmartVolume) {
	if volume.SmartRules == nil || len(volume.SmartRules) == 0 {
		log.LogErrorf("[parseLayerPolicy] smart volume smart rules is empty, clusterId(%v), volumeName(%v), smartRules(%v)",
			volume.ClusterId, volume.Name, volume.SmartRules)
		return
	}

	var (
		err error
		lt  proto.LayerType
		lp  interface{}
	)
	if err = proto.CheckLayerPolicy(volume.ClusterId, volume.Name, volume.SmartRules); err != nil {
		log.LogErrorf("[parseLayerPolicy] check volume layer policy failed, cluster(%v) ,volumeName(%v), smartRules(%v), err(%v)",
			volume.ClusterId, volume.Name, volume.SmartRules, err)
		return
	}

	volume.LayerPolicies = make(map[proto.LayerType][]interface{})
	for _, originRule := range volume.SmartRules {
		originRule = strings.ReplaceAll(originRule, " ", "")
		lt, lp, err = proto.ParseLayerPolicy(volume.ClusterId, volume.Name, originRule)
		if err != nil {
			log.LogErrorf("[parseLayerPolicy] parse layer type failed, cluster(%v) ,volumeName(%v), originRule(%v), err(%v)",
				volume.ClusterId, volume.Name, originRule, err)
			return
		}
		switch lt {
		case proto.LayerTypeActionMetrics:
			am := lp.(*proto.LayerPolicyActionMetrics)
			volume.LayerPolicies[proto.LayerTypeActionMetrics] = append(volume.LayerPolicies[proto.LayerTypeActionMetrics], am)
		case proto.LayerTypeDPCreateTime:
			dpc := lp.(*proto.LayerPolicyDPCreateTime)
			volume.LayerPolicies[proto.LayerTypeDPCreateTime] = append(volume.LayerPolicies[proto.LayerTypeDPCreateTime], dpc)
		}
	}
}

func (sv *SmartVolumeWorker) CreateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	metric := exporter.NewTPCnt(proto.MonitorSmartCreateTask)
	defer metric.Set(err)

	clusterSmartVolumeView, ok := sv.svv[clusterId]
	if !ok {
		log.LogInfof("[SmartVolumeWorker CreateTask] cluster's smart volume view is empty, cluster(%v)", clusterId)
		return
	}
	log.LogDebugf("[SmartVolumeWorker CreateTask] clusterId(%v), taskNum(%v), runningTasksLength(%v), clusterSmartVolumeLength(%v)",
		clusterId, taskNum, len(runningTasks), len(clusterSmartVolumeView.SmartVolumes))
	res, errInner := sv.hBaseClient.CheckSparkTaskRunning(clusterId)
	if !res || errInner != nil {
		log.LogErrorf("[SmartVolumeWorker CreateTask] spark compute task is not running, cluster(%v), res(%v), err(%v)",
			clusterId, res, errInner)
		return
	}

	volumeTasks := make(map[string][]*proto.Task)
	for _, task := range runningTasks {
		volumeTasks[task.VolName] = append(volumeTasks[task.VolName], task)
	}
	for _, volume := range clusterSmartVolumeView.SmartVolumes {
		log.LogDebugf("[SmartVolumeWorker CreateTask] clusterId(%v), volume(%v), volumeTasksLength(%v)", clusterId, volume.Name, len(volumeTasks[volume.Name]))
		if len(volume.LayerPolicies) == 0 {
			log.LogErrorf("[SmartVolumeWorker CreateTask] have no layer policy, cluster(%v), volume(%v)", clusterId, volume.Name)
			continue
		}

		var result bool
		var running bool
		var volumeTaskNum int
		var writableDPNums int
		var migrateWritableDPNums int
		var usedRate float64
		var volumeMigrateThreshold float64
		for _, partition := range volume.DataPartitions {
			if partition.Status == proto.ReadWrite && !partition.IsFrozen {
				writableDPNums++
			}
		}
		volumeTaskNum = len(volumeTasks[volume.Name])
		volumeMigrateThreshold = sv.getVolumeMigrateThreshold(volume.Name)

		for _, dp := range volume.DataPartitions {
			if volumeTaskNum >= DefaultVolumeMaxMigratingDPNums {
				log.LogInfof("[SmartVolumeWorker CreateTask] volume migrate tasks has reached max task number, cluster(%v), volume(%v), volumeTaskNum(%v)",
					clusterId, volume.Name, volumeTaskNum)
				break
			}
			if dp.IsFrozen {
				log.LogInfof("[SmartVolumeWorker CreateTask] data partition is frozen, cluster(%v), volume(%v), dpId(%v)",
					clusterId, volume.Name, dp.PartitionID)
				continue
			}
			if dp.Used <= 0 || dp.Total <= 0 {
				log.LogInfof("[SmartVolumeWorker CreateTask] data partition used or total size is zero, cluster(%v), volume(%v), dpId(%v), used(%v), total(%v)",
					clusterId, volume.Name, dp.PartitionID, dp.Used, dp.Total)
				continue
			}
			// dp can not be migrated if dp size used rate less then 50%
			usedRate, err = strconv.ParseFloat(fmt.Sprintf("%.5f", float64(dp.Used)/float64(dp.Total)), 10)
			if err != nil {
				log.LogErrorf("[SmartVolumeWorker CreateTask] compute data partition used rate failed, cluster(%v), volume(%v), dpId(%v), used(%v), total(%v)",
					clusterId, volume.Name, dp.PartitionID, dp.Used, dp.Total)
				continue
			}
			if usedRate <= volumeMigrateThreshold {
				log.LogInfof("[SmartVolumeWorker CreateTask] data partition used rate not reach migrate threshold, cluster(%v), volume(%v), dpId(%v), usedRate(%v), used(%v), total(%v), migrateThreshold(%v)",
					clusterId, volume.Name, dp.PartitionID, usedRate, dp.Used, dp.Total, volumeMigrateThreshold)
				continue
			}
			if dp.IsRecover {
				log.LogInfof("[SmartVolumeWorker CreateTask] data partition is recovering, cluster(%v), volume(%v), dpId(%v)",
					clusterId, volume.Name, dp.PartitionID)
				continue
			}
			if writableDPNums > 0 && migrateWritableDPNums >= writableDPNums/2 {
				log.LogInfof("[SmartVolumeWorker CreateTask] migrate writable data partition is more then half of writable data partitions, "+
					"cluster(%v), volume(%v), dpId(%v), writableDPNums(%v), migrateWritableDPNums(%v)", clusterId, volume.Name, dp.PartitionID, writableDPNums, migrateWritableDPNums)
				continue
			}
			// if data partition is writable, check volume's writable nums, if volume's writable data partition is too less, can not continue transferring
			result, err = sv.isDPSufficientMigrated(volume.ClusterId, volume.Name, dp)
			if err != nil {
				log.LogErrorf("[SmartVolumeWorker CreateTask] check writable data partition is sufficient to migrate failed, cluster(%v), volume(%v), dpId(%v), err(%v)",
					clusterId, volume.Name, dp.PartitionID, err)
				continue
			}
			if !result {
				log.LogInfof("[SmartVolumeWorker CreateTask] writable data partition is not sufficient to migrate, cluster(%v), volume(%v), dpId(%v)",
					clusterId, volume.Name, dp.PartitionID)
				continue
			}
			// check data partition task is whether running
			dataTask := proto.NewDataTask(proto.WorkerTypeSmartVolume, clusterId, volume.Name, dp.PartitionID, 0, "")
			if running, _, err = sv.ContainDPTask(dataTask, runningTasks); err != nil {
				log.LogErrorf("[SmartVolumeWorker CreateTask] check task is whether running has exception, cluster(%v), volume(%v), dp(%v)，err(%v)",
					clusterId, volume.Name, dp.PartitionID, err)
				continue
			}
			if running {
				log.LogInfof("[SmartVolumeWorker CreateTask] task is running, cluster(%v), volume(%v), dp(%v)",
					clusterId, volume.Name, dp.PartitionID)
				continue
			}

			var flag bool
			var smartTaskInfo string
			for layerType, policies := range volume.LayerPolicies {
				if flag {
					break
				}
				switch layerType {
				case proto.LayerTypeActionMetrics:
					// all action metrics policies must be met
					var metricsData []*proto.HBaseMetricsData
					if len(policies) <= 0 {
						continue
					}
					amFlag := true
					for _, p := range policies {
						policy, ok := p.(*proto.LayerPolicyActionMetrics)
						if !ok {
							log.LogErrorf("[SmartVolumeWorker CreateTask] got action metrics layer policy is invalid, cluster(%v), volume(%v), dp(%v), policy(%v)",
								clusterId, volume.Name, dp.PartitionID, p)
							amFlag = false
							break
						}
						metricsData, err = sv.getHBaseMetrics(clusterId, volume.Name, volume.SmartEnableTime, dp, policy)
						if err != nil {
							log.LogErrorf("[SmartVolumeWorker CreateTask] get action metrics failed, cluster(%v), volume(%v), dp(%v), policy(%v), err(%v)",
								clusterId, volume.Name, dp.PartitionID, policy.String(), err)
							amFlag = false
							break
						}
						if !policy.CheckDPMigrated(dp, metricsData) {
							log.LogInfof("[SmartVolumeWorker CreateTask] check dp is need to be migrated is false, cluster(%v), volume(%v), dp(%v), policy(%v)",
								clusterId, volume.Name, dp.PartitionID, policy.String())
							amFlag = false
							break
						}
						smartTaskInfo, err = proto.NewActionMetricsTaskInfo(policy, metricsData, dp.Hosts)
						if err != nil {
							log.LogInfof("[SmartVolumeWorker CreateTask] marshall task info failed, cluster(%v), volume(%v), dp(%v), policy(%v), metricsData(%v), err(%v)",
								clusterId, volume.Name, dp.PartitionID, policy.String(), metricsData, err)
							amFlag = false
							break
						}
					}
					if amFlag {
						flag = true
					}
				case proto.LayerTypeDPCreateTime:
					// one dp create time policies meet
					for _, p := range policies {
						policy, ok := p.(*proto.LayerPolicyDPCreateTime)
						if !ok {
							log.LogErrorf("[SmartVolumeWorker CreateTask] got data partition create time layer policy is invalid, cluster(%v), volume(%v), dp(%v), policy(%v)",
								clusterId, volume.Name, dp.PartitionID, p)
							continue
						}
						if !policy.CheckDPMigrated(dp, nil) {
							log.LogInfof("[SmartVolumeWorker CreateTask] check dp is need to be migrated is false, cluster(%v), volume(%v), dp(%v), policy(%v)",
								clusterId, volume.Name, dp.PartitionID, policy.String())
							continue
						}
						smartTaskInfo, err = proto.NewDPCreateTimeTaskInfo(policy, dp.CreateTime, dp.Hosts)
						if err != nil {
							log.LogInfof("[SmartVolumeWorker CreateTask] marshall task info failed, cluster(%v), volume(%v), dp(%v), policy(%v), dpCreateTime(%v), err(%v)",
								clusterId, volume.Name, dp.PartitionID, policy.String(), dp.CreateTime, err)
							continue
						}
						flag = true
						break
					}
				}
			}

			if !flag {
				log.LogWarnf("[SmartVolumeWorker CreateTask] all policies are not satisfied, cluster(%v), volume(%v), dp(%v), flag(%v)",
					clusterId, volume.Name, dp.PartitionID, flag)
				continue
			}
			if _, err = sv.mcw[clusterId].AdminAPI().FreezeDataPartition(volume.Name, dp.PartitionID); err != nil {
				log.LogErrorf("[SmartVolumeWorker CreateTask] frozen data partition failed, cluster(%v), volume(%v), dp(%v), taskInfo(%v), err(%v)",
					clusterId, volume.Name, dp.PartitionID, smartTaskInfo, err)
				continue
			}
			var taskId uint64
			task := proto.NewDataTask(proto.WorkerTypeSmartVolume, clusterId, volume.Name, dp.PartitionID, 0, smartTaskInfo)
			if taskId, err = sv.AddTask(task); err != nil {
				log.LogErrorf("[SmartVolumeWorker CreateTask] add task to database failed, cluster(%v), volume(%v), dp(%v), taskInfo(%v), err(%v)",
					clusterId, volume.Name, dp.PartitionID, smartTaskInfo, err)
				continue
			}

			log.LogInfof("[SmartVolumeWorker CreateTask] add task to database success, cluster(%v), volume(%v), dp(%v), dpStatus(%v) taskInfo(%v), taskId(%v), usedRate(%v), writableDPNums(%v), migrateWritableDPNums(%b)",
				clusterId, volume.Name, dp.PartitionID, dp.Status, smartTaskInfo, taskId, usedRate, writableDPNums, migrateWritableDPNums)

			task.TaskId = taskId
			volumeTaskNum++
			newTasks = append(newTasks, task)
			if dp.Status == proto.ReadWrite {
				migrateWritableDPNums++
			}
			if int64(len(newTasks)) >= taskNum {
				log.LogInfof("[SmartVolumeWorker CreateTask] create tasks has enough, cluster(%v), volume(%v), dp(%v), newTasksLength(%v), taskNum(%v)",
					clusterId, volume.Name, dp.PartitionID, len(newTasks), taskNum)
				return
			}
		}
	}
	return
}

func (sv *SmartVolumeWorker) getHBaseMetrics(clusterId, volName string, smartEnableTime int64, dp *proto.DataPartitionResponse, policy *proto.LayerPolicyActionMetrics) (metricsData []*proto.HBaseMetricsData, err error) {
	var (
		startTime      string
		startTimestamp int64
		stopTime       string
	)
	if startTimestamp, startTime, err = getStartTime(policy.TimeType, policy.LessCount); err != nil {
		log.LogErrorf("[getHBaseMetrics] get start time failed, cluster(%v), volume(%v), dp(%v), policy(%v), err(%v)",
			clusterId, volName, dp.PartitionID, policy.String(), err)
		return nil, err
	}
	// The start time must be later than the creation time of data partition, and must be later than the volume smart rule effective time too.
	// Otherwise the data partition may be transferred for have no request.
	// because if data partition has not been created, the records from its creation time to the start time are empty, and it will be considered that there is no request
	if startTimestamp < smartEnableTime {
		log.LogInfof("[getHBaseMetrics] query start time early then volume smart rule effective time, cluster(%v), volume(%v), dp(%v), policy(%v), startTime(%v), smartEnableTime(%v)",
			clusterId, volName, dp.PartitionID, policy.String(), util.FormatTimestamp(startTimestamp), util.FormatTimestamp(smartEnableTime))
		return nil, errors.New("query start time early then volume smart rule effective time")
	}
	if startTimestamp < dp.CreateTime {
		log.LogInfof("[getHBaseMetrics] query start time early then create time, cluster(%v), volume(%v), dp(%v), policy(%v), startTime(%v), dpCreateTime(%v)",
			clusterId, volName, dp.PartitionID, policy.String(), util.FormatTimestamp(startTimestamp), util.FormatTimestamp(dp.CreateTime))
		return nil, errors.New("query start time early then create time")
	}
	if stopTime, err = getStopTime(policy.TimeType); err != nil {
		log.LogErrorf("[getHBaseMetrics] get stop time failed, cluster(%v), volume(%v), dp(%v), policy(%v), err(%v)",
			clusterId, volName, dp.PartitionID, policy.String(), err)
		return nil, err
	}
	request := proto.NewHBaseDPRequest(policy.ClusterId, policy.VolumeName, dp.PartitionID, policy.TimeType, policy.Action, startTime, stopTime, policy.LessCount)
	//var metricsData []*proto.HBaseMetricsData
	metricsData, err = sv.hBaseClient.SelectDataPartitionMetrics(request)
	if err != nil {
		log.LogErrorf("[getHBaseMetrics] select data partition count info failed, cluster(%v), volume(%v), dp(%v), policy(%v), err(%v)",
			clusterId, volName, dp.PartitionID, policy.String(), err)
		return nil, err
	}
	return
}

// consume single task
// if err is not empty, it candidate current task was failed, and will not retry it.
// if err is empty but restore is true, it candidate this task does not meet the processing conditions
// and task will be restored to queue to consume again
func (sv *SmartVolumeWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	var (
		isFinished   bool
		sourceHosts  []string
		targetMedium proto.MediumType
		lt           proto.LayerType
		dp           *proto.DataPartitionInfo
		taskInfo     interface{}
		dpHostsMap   = make(map[string]*proto.DataReplica)
		migrateHosts = make(map[string]string)
		sortHosts    []string
	)
	metric := exporter.NewTPCnt(proto.MonitorSmartConsumeTask)
	defer metric.Set(err)

	sourceHosts, targetMedium, lt, taskInfo, err = sv.unmarshallTaskInfo(task.TaskInfo)
	if err != nil {
		log.LogErrorf("[SmartVolumeWorker ConsumeTask], unmarshall task info failed, taskInfo(%v)", task.String())
		return
	}
	for _, host := range sourceHosts {
		migrateHosts[host] = host
	}
	dp, err = sv.mcw[task.Cluster].AdminAPI().GetDataPartition(task.VolName, task.DpId)
	// if volume been deleted, get data partition get "data partition not exists" exception
	// ErrDataPartitionNotExists = errors.New("data partition not exists")
	if err != nil && strings.Contains(err.Error(), "data partition not exists") {
		log.LogInfof("[ConsumeTask] data partition or volume may be deleted, mask task succeed, cluster(%v), volume(%v), dpId(%v), "+
			"taskInfo(%v), err(%v)", task.Cluster, task.VolName, task.DpId, task.String(), err)
		return false, nil
	}
	if err != nil {
		log.LogErrorf("[ConsumeTask] get data partition failed, taskInfo(%v), err(%v)", task.String(), err)
		return
	}
	if len(dp.Replicas) < int(dp.ReplicaNum) {
		errorInfo := fmt.Sprintf("dp current replicas is less then its replica num, replicaLength(%v), replicaNum(%v)\n", len(dp.Replicas), dp.ReplicaNum)
		log.LogErrorf("[ConsumeTask] %s, taskInfo(%v), err(%v)", errorInfo, task.String(), err)
		return true, errors.New(errorInfo)
	}

	// restore task to queue if data partition is repairing,
	// data partition is repairing, it indicates one replication is transferring, continue to retry the task after this replication finished transferring
	if dp.IsRecover {
		log.LogInfof("[ConsumeTask] data partition is recovering, can not add new migrate task, taskInfo(%v), err(%v)", task.String(), err)
		return true, nil
	}

	// it indicates task is finished successfully, if data partition is not repairing and all source hosts not in data partition replications's host
	for _, dataReplica := range dp.Replicas {
		dpHostsMap[dataReplica.Addr] = dataReplica
	}

	isFinished = true
	for _, dataReplica := range dp.Replicas {
		dpMediumType, _ := proto.StrToMediumType(dataReplica.MType)
		if dpMediumType != targetMedium {
			isFinished = false
			// if data replication addr medium type is not target medium type, this replication should be migrated too, maybe data replication changed after task created
			if _, ok := migrateHosts[dataReplica.Addr]; !ok {
				log.LogInfof("[ConsumeTask] dp replication medium type is not target medium type, this replication will be migrated too, "+
					"taskInfo(%v), replicaAddr(%v)", task.String(), dataReplica.Addr)
				migrateHosts[dataReplica.Addr] = dataReplica.Addr
			}
		}
	}
	for host := range migrateHosts {
		if _, exist := dpHostsMap[host]; exist {
			isFinished = false
			break
		}
	}
	if isFinished {
		if err = sv.updateTaskTargetHosts(task, lt, taskInfo, dp.Hosts); err != nil {
			log.LogErrorf("[] update task target hosts failed, cluster(%v), volName(%v), dpId(%v), targetHosts(%v), err(%v)",
				task.Cluster, task.VolName, task.DpId, dp.Hosts, err)
			return
		}
		log.LogInfof("[ConsumeTask] migrate finished, all replication target medium type is same with target medium type, taskInfo(%v), err(%v)", task.String(), err)
		return false, nil
	}

	for migrateHost := range migrateHosts {
		sortHosts = append(sortHosts, migrateHost)
	}
	sort.Strings(sortHosts)
	log.LogDebugf("[ConsumeTask] taskId(%v), cluster(%v), volName(%v), dpId(%v), sourceHosts(%v), migrateHosts(%v), sortHosts(%v)",
		task.TaskId, task.Cluster, task.VolName, task.DpId, sourceHosts, migrateHosts, sortHosts)
	for _, sourceHost := range sortHosts {
		// it need not to transfer, if source host is not in data partition's three replications
		if _, exist := dpHostsMap[sourceHost]; !exist {
			log.LogInfof("[ConsumeTask] sourceHost has not exist in data partition replications, cluster(%v), volName(%v), dpId(%v), sourceHost(%v), dpHosts(%v)",
				task.Cluster, task.VolName, task.DpId, sourceHost, dp.Hosts)
			continue
		}
		// it need not to transfer, if replication medium type is same with smart rule's target medium type
		dpMediumType, _ := proto.StrToMediumType(dpHostsMap[sourceHost].MType)
		if dpMediumType == targetMedium {
			log.LogInfof("[ConsumeTask] sourceHost medium type is same with task target medium, cluster(%v), volName(%v), dpId(%v), sourceHost(%v), hostMediumType(%v), targetMediumType(%v)",
				task.Cluster, task.VolName, task.DpId, sourceHost, dpHostsMap[sourceHost].MType, targetMedium)
			continue
		}
		// if dp replication has not exist in source host, master return error like this: "%v is not in data partition hosts:%v"
		err = sv.mcw[task.Cluster].AdminAPI().TransferSmartVolDataPartition(task.DpId, sourceHost)
		if err != nil {
			log.LogErrorf("[ConsumeTask] decommission data partition failed, taskInfo(%v), sourceHost(%v), err(%v)", task.String(), sourceHost, err)
			return true, err
		}
		// stop to continue transfer after transfer one replication
		log.LogInfof("[ConsumeTask] decommission data partition, cluster(%v), volName(%v), dpId(%v), taskId(%v), sourceHost(%v)",
			task.Cluster, task.VolName, task.DpId, task.TaskId, sourceHost)
		return true, nil
	}
	return true, nil
}

func (sv *SmartVolumeWorker) unmarshallTaskInfo(taskInfo string) (sourceHosts []string, targetMedium proto.MediumType, lt proto.LayerType, ti interface{}, err error) {
	var (
		am *proto.ActionMetricsTaskInfo
		dc *proto.DPCreateTimeTaskInfo
	)
	log.LogDebugf("[unmarshallTaskInfo] taskInfo(%v)", taskInfo)
	index := strings.Index(taskInfo, ":")
	if index <= 0 {
		log.LogErrorf("[unmarshallTaskInfo] parse task info failed, index is less then zero, taskInfo(%v)", taskInfo)
		return nil, 0, 0, nil, errors.New("parse task info failed")
	}
	lpType := util.SubString(taskInfo, 0, index)
	if len(lpType) <= 0 {
		log.LogErrorf("[unmarshallTaskInfo] parse task info layer type failed, taskInfo(%v)", taskInfo)
		return nil, 0, 0, nil, errors.New("parse task info layer type failed")
	}
	taskInfoSubStr := util.SubString(taskInfo, index+1, len(taskInfo))
	if len(taskInfoSubStr) <= 0 {
		log.LogErrorf("[unmarshallTaskInfo] parse task info sub string failed, taskInfo(%v)", taskInfo)
		return nil, 0, 0, nil, errors.New("parse task info sub string failed")
	}
	if lpType == proto.LayerTypeActionMetricsString {
		if am, err = proto.UnmarshallActionMetricsTaskInfo(taskInfoSubStr); err != nil {
			log.LogErrorf("[unmarshallTaskInfo] unmarshall action metrics task info failed, taskInfo(%v), err(%v)", taskInfo, err)
			return nil, 0, 0, nil, err
		}
		return am.SourceHosts, am.TargetMedium, proto.LayerTypeActionMetrics, am, nil
	}
	if lpType == proto.LayerTypeDPCreateTimeString {
		if dc, err = proto.UnmarshallDPCreateTimeTaskInfo(taskInfoSubStr); err != nil {
			log.LogErrorf("[unmarshallTaskInfo] unmarshall data create time task info failed, taskInfo(%v), err(%v)", taskInfo, err)
			return nil, 0, 0, nil, err
		}
		return dc.SourceHosts, dc.TargetMedium, proto.LayerTypeDPCreateTime, dc, nil
	}
	log.LogErrorf("[unmarshallTaskInfo] invalid layer type, taskInfo(%v), lpType(%v)", taskInfo, lpType)
	return nil, 0, 0, nil, errors.New("invalid layer type")
}

// Update task's task info to database, this update to save data partition three replications' hosts after transfer
func (sv *SmartVolumeWorker) updateTaskTargetHosts(task *proto.Task, lt proto.LayerType, taskInfo interface{}, hosts []string) (err error) {
	var info string
	switch lt {
	case proto.LayerTypeActionMetrics:
		am, ok := taskInfo.(*proto.ActionMetricsTaskInfo)
		if !ok {
			return errors.New("task info type is not expected")
		}
		am.TargetHosts = hosts
		if info, err = am.Marshall(); err != nil {
			log.LogErrorf("[updateTaskTargetHosts] marshall action metrics task info failed, cluster(%v), volName(%v), dpId(%v), targetHosts(%v), err(%v)",
				task.Cluster, task.VolName, task.DpId, hosts, err)
			return
		}
		info = fmt.Sprintf("%s:%s", proto.LayerTypeActionMetricsString, info)
	case proto.LayerTypeDPCreateTime:
		dc, ok := taskInfo.(*proto.DPCreateTimeTaskInfo)
		if !ok {
			return errors.New("task info type is not expected")
		}
		dc.TargetHosts = hosts
		if info, err = dc.Marshall(); err != nil {
			log.LogErrorf("[updateTaskTargetHosts] marshall dp create time task info failed, cluster(%v), volName(%v), dpId(%v), targetHosts(%v), err(%v)",
				task.Cluster, task.VolName, task.DpId, hosts, err)
			return
		}
		info = fmt.Sprintf("%s:%s", proto.LayerTypeDPCreateTimeString, info)
	}
	if err = sv.UpdateTaskInfo(task.TaskId, info); err != nil {
		log.LogErrorf("[updateTaskTargetHosts] update task info failed, cluster(%v), volName(%v), dpId(%v), taskInfo(%v), err(%v)",
			task.Cluster, task.VolName, task.DpId, info, err)
		return
	}
	return
}

// The quantity of total data partitions is less than 10, the writable DP cannot be less than 50%
// The quantity of total data partitions is more than 10 and less then 100, the writable DP cannot be less than 20%
// The quantity of total data partitions is more than 100, the writable DP cannot be less than 10%
func (sv *SmartVolumeWorker) isDPSufficientMigrated(cluster, volume string, dpInfo *proto.DataPartitionResponse) (result bool, err error) {
	if dpInfo.Status != proto.ReadWrite {
		return true, nil
	}
	var rate float32
	var writableNum int
	var dpCount int
	var volumeView *proto.SmartVolume

	sv.svvLock.RLock()
	defer sv.svvLock.RUnlock()
	smartVolumeView := sv.svv[cluster]
	if smartVolumeView == nil || len(smartVolumeView.SmartVolumes) == 0 {
		log.LogErrorf("[isDPSufficientMigrated] smart volume is empty, cluster(%v), volumeName(%v), dpInfo(%v)", cluster, volume, dpInfo.PartitionID)
		return false, errors.New("smart volume is empty")
	}
	for _, vv := range smartVolumeView.SmartVolumes {
		if vv.Name == volume {
			volumeView = vv
			break
		}
	}
	if volumeView == nil {
		log.LogErrorf("[isDPSufficientMigrated] volume not exist, cluster(%v), volumeName(%v), dpInfo(%v)", cluster, volume, dpInfo.PartitionID)
		return false, errors.New("volume not exist")
	}
	dpCount = len(volumeView.DataPartitions)
	if dpCount <= 10 {
		rate = 0.5
	}
	if dpCount > 10 && dpCount <= 100 {
		rate = 0.2
	}
	if dpCount > 100 {
		rate = 0.1
	}
	for _, dpInfo := range volumeView.DataPartitions {
		if dpInfo.Status == proto.ReadWrite {
			writableNum++
		}
	}
	log.LogDebugf("[isDPSufficientMigrated] cluster(%v), volName(%v), dpId(%v), status(%v), writableNum(%v), dpCount(%v), rate(%v)",
		cluster, volume, dpInfo.PartitionID, dpInfo.Status, writableNum, dpCount, rate)
	if float32(writableNum)/float32(dpCount) < rate {
		log.LogInfof("[isDPSufficientMigrated] data partition is not sufficient to migrate, writableNum(%v), dpCount(%v), rate(%v), cluster(%v), volName(%v), dpId(%v)",
			writableNum, dpCount, rate, cluster, volume, dpInfo.PartitionID)
		return false, nil
	} else {
		return true, nil
	}
}

func (sv *SmartVolumeWorker) GetCreatorDuration() int {
	return sv.WorkerConfig.TaskCreatePeriod
}

func (sv *SmartVolumeWorker) Shutdown() {
	sv.Control.Shutdown(sv, doShutdown)
}

func doShutdown(server common.Server) {
	sv, ok := server.(*SmartVolumeWorker)
	if !ok {
		return
	}
	close(sv.StopC)
}

func (sv *SmartVolumeWorker) parseConfig(cfg *config.Config) (err error) {
	err = sv.ParseBaseConfig(cfg)
	if err != nil {
		return
	}
	// parse cluster master address
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	var masterAddr []string
	for clusterName, value := range baseInfo {
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
	sv.masterAddr = masters
	// used for cmd to report version
	if len(masterAddr) == 0 {
		cfg.SetStringSlice(proto.MasterAddr, masterAddr)
	}
	// parse HBase config
	hBaseUrl := cfg.GetString(config.ConfigKeyHBaseUrl)
	if util.IsStrEmpty(hBaseUrl) {
		return fmt.Errorf("error: no hBase url")
	}
	sv.HBaseConfig = config.NewHBaseConfig(hBaseUrl)
	return
}

func GetSmartVolumes(cluster string, mc *master.MasterClient) (svv *proto.SmartVolumeView, err error) {
	var volumes []*proto.SmartVolume
	volumes, err = mc.AdminAPI().ListSmartVolumes()
	if err != nil {
		log.LogErrorf("[GetSmartVolumes] list smart volumes failed, cluster(%v), err(%v)", cluster, err)
		return
	}
	if len(volumes) == 0 {
		log.LogInfof("[GetSmartVolumes] no smart volumes, cluster(%v)", cluster)
		return
	}

	smartVolumes := make([]*proto.SmartVolume, 0)
	var smartVolume *proto.SmartVolume
	for _, volume := range volumes {
		smartVolume, err = mc.AdminAPI().GetSmartVolume(volume.Name, CalcAuthKey(volume.Owner))
		if err != nil {
			log.LogErrorf("[GetSmartVolumes] get volume info failed, cluster(%v), volumeName(%v), err(%v)", cluster, volume.Name, err)
			return
		}

		smartVolume.ClusterId = cluster
		smartVolumes = append(smartVolumes, smartVolume)
	}
	svv = proto.NewSmartVolumeView(cluster, smartVolumes)
	return
}

func getStartTime(timeType int8, lessCount int) (timestamp int64, res string, err error) {
	t := time.Now()
	var format string
	switch timeType {
	case proto.ActionMetricsTimeTypeMinute:
		format = "20060102150400"
		duration, _ := time.ParseDuration("-30m")
		t = t.Add(duration)

		lessCount = getStandardMinuteLessCount(lessCount)
		t = t.Add(time.Minute * time.Duration(-lessCount))

		minute := t.Minute()
		if minute%DefaultActionMetricMinuteUnit != 0 {
			remainder := minute % DefaultActionMetricMinuteUnit
			duration, _ := time.ParseDuration(fmt.Sprintf("-%vm", remainder))
			t = t.Add(duration)
		}
	case proto.ActionMetricsTimeTypeHour:
		format = "20060102150000"
		t = t.Add(time.Hour * time.Duration(-lessCount))
	case proto.ActionMetricsTimeTypeDay:
		format = "20060102000000"
		t = t.Add(time.Hour * time.Duration(-lessCount) * 24)
	default:
		err = errors.New(fmt.Sprintf("invalid time type: %v", timeType))
		return
	}
	return t.Unix(), t.Format(format), nil
}

func getStandardMinuteLessCount(lessCountOrigin int) int {
	value := lessCountOrigin / DefaultActionMetricMinuteUnit
	remainder := lessCountOrigin % DefaultActionMetricMinuteUnit
	if remainder == 0 {
		return lessCountOrigin
	}
	return (value + 1) * DefaultActionMetricMinuteUnit
}

func getStopTime(timeType int8) (res string, err error) {
	t := time.Now()
	var format string
	switch timeType {
	case proto.ActionMetricsTimeTypeMinute:
		format = "20060102150400"
		duration, _ := time.ParseDuration("-30m")
		t = t.Add(duration)

		minute := t.Minute()
		if minute%DefaultActionMetricMinuteUnit != 0 {
			remainder := minute % DefaultActionMetricMinuteUnit
			duration, _ := time.ParseDuration(fmt.Sprintf("-%vm", remainder))
			t = t.Add(duration)
		}
	case proto.ActionMetricsTimeTypeHour:
		format = "20060102150000"
	case proto.ActionMetricsTimeTypeDay:
		format = "20060102000000"
	default:
		err = errors.New(fmt.Sprintf("invalid time type: %v", timeType))
		return
	}
	return t.Format(format), nil
}

func (sv *SmartVolumeWorker) MigrateThreshold() map[string]*proto.ScheduleConfig {
	values := make(map[string]*proto.ScheduleConfig)
	sv.dpMigrateThreshold.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(*proto.ScheduleConfig)
		values[k] = v
		return true
	})
	return values
}

func CalcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
