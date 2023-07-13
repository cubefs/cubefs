package fsck

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	scheduleCommon "github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"path"
	"sync"
	"time"
)

type ClusterConfig struct {
	ClusterName  string   `json:"clusterName"`
	MasterAddrs  []string `json:"mastersAddr"`
	MetaProfPort uint16   `json:"mnProfPort"`
	DataProfPort uint16   `json:"dnProfPort"`
}

type FSCheckWorker struct {
	worker.BaseWorker
	sync.RWMutex
	port               string
	clusterConfigMap   map[string]*ClusterConfig
	mcw                map[string]*master.MasterClient
	exportDir          string
	specialCheckRules  map[string]map[string]*scheduleCommon.SpecialOwnerCheckRule
	concurrencyLimiter *scheduleCommon.ConcurrencyLimiter
}


func NewFSCheckWorker() *FSCheckWorker {
	return &FSCheckWorker{}
}

func (fsckWorker *FSCheckWorker) Start(cfg *config.Config) (err error) {
	return fsckWorker.Control.Start(fsckWorker, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	fsckWorker, ok := s.(*FSCheckWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	fsckWorker.StopC = make(chan struct{}, 0)
	fsckWorker.mcw = make(map[string]*master.MasterClient)
	fsckWorker.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	fsckWorker.concurrencyLimiter = scheduleCommon.NewConcurrencyLimiter(DefaultTaskConcurrency)
	fsckWorker.specialCheckRules = make(map[string]map[string]*scheduleCommon.SpecialOwnerCheckRule)

	//parse config
	if err = fsckWorker.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	for clusterName, clusterConfig := range fsckWorker.clusterConfigMap {
		masterClient := master.NewMasterClient(clusterConfig.MasterAddrs, false)
		masterClient.MetaNodeProfPort = clusterConfig.MetaProfPort
		masterClient.DataNodeProfPort = clusterConfig.DataProfPort
		fsckWorker.mcw[clusterName] = masterClient
	}

	if err = mysql.InitMysqlClient(fsckWorker.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	if err = fsckWorker.RegisterWorker(proto.WorkerTypeFSCheck, fsckWorker.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}
	go fsckWorker.updateCheckRule()
	fsckWorker.registerHandle()
	return
}

func (fsckWorker *FSCheckWorker) Shutdown() {
	fsckWorker.Control.Shutdown(fsckWorker, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*FSCheckWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (fsckWorker *FSCheckWorker) Sync() {
	fsckWorker.Control.Sync()
}

func (fsckWorker *FSCheckWorker) parseConfig(cfg *config.Config) (err error) {
	err = fsckWorker.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	fsckWorker.exportDir = cfg.GetString(config.ConfigKeyExportDir)
	clustersInfoData := cfg.GetJsonObjectSlice(config.ConfigKeyClusterInfo)
	fsckWorker.clusterConfigMap = make(map[string]*ClusterConfig, len(clustersInfoData))
	for _, clusterInfoData := range clustersInfoData {
		clusterConf := new(ClusterConfig)
		if err = json.Unmarshal(clusterInfoData, clusterConf); err != nil {
			err = fmt.Errorf("parse cluster info failed:%v", err)
			return
		}
		fsckWorker.clusterConfigMap[clusterConf.ClusterName] = clusterConf
	}
	fsckWorker.port = fsckWorker.Port
	return
}

func (fsckWorker *FSCheckWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogDebugf("ConsumeTask task(%v)", task)
	fsckWorker.concurrencyLimiter.Add()
	defer func() {
		fsckWorker.concurrencyLimiter.Done()
	}()
	if fsckWorker.NodeException {
		return
	}
	masterClient, ok := fsckWorker.mcw[task.Cluster]
	if !ok {
		log.LogErrorf("ConsumeTask get cluster %s masterClient failed", task.Cluster)
		return
	}

	var volumeView *proto.SimpleVolView
	if volumeView, err = masterClient.AdminAPI().GetVolumeSimpleInfo(task.VolName); err != nil {
		log.LogErrorf("ConsumeTask get cluster %s volume %s simple info failed:%v", task.Cluster, task.VolName, err)
		err = nil
		return
	}

	safeCleanInterval := DefaultSafeCleanInterval
	if rule := fsckWorker.getSpecialRule(task.Cluster, volumeView.Owner); rule != nil {
		safeCleanInterval = time.Duration(rule.SafeCleanIntervalMin) * time.Minute
	}
	exportDir := path.Join(fsckWorker.exportDir, task.Cluster, fmt.Sprintf("%s_%s", task.VolName, time.Now().Format(proto.TimeFormat2)))
	fsckTask := NewFSCheckTask(task, masterClient, InodeCheckOpt|DentryCheckOpt, true, safeCleanInterval, exportDir)
	fsckTask.RunOnce()
	return
}

func (fsckWorker *FSCheckWorker) getSpecialRule(clusterID, owner string) (rule *scheduleCommon.SpecialOwnerCheckRule) {
	fsckWorker.RLock()
	defer fsckWorker.RUnlock()

	clusterSpecialRule, ok := fsckWorker.specialCheckRules[clusterID]
	if !ok {
		return
	}

	rule, ok = clusterSpecialRule[owner]
	if !ok {
		return
	}
	return
}

func (fsckWorker *FSCheckWorker) updateCheckRule() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- fsckWorker.StopC:
			return
		case <- timer.C:
			clustersID := fsckWorker.clustersID()

			for _, clusterID := range clustersID {
				var checkRules []*proto.CheckRule
				checkRules, _ = mysql.SelectCheckRule(int(fsckWorker.WorkerType), clusterID)
				ruleMap := make(map[string]string, len(checkRules))
				for _, rule := range checkRules {
					ruleMap[rule.RuleType] = rule.RuleValue
				}
				rules := scheduleCommon.ParseSpecialOwnerRules(ruleMap)
				if len(rules) == 0 {
					continue
				}
				fsckWorker.Lock()
				fsckWorker.specialCheckRules[clusterID] = rules
				fsckWorker.Unlock()
			}
			timer.Reset(time.Minute * 10)
		}
	}
}

func (fsckWorker *FSCheckWorker) clustersID() []string {
	fsckWorker.RLock()
	defer fsckWorker.RUnlock()

	clustersID := make([]string, 0, len(fsckWorker.clusterConfigMap))
	for clusterID := range fsckWorker.clusterConfigMap {
		clustersID = append(clustersID, clusterID)
	}
	return clustersID
}