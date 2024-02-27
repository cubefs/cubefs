package blck

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
	IsDBBack     bool     `json:"isDBBack"`
}

type BlcokCheckWorker struct {
	worker.BaseWorker
	sync.RWMutex
	port               string
	clusterConfigMap   map[string]*ClusterConfig
	mcw                map[string]*master.MasterClient
	exportDir          string
	concurrencyLimiter *scheduleCommon.ConcurrencyLimiter
	mailTo             []string
	alarmErps          []string
	alarmPhones        []string
}


func NewBLockCheckWorker() *BlcokCheckWorker {
	return &BlcokCheckWorker{}
}

func (blckWorker *BlcokCheckWorker) Start(cfg *config.Config) (err error) {
	return blckWorker.Control.Start(blckWorker, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	blckWorker, ok := s.(*BlcokCheckWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	blckWorker.StopC = make(chan struct{}, 0)
	blckWorker.mcw = make(map[string]*master.MasterClient)
	blckWorker.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	blckWorker.concurrencyLimiter = scheduleCommon.NewConcurrencyLimiter(DefaultTaskConcurrency)

	//parse config
	if err = blckWorker.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	for clusterName, clusterConfig := range blckWorker.clusterConfigMap {
		masterClient := master.NewMasterClient(clusterConfig.MasterAddrs, false)
		if clusterConfig.IsDBBack {
			masterClient.IsDbBack = true
		}
		masterClient.MetaNodeProfPort = clusterConfig.MetaProfPort
		masterClient.DataNodeProfPort = clusterConfig.DataProfPort
		blckWorker.mcw[clusterName] = masterClient
	}

	if err = mysql.InitMysqlClient(blckWorker.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	if err = blckWorker.RegisterWorker(proto.WorkerTypeBlockCheck, blckWorker.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}

	blckWorker.registerHandle()
	go blckWorker.updateNotifyMembers()
	return
}

func (blckWorker *BlcokCheckWorker) Shutdown() {
	blckWorker.Control.Shutdown(blckWorker, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*BlcokCheckWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (blckWorker *BlcokCheckWorker) Sync() {
	blckWorker.Control.Sync()
}

func (blckWorker *BlcokCheckWorker) parseConfig(cfg *config.Config) (err error) {
	err = blckWorker.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	blckWorker.exportDir =cfg.GetString(config.ConfigKeyExportDir)
	clustersInfoData := cfg.GetJsonObjectSlice(config.ConfigKeyClusterInfo)
	blckWorker.clusterConfigMap = make(map[string]*ClusterConfig, len(clustersInfoData))
	for _, clusterInfoData := range clustersInfoData {
		clusterConf := new(ClusterConfig)
		if err = json.Unmarshal(clusterInfoData, clusterConf); err != nil {
			err = fmt.Errorf("parse cluster info failed:%v", err)
			return
		}
		blckWorker.clusterConfigMap[clusterConf.ClusterName] = clusterConf
	}
	blckWorker.port = blckWorker.Port
	return
}

func (blckWorker *BlcokCheckWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogDebugf("ConsumeTask task(%v)", task)
	blckWorker.concurrencyLimiter.Add()
	defer func() {
		blckWorker.concurrencyLimiter.Done()
	}()
	if blckWorker.NodeException {
		return
	}
	if task.Status == proto.TaskStatusSucceed {
		return
	}
	masterClient, ok := blckWorker.mcw[task.Cluster]
	if !ok {
		log.LogErrorf("ConsumeTask get cluster %s masterClient failed:%v", task.Cluster, err)
		return
	}

	exportDir := path.Join(blckWorker.exportDir, task.Cluster, fmt.Sprintf("%s_%s", task.VolName, time.Now().Format(proto.TimeFormat2)))
	blckTask := NewBlockCheckTask(task, masterClient, true, true, DefaultSafeCleanInterval, exportDir)
	blckTask.RunOnce()
	return
}

func (blckWorker *BlcokCheckWorker) clustersID() []string {
	blckWorker.RLock()
	defer blckWorker.RUnlock()

	clustersID := make([]string, 0, len(blckWorker.clusterConfigMap))
	for clusterID := range blckWorker.clusterConfigMap {
		clustersID = append(clustersID, clusterID)
	}
	return clustersID
}

func (blckWorker *BlcokCheckWorker) updateNotifyMembers() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- blckWorker.StopC:
			return
		case <- timer.C:
			timer.Reset(time.Hour * 1)
			emailMembers, alarmMembers, callMembers, err := mysql.SelectNotifyMembers(blckWorker.WorkerType)
			if err != nil {
				log.LogErrorf("updateMailToMember select mail to members failed:%v", err)
			}
			log.LogInfof("updateMailToMember email members: %v, alarm members: %v, call members: %v",
				emailMembers, alarmMembers, callMembers)

			blckWorker.mailTo = emailMembers
			blckWorker.alarmErps = alarmMembers
			blckWorker.alarmPhones = callMembers

			if len(blckWorker.mailTo) == 0 {
				blckWorker.mailTo = []string{DefaultMailToMember}
			}

			if len(blckWorker.alarmErps) == 0 {
				blckWorker.alarmErps = []string{DefaultAlarmErps}
			}
		}
	}
}