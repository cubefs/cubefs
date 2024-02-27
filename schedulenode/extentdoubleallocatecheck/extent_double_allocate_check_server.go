package extentdoubleallocatecheck

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

type ExtentDoubleAllocateCheckWorker struct {
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


func NewExtentDoubleAllocateCheckWorker() *ExtentDoubleAllocateCheckWorker {
	return &ExtentDoubleAllocateCheckWorker{}
}

func (w *ExtentDoubleAllocateCheckWorker) Start(cfg *config.Config) (err error) {
	return w.Control.Start(w, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	w, ok := s.(*ExtentDoubleAllocateCheckWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	w.StopC = make(chan struct{}, 0)
	w.mcw = make(map[string]*master.MasterClient)
	w.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	w.concurrencyLimiter = scheduleCommon.NewConcurrencyLimiter(DefaultTaskConcurrency)

	//parse config
	if err = w.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	for clusterName, clusterConfig := range w.clusterConfigMap {
		masterClient := master.NewMasterClient(clusterConfig.MasterAddrs, false)
		if clusterConfig.IsDBBack {
			masterClient.IsDbBack = true
		}
		masterClient.MetaNodeProfPort = clusterConfig.MetaProfPort
		masterClient.DataNodeProfPort = clusterConfig.DataProfPort
		w.mcw[clusterName] = masterClient
	}

	if err = mysql.InitMysqlClient(w.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	if err = w.RegisterWorker(proto.WorkerTypeExtentDoubleAllocateCheck, w.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}

	w.registerHandle()
	go w.updateNotifyMembers()
	return
}

func (w *ExtentDoubleAllocateCheckWorker) Shutdown() {
	w.Control.Shutdown(w, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*ExtentDoubleAllocateCheckWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (w *ExtentDoubleAllocateCheckWorker) Sync() {
	w.Control.Sync()
}

func (w *ExtentDoubleAllocateCheckWorker) parseConfig(cfg *config.Config) (err error) {
	err = w.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	w.exportDir =cfg.GetString(config.ConfigKeyExportDir)
	clustersInfoData := cfg.GetJsonObjectSlice(config.ConfigKeyClusterInfo)
	w.clusterConfigMap = make(map[string]*ClusterConfig, len(clustersInfoData))
	for _, clusterInfoData := range clustersInfoData {
		clusterConf := new(ClusterConfig)
		if err = json.Unmarshal(clusterInfoData, clusterConf); err != nil {
			err = fmt.Errorf("parse cluster info failed:%v", err)
			return
		}
		w.clusterConfigMap[clusterConf.ClusterName] = clusterConf
	}
	w.port = w.Port
	return
}

func (w *ExtentDoubleAllocateCheckWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogDebugf("ConsumeTask task(%v)", task)
	w.concurrencyLimiter.Add()
	defer func() {
		w.concurrencyLimiter.Done()
	}()
	if w.NodeException {
		return
	}
	if task.Status == proto.TaskStatusSucceed {
		return
	}
	masterClient, ok := w.mcw[task.Cluster]
	if !ok {
		log.LogErrorf("ConsumeTask get cluster %s masterClient failed:%v", task.Cluster, err)
		return
	}

	exportDir := path.Join(w.exportDir, task.Cluster, fmt.Sprintf("%s_%s", task.VolName, time.Now().Format(proto.TimeFormat2)))
	t := NewExtentDoubleAllocateCheckTask(task, masterClient, exportDir)
	t.RunOnce()
	return
}

func (w *ExtentDoubleAllocateCheckWorker) clustersID() []string {
	w.RLock()
	defer w.RUnlock()

	clustersID := make([]string, 0, len(w.clusterConfigMap))
	for clusterID := range w.clusterConfigMap {
		clustersID = append(clustersID, clusterID)
	}
	return clustersID
}

func (w *ExtentDoubleAllocateCheckWorker) updateNotifyMembers() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- w.StopC:
			return
		case <- timer.C:
			timer.Reset(time.Hour * 1)
			emailMembers, alarmMembers, callMembers, err := mysql.SelectNotifyMembers(w.WorkerType)
			if err != nil {
				log.LogErrorf("updateMailToMember select mail to members failed:%v", err)
			}
			log.LogInfof("updateMailToMember email members: %v, alarm members: %v, call members: %v",
				emailMembers, alarmMembers, callMembers)

			w.mailTo = emailMembers
			w.alarmErps = alarmMembers
			w.alarmPhones = callMembers

			if len(w.mailTo) == 0 {
				w.mailTo = []string{DefaultMailToMember}
			}

			if len(w.alarmErps) == 0 {
				w.alarmErps = []string{DefaultAlarmErps}
			}
		}
	}
}