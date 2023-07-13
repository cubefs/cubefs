package mdck

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
	"sync"
	"time"
)

type ClusterConfig struct {
	ClusterName  string   `json:"clusterName"`
	MasterAddrs  []string `json:"mastersAddr"`
	MetaProfPort uint16   `json:"mnProfPort"`
	DataProfPort uint16   `json:"dnProfPort"`
}

type MetaDataCheckWorker struct {
	worker.BaseWorker
	sync.RWMutex
	port               string
	clusterConfigMap   map[string]*ClusterConfig
	mcw                map[string]*master.MasterClient
	exportDir          string
	mailTo             []string
	concurrencyLimiter *scheduleCommon.ConcurrencyLimiter
}


func NewMetaDataCheckWorker() *MetaDataCheckWorker {
	return &MetaDataCheckWorker{}
}

func (mdckWorker *MetaDataCheckWorker) Start(cfg *config.Config) (err error) {
	return mdckWorker.Control.Start(mdckWorker, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	mdckWorker, ok := s.(*MetaDataCheckWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	mdckWorker.StopC = make(chan struct{}, 0)
	mdckWorker.mcw = make(map[string]*master.MasterClient)
	mdckWorker.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	mdckWorker.concurrencyLimiter = scheduleCommon.NewConcurrencyLimiter(DefaultMpConcurrency)

	//parse config
	if err = mdckWorker.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	for clusterName, clusterConfig := range mdckWorker.clusterConfigMap {
		masterClient := master.NewMasterClient(clusterConfig.MasterAddrs, false)
		masterClient.MetaNodeProfPort = clusterConfig.MetaProfPort
		masterClient.DataNodeProfPort = clusterConfig.DataProfPort
		mdckWorker.mcw[clusterName] = masterClient
	}

	if err = mysql.InitMysqlClient(mdckWorker.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	if err = mdckWorker.RegisterWorker(proto.WorkerTypeMetaDataCrcCheck, mdckWorker.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}

	go mdckWorker.updateMailToMember()
	mdckWorker.registerHandle()
	return
}

func (mdckWorker *MetaDataCheckWorker) Shutdown() {
	mdckWorker.Control.Shutdown(mdckWorker, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*MetaDataCheckWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (mdckWorker *MetaDataCheckWorker) Sync() {
	mdckWorker.Control.Sync()
}

func (mdckWorker *MetaDataCheckWorker) parseConfig(cfg *config.Config) (err error) {
	err = mdckWorker.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	clustersInfoData := cfg.GetJsonObjectSlice(config.ConfigKeyClusterInfo)
	mdckWorker.clusterConfigMap = make(map[string]*ClusterConfig, len(clustersInfoData))
	for _, clusterInfoData := range clustersInfoData {
		clusterConf := new(ClusterConfig)
		if err = json.Unmarshal(clusterInfoData, clusterConf); err != nil {
			err = fmt.Errorf("parse cluster info failed:%v", err)
			return
		}
		mdckWorker.clusterConfigMap[clusterConf.ClusterName] = clusterConf
	}
	mdckWorker.port = mdckWorker.Port
	return
}

func (mdckWorker *MetaDataCheckWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogDebugf("ConsumeTask task(%v)", task)
	mdckWorker.concurrencyLimiter.Add()
	defer func() {
		mdckWorker.concurrencyLimiter.Done()
	}()
	if mdckWorker.NodeException {
		return
	}
	masterClient, ok := mdckWorker.mcw[task.Cluster]
	if !ok {
		log.LogErrorf("ConsumeTask get cluster %s masterClient failed", task.Cluster)
		return
	}

	mdckTask := NewMetaDataCheckTask(task, masterClient, mdckWorker.mailTo)
	mdckTask.RunOnce()
	return
}

func (mdckWorker *MetaDataCheckWorker) clustersID() []string {
	mdckWorker.RLock()
	defer mdckWorker.RUnlock()

	clustersID := make([]string, 0, len(mdckWorker.clusterConfigMap))
	for clusterID := range mdckWorker.clusterConfigMap {
		clustersID = append(clustersID, clusterID)
	}
	return clustersID
}

func (mdckWorker *MetaDataCheckWorker) updateMailToMember() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- mdckWorker.StopC:
			return
		case <- timer.C:
			timer.Reset(time.Hour * 1)
			members, err := mysql.SelectEmailMembers(mdckWorker.WorkerType)
			if err != nil {
				log.LogErrorf("updateMailToMember select mail to members failed:%v", err)
			}

			if len(members) != 0 {
				mdckWorker.mailTo = members
			}

			if len(mdckWorker.mailTo) == 0 {
				mdckWorker.mailTo = []string{DefaultMailToMember}
			}
		}
	}
}