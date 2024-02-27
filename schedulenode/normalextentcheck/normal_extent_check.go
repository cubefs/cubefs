package normalextentcheck

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
	"github.com/cubefs/cubefs/util/notify"
	"sync"
	"time"
)

var InodeExtentCountMaxCountThreshold int64 = DefaultInodeEKMaxCountThreshold

type ClusterConfig struct {
	ClusterName  string   `json:"clusterName"`
	MasterAddrs  []string `json:"mastersAddr"`
	MetaProfPort uint16   `json:"mnProfPort"`
	DataProfPort uint16   `json:"dnProfPort"`
	IsDBBack     bool     `json:"isDBBack"`
}

type NormalExtentCheckWorker struct {
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

func NewNormalExtentCheckWorker() *NormalExtentCheckWorker {
	return &NormalExtentCheckWorker{}
}

func (w *NormalExtentCheckWorker) Start(cfg *config.Config) (err error) {
	return w.Control.Start(w, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	w, ok := s.(*NormalExtentCheckWorker)
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

	if err = w.RegisterWorker(proto.WorkerTypeNormalExtentMistakeDelCheck, w.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}

	w.registerHandle()
	go w.updateNotifyMembers()
	return
}

func (w *NormalExtentCheckWorker) Shutdown() {
	w.Control.Shutdown(w, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*NormalExtentCheckWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (w *NormalExtentCheckWorker) Sync() {
	w.Control.Sync()
}

func (w *NormalExtentCheckWorker) parseConfig(cfg *config.Config) (err error) {
	err = w.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	inodeEKCountThreshold := cfg.GetInt64(config.ConfigKeyExtentMaxCountThreshold)
	if inodeEKCountThreshold != 0 {
		InodeExtentCountMaxCountThreshold = inodeEKCountThreshold
	}

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

func (w *NormalExtentCheckWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogInfof("ConsumeTask task(%v)", task)
	w.concurrencyLimiter.Add()
	defer func() {
		w.concurrencyLimiter.Done()
	}()
	if w.NodeException {
		return
	}
	masterClient, ok := w.mcw[task.Cluster]
	if !ok {
		log.LogErrorf("ConsumeTask get cluster %s masterClient failed:%v", task.Cluster, err)
		return
	}

	ekCheckTask := NewNormalExtentCheckTask(task, masterClient, true, "", w.mailTo, w.alarmErps, w.alarmPhones)
	if err = ekCheckTask.RunOnce(); err != nil {
		log.LogErrorf("ConsumeTask task run failed, cluster(%v) taskType(%v) err(%v)", task.Cluster, proto.WorkerTypeToName(task.TaskType), err)
		_ = mysql.RecordNormalEKCheckFailedResult(task.Cluster, task.VolName, err.Error())
		err = nil
		return
	}

	if len(ekCheckTask.mistakeDelEK) != 0 {
		needSendEmail := false
		for start := 0; start < len(ekCheckTask.searchResult); {
			end := start + 100
			if end > len(ekCheckTask.searchResult) {
				end = len(ekCheckTask.searchResult)
			}
			if err = mysql.RecordBatchNormalEKCheckResult(task.Cluster, task.VolName, ekCheckTask.searchResult[start:end]); err != nil {
				//记录失败通过单独的邮件发送结果
				needSendEmail = true
				break
			}
			start = end
		}

		for _, extentInfo := range ekCheckTask.searchFailed {
			var count int
			if count, err = mysql.SelectAlreadyNotifyCheckResultByDpIdAndExtentId(ekCheckTask.Cluster, ekCheckTask.VolName,
				extentInfo.DataPartitionID, extentInfo.ExtentID); err != nil {
					//记录失败通过单独的邮件发送结果
					needSendEmail = true
					break
			}
			if count != 0 {
				log.LogDebugf("ConsumeTask search inodeID failed extent already notify, cluster(%s) volName(%s) dpID(%v) extentID(%v)",
					ekCheckTask.Cluster, ekCheckTask.VolName, extentInfo.DataPartitionID, extentInfo.ExtentID)
				continue
			}
			if err = mysql.RecordNormalEKSearchFailedResult(ekCheckTask.Cluster, ekCheckTask.VolName,
				extentInfo.DataPartitionID, extentInfo.ExtentID); err != nil {
				//记录失败通过单独的邮件发送结果
				needSendEmail = true
				break
			}
		}

		if needSendEmail {
			notifyServer := notify.NewNotify(w.NotifyConfig)
			notifyServer.SetAlarmEmails(w.mailTo)
			notifyServer.SetAlarmErps(w.alarmErps)
			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS 误删除Extent查找结果", ekCheckTask.formatMistakeDelEKSearchEmailContent())
		}
	}

	if len(ekCheckTask.extentConflict) != 0 {
		for conflictExtent, ownerInodes := range ekCheckTask.extentConflict {
			if err = mysql.RecordNormalEKAllocateConflict(ekCheckTask.Cluster, ekCheckTask.VolName, conflictExtent.DataPartitionID,
				conflictExtent.ExtentID, ownerInodes); err != nil {
				//记录失败通过单独的邮件发送结果
				notifyServer := notify.NewNotify(w.NotifyConfig)
				notifyServer.SetAlarmEmails(w.mailTo)
				notifyServer.SetAlarmErps(w.alarmErps)
				notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS 冲突Extent查找结果", ekCheckTask.formatExtentConflictEmailContent())
				break
			}
		}
	}
	return
}

func (w *NormalExtentCheckWorker) clustersID() []string {
	w.RLock()
	defer w.RUnlock()

	clustersID := make([]string, 0, len(w.clusterConfigMap))
	for clusterID := range w.clusterConfigMap {
		clustersID = append(clustersID, clusterID)
	}
	return clustersID
}

func (w *NormalExtentCheckWorker) updateMailToMember() {
	timer := time.NewTimer(0)
	for {
		select {
		case <- w.StopC:
			return
		case <- timer.C:
			timer.Reset(time.Hour * 1)
			members, err := mysql.SelectEmailMembers(w.WorkerType)
			if err != nil {
				log.LogErrorf("updateMailToMember select mail to members failed:%v", err)
			}
			log.LogInfof("updateMailToMember email members: %v", members)

			if len(members) != 0 {
				w.mailTo = members
			}

			if len(w.mailTo) == 0 {
				w.mailTo = []string{DefaultMailToMember}
			}
		}
	}
}

func (w *NormalExtentCheckWorker) updateNotifyMembers() {
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