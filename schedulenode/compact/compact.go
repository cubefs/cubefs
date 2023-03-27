package compact

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type CompactWorker struct {
	worker.BaseWorker
	port          string
	masterAddr    map[string][]string
	mcw           map[string]*master.MasterClient
	mcwRWMutex    sync.RWMutex
	cvv           map[string]*proto.CompactVolumeView
	volumeTaskPos map[string]uint64
	volumeTaskCnt map[string]uint64
	clusterMap    map[string]*ClusterInfo
	mcc           *metaNodeControlConfig
	sync.RWMutex
	concurrencyLimiter *concurrencyLimiter
}

type metaNodeControlConfig struct {
	inodeCheckStep  int
	inodeConcurrent int
	minEkLen        int
	minInodeSize    uint64
	maxEkAvgSize    uint64
}

func NewCompactWorker() *CompactWorker {
	return &CompactWorker{}
}

func (cw *CompactWorker) Start(cfg *config.Config) (err error) {
	return cw.Control.Start(cw, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	cw, ok := s.(*CompactWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}

	cw.StopC = make(chan struct{}, 0)
	cw.mcw = make(map[string]*master.MasterClient)
	cw.cvv = make(map[string]*proto.CompactVolumeView)
	cw.clusterMap = make(map[string]*ClusterInfo)
	cw.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	cw.concurrencyLimiter = NewConcurrencyLimiter(DefaultMpConcurrency)
	if err = cw.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}
	cw.parseMetaNodeControlConfig(cfg)

	for clusterName, nodes := range cw.masterAddr {
		materClient := master.NewMasterClient(nodes, false)
		cw.mcw[clusterName] = materClient
		cw.clusterMap[clusterName] = NewClusterInfo(clusterName, materClient)
	}

	if err = mysql.InitMysqlClient(cw.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}

	if err = cw.RegisterWorker(proto.WorkerTypeCompact, cw.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register compact volume worker failed, error(%v)", err)
		return
	}

	go cw.loadVolumeInfo()
	go cw.releaseCompVolume()
	cw.registerHandler()

	return
}

func (cw *CompactWorker) Shutdown() {
	cw.Control.Shutdown(cw, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*CompactWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (cw *CompactWorker) Sync() {
	cw.Control.Sync()
}

func (cw *CompactWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	log.LogDebugf("ConsumeTask task(%v)", task)
	cw.concurrencyLimiter.Add()
	defer func() {
		cw.concurrencyLimiter.Done()
	}()
	if cw.NodeException {
		return
	}
	forceRow := cw.getVolumeForceRow(task.Cluster, task.VolName)
	if !forceRow {
		log.LogWarnf("ConsumeTask getVolumeForceRow forceRow(%v) task(%v)", forceRow, task)
		return false, nil
	}

	if task.TaskInfo != proto.CompactOpenName {
		log.LogWarnf("ConsumeTask has canceled the compact task(%v)", task)
		return false, nil
	}
	if err = cw.addClusterVolume(task.Cluster, task.VolName); err != nil {
		return
	}
	var mc *master.MasterClient
	var ok bool
	if mc, ok = cw.getMasterClient(task.Cluster); !ok {
		log.LogErrorf("ConsumeTask getMasterClient cluster(%v) does not exist", task.Cluster)
		return true, nil
	}
	var vol *CompactVolumeInfo
	if vol, ok = cw.getVolumeInfo(task.Cluster, task.VolName); !ok {
		log.LogErrorf("ConsumeTask getVolumeInfo cluster(%v) volName(%v) does not exist", task.Cluster, task.VolName)
		return true, nil
	}

	cmpTask := NewMpCmpTask(task, mc, vol)
	var isFinished bool
	isFinished, err = cmpTask.RunOnce(false)
	if err != nil {
		log.LogErrorf("ConsumeTask cmpTask RunOnce cluster(%v) volName(%v) mpId(%v) err(%v)", task.Cluster, task.VolName, cmpTask.id, err)
		switch err.Error() {
		case proto.ErrMetaPartitionNotExists.Error(), proto.ErrVolNotExists.Error():
			return false, nil
		default:
			return true, err
		}
	}
	return !isFinished, nil
}

func (cw *CompactWorker) getVolumeForceRow(cluster, volName string) bool {
	cw.RLock()
	defer cw.RUnlock()
	volViews, ok := cw.cvv[cluster]
	if ok {
		for _, cmpVolume := range volViews.CompactVolumes {
			if volName == cmpVolume.Name {
				return cmpVolume.ForceROW
			}
		}
	}
	return false
}

func (cw *CompactWorker) parseConfig(cfg *config.Config) (err error) {
	err = cw.ParseBaseConfig(cfg)
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
	cw.masterAddr = masters
	cw.port = cw.Port
	return
}

func (cw *CompactWorker) parseMetaNodeControlConfig(cfg *config.Config) {
	mcc := &metaNodeControlConfig{}
	if mcc.inodeCheckStep = int(cfg.GetFloat(ConfigKeyInodeCheckStep)); mcc.inodeCheckStep <= 0 {
		mcc.inodeCheckStep = DefaultInodeCheckStep
	}
	if mcc.inodeConcurrent = int(cfg.GetFloat(ConfigKeyInodeConcurrent)); mcc.inodeConcurrent <= 0 {
		mcc.inodeConcurrent = DefaultInodeConcurrent
	}
	if mcc.minEkLen = int(cfg.GetFloat(ConfigKeyMinEkLen)); mcc.minEkLen <= 0 {
		mcc.minEkLen = DefaultMinEkLen
	}
	if mcc.minInodeSize = uint64(cfg.GetFloat(ConfigKeyMinInodeSize)); mcc.minInodeSize <= 0 {
		mcc.minInodeSize = DefaultMinInodeSize
	}
	if mcc.maxEkAvgSize = uint64(cfg.GetFloat(ConfigKeyMaxEkAvgSize)); mcc.maxEkAvgSize <= 0 {
		mcc.maxEkAvgSize = DefaultMaxEkAvgSize
	}
	cw.mcc = mcc
}

// loadVolumeInfo worker进程调用此方法
func (cw *CompactWorker) loadVolumeInfo() {
	timer := time.NewTimer(0)
	for {
		log.LogDebugf("loadVolumeInfo volume info loader is running, workerId(%v), workerType(%v), localIp(%v)",
			cw.WorkerId, cw.WorkerType, cw.LocalIp)
		select {
		case <-timer.C:
			loader := func() (err error) {
				cw.mcwRWMutex.RLock()
				defer cw.mcwRWMutex.RUnlock()
				for clusterName, masterClient := range cw.mcw {
					var cmpVolView *proto.CompactVolumeView
					cmpVolView, err = GetCompactVolumes(clusterName, masterClient)
					if err != nil {
						log.LogErrorf("loadVolumeInfo get cluster compact volumes failed, cluster(%v), err(%v)", clusterName, err)
						continue
					}
					if cmpVolView == nil || len(cmpVolView.CompactVolumes) == 0 {
						log.LogWarnf("loadVolumeInfo got all compact volumes is empty, cluster(%v)", clusterName)
						continue
					}
					cw.Lock()
					cw.cvv[clusterName] = cmpVolView
					cw.Unlock()
				}
				return
			}
			if err := loader(); err != nil {
				log.LogErrorf("[loadVolumeInfo] compact volume loader has exception, err(%v)", err)
			}
			cw.updateVolumeState()
			timer.Reset(time.Second * DefaultCompactVolumeLoadDuration)
		case <-cw.StopC:
			timer.Stop()
			return
		}
	}
}

func (cw *CompactWorker) releaseCompVolume() {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			releaseUnusedVolume := func() {
				cw.RLock()
				defer cw.RUnlock()
				for _, clusterInfo := range cw.clusterMap {
					clusterInfo.releaseUnusedCompVolume()
				}
			}
			releaseUnusedVolume()
			timer.Reset(time.Second * DefaultCompactVolumeLoadDuration)
		case <-cw.StopC:
			timer.Stop()
			return
		}
	}
}

func (cw *CompactWorker) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("compact")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc(CmpList, cw.compactInfo)
	http.HandleFunc(CmpInfo, cw.info)
	http.HandleFunc(CmpStop, cw.stop)
	http.HandleFunc(CmpInsertMp, cw.addMp)
	http.HandleFunc(CmpInsertInode, cw.addInode)
	http.HandleFunc(CmpStatus, cw.status)
	http.HandleFunc(CmpConcurrencySetLimit, cw.setLimit)
	http.HandleFunc(CmpConcurrencyGetLimit, cw.getLimit)
}

func (cw *CompactWorker) genCmpMpTaskKey(clusterName, volName string, pid uint64) string {
	return fmt.Sprintf("%s#%s#%d", clusterName, volName, pid)
}

func (cw *CompactWorker) GetClusterInfo(clusterName string) *ClusterInfo {
	cw.RLock()
	defer cw.RUnlock()
	mc, ok := cw.clusterMap[clusterName]
	if !ok {
		return nil
	}
	return mc
}

func (cw *CompactWorker) addVolume(clusterName, volName string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("Add vol[%s-%s] failed:%s", clusterName, volName, err.Error())
		}
	}()
	clusterInfo := cw.GetClusterInfo(clusterName)
	if clusterInfo == nil {
		err = fmt.Errorf("can not support cluster[%s]", clusterName)
		return
	}

	if err = clusterInfo.CheckAndAddCompactVol(volName, cw.mcc); err != nil {
		log.LogErrorf("CheckAndAddCompactVol cluster:%v, volume:%v", clusterInfo.name, volName)
		return
	}
	return
}

func (cw *CompactWorker) addCompactInode(clusterName, volName, mpId, inodeId string) (err error) {
	// 判断cluster和volume是否存在
	clusterInfo := cw.GetClusterInfo(clusterName)
	if clusterInfo == nil {
		return
	}

	return
}

func (cw *CompactWorker) getCmpVolume(clusterName, volName string) (*CompactVolumeInfo, error) {
	mc := cw.GetClusterInfo(clusterName)
	if mc == nil {
		return nil, fmt.Errorf("can not support cluster[%s]", clusterName)
	}

	return mc.GetCompactVolByName(volName), nil
}

func (cw *CompactWorker) addClusterVolume(cluster, volName string) (err error) {
	if err = cw.addClusterInfo(cluster, cw.masterAddr[cluster]); err != nil {
		err = fmt.Errorf("[addVolumeTask] addClusterInfo fail task.Cluster(%v), task.VolName(%v), err(%v)", cluster, volName, err)
		return
	}
	if err = cw.addVolume(cluster, volName); err != nil {
		err = fmt.Errorf("[addVolumeTask] addVolume fail task.Cluster(%v), task.VolName(%v), err(%v)", cluster, volName, err)
	}
	return
}

func (cw *CompactWorker) addClusterInfo(clusterName string, nodes []string) (err error) {
	cw.Lock()
	defer cw.Unlock()
	if len(nodes) == 0 {
		return fmt.Errorf("addMp cluster no node in param")
	}
	mc, ok := cw.clusterMap[clusterName]
	if !ok {
		materClient := master.NewMasterClient(nodes, false)
		cw.mcwRWMutex.Lock()
		cw.mcw[clusterName] = materClient
		cw.mcwRWMutex.Unlock()
		cw.clusterMap[clusterName] = NewClusterInfo(clusterName, materClient)
		return
	}
	for _, node := range nodes {
		for _, existNode := range mc.Nodes() {
			if strings.Contains(existNode, node) {
				continue
			}
			mc.AddNode(node)
		}
	}
	return
}

func (cw *CompactWorker) getClusterConf(clusterName string) (mc *ClusterInfo, exist bool) {
	cw.RLock()
	defer cw.RUnlock()
	mc, exist = cw.clusterMap[clusterName]
	return
}

func (cw *CompactWorker) getMasterClient(clusterName string) (*master.MasterClient, bool) {
	cw.RLock()
	defer cw.RUnlock()
	clusterInfo := cw.clusterMap[clusterName]
	if clusterInfo == nil {
		return nil, false
	}
	return clusterInfo.mc, true
}

func (cw *CompactWorker) getVolumeInfo(clusterName, volName string) (*CompactVolumeInfo, bool) {
	cw.RLock()
	defer cw.RUnlock()
	clusterInfo := cw.clusterMap[clusterName]
	if clusterInfo == nil {
		return nil, false
	}
	volumeInfo := clusterInfo.GetCompactVolByName(volName)
	if volumeInfo == nil {
		return nil, false
	}
	return volumeInfo, true
}

func (cw *CompactWorker) updateVolumeState() {
	cw.RLock()
	defer cw.RUnlock()
	for clusterName, volumeView := range cw.cvv {
		for _, compactVolume := range volumeView.CompactVolumes {
			var clusterInfo *ClusterInfo
			var ok bool
			if clusterInfo, ok = cw.clusterMap[clusterName]; !ok {
				break
			}
			switch compactVolume.CompactTag {
			case proto.CompactClose:
				clusterInfo.UpdateVolState(compactVolume.Name, CmpVolClosingST)
			default:
			}
		}
	}
}
