package fetchtopology

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var rateLimitProperties = multirate.Properties{
	{multirate.PropertyTypeOp, strconv.Itoa(int(proto.OpFetchDataPartitionView))},
}

const (
	sleepForUpdateVolConf                  = time.Second * 1
	intervalToUpdateVolConf                = time.Minute * 5
	intervalFetchDataPartitionView         = time.Hour * 24
	defIntervalForceFetchDataPartitionView = time.Minute * 5
	forceFetchDataPartitionViewChSize      = 1024
	defPostByDomainMaxErrorCount           = 1000
)

type ForceFetchDataPartition struct {
	volumeName      string
	dataPartitionID uint64
}

type BatchFetchDataPartitionsMap map[string][]uint64

type FetchTopologyManager struct {
	vols                          *sync.Map
	forceFetchDPViewCh            chan *ForceFetchDataPartition
	stopCh                        chan bool
	masterClient                  *master.MasterClient
	masterDomainClient            *master.MasterClient
	masterDomainRequestErrorCount uint64
	needFetchVolAllDPView         bool
	needUpdateVolsConf            bool
	limiter                       *multirate.MultiLimiter
	forceFetchTimerInterval       time.Duration
}

func NewFetchTopoManager(forceFetchTimerInterval time.Duration, masterClient, masterDomainClient *master.MasterClient,
	needFetchVolAllDPView, needUpdateVolsConf bool, limiter *multirate.MultiLimiter) *FetchTopologyManager {
	if forceFetchTimerInterval == 0 {
		forceFetchTimerInterval = defIntervalForceFetchDataPartitionView
	}
	return &FetchTopologyManager{
		vols:                    new(sync.Map),
		forceFetchDPViewCh:      make(chan *ForceFetchDataPartition, forceFetchDataPartitionViewChSize),
		stopCh:                  make(chan bool),
		forceFetchTimerInterval: forceFetchTimerInterval,
		masterClient:            masterClient,
		masterDomainClient:      masterDomainClient,
		needFetchVolAllDPView:   needFetchVolAllDPView,
		needUpdateVolsConf:      needUpdateVolsConf,
		limiter:                 limiter,
	}
}

func (f *FetchTopologyManager) Start() (err error) {
	if err = f.updateVolumeConfSchedule(); err != nil {
		err = fmt.Errorf("FetchTopologyManager Start updateVolumeConfSchedule failed: %v", err)
		return
	}
	go f.backGroundFetchDataPartitions()
	return
}

func (f *FetchTopologyManager) AddVolume(name string) {
	f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	return
}

func (f *FetchTopologyManager) DeleteVolume(name string) {
	f.vols.Delete(name)
}

func (f *FetchTopologyManager) GetVolume(name string) (volumeTopo *VolumeTopologyInfo) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo = value.(*VolumeTopologyInfo)
	return
}

func (f *FetchTopologyManager) FetchDataPartitionView(volName string, dpID uint64) {
	select {
	case f.forceFetchDPViewCh <- &ForceFetchDataPartition{volumeName: volName, dataPartitionID: dpID}:
		log.LogDebugf("ForceFetchDataPartitionView volumeName: %s, dataPartitionID: %v", volName, dpID)
	default:
		log.LogDebugf("ForceFetchDataPartitionView dropInfo, volumeName: %s, dataPartitionID: %v", volName, dpID)
	}
}

//获取缓存中的partition视图信息
func (f *FetchTopologyManager) GetPartitionFromCache(volName string, dpID uint64) *DataPartition {
	topoInfoValue, ok := f.vols.Load(volName)
	if !ok {
		return nil
	}

	topoInfo := topoInfoValue.(*VolumeTopologyInfo)
	var dataPartitionViewValue interface{}
	dataPartitionViewValue, ok = topoInfo.dataPartitionsView.Load(dpID)
	if !ok {
		return nil
	}
	return dataPartitionViewValue.(*DataPartition)
}

//缓存中partition的视图信息不存在立即通过接口从master获取一次
func (f *FetchTopologyManager) GetPartition(volName string, dpID uint64) (dataPartition *DataPartition, err error) {
	dataPartition = f.GetPartitionFromCache(volName, dpID)
	if dataPartition != nil {
		return
	}

	return f.GetPartitionFromMaster(volName, dpID)
}

//调用master接口立即获取一次partition的信息,仅给data node使用
func (f *FetchTopologyManager) GetPartitionFromMaster(volName string, dpID uint64) (dataPartition *DataPartition, err error) {
	_ = f.limiter.Wait(context.Background(), rateLimitProperties)
	var dataPartitionInfo *proto.DataPartitionInfo
	dataPartitionInfo, err = f.masterDomainClient.AdminAPI().GetDataPartition(volName, dpID)
	if err != nil {
		return
	}
	dataPartition = &DataPartition{
		PartitionID:     dataPartitionInfo.PartitionID,
		Hosts:           dataPartitionInfo.Hosts,
	}

	value, _ := f.vols.LoadOrStore(volName, NewVolumeTopologyInfo(volName))
	volTopologyInfo := value.(*VolumeTopologyInfo)
	volTopologyInfo.updateDataPartitionsView([]*DataPartition{dataPartition})
	return
}

//调用master接口立即获取一次partition raft peer的信息,仅给data node使用
func (f *FetchTopologyManager) GetPartitionRaftPeerFromMaster(volName string, dpID uint64) (peers []proto.Peer, err error) {
	_ = f.limiter.Wait(context.Background(), rateLimitProperties)
	var dataPartitionInfo *proto.DataPartitionInfo
	dataPartitionInfo, err = f.masterDomainClient.AdminAPI().GetDataPartition(volName, dpID)
	if err != nil {
		return
	}
	dataPartition := &DataPartition{
		PartitionID:     dataPartitionInfo.PartitionID,
		Hosts:           dataPartitionInfo.Hosts,
	}

	value, _ := f.vols.LoadOrStore(volName, NewVolumeTopologyInfo(volName))
	volTopologyInfo := value.(*VolumeTopologyInfo)
	volTopologyInfo.updateDataPartitionsView([]*DataPartition{dataPartition})
	peers = dataPartitionInfo.Peers
	return
}

func (f *FetchTopologyManager) GetVolConf(name string) *VolumeConfig {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	return volumeTopo.config
}

func (f *FetchTopologyManager) GetBatchDelInodeCntConf(name string) (batchDelInodeCnt uint64) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		batchDelInodeCnt = 0
	} else {
		batchDelInodeCnt = uint64(volumeTopo.config.GetBatchDelInodeCount())
	}
	return
}

func (f *FetchTopologyManager) GetDelInodeIntervalConf(name string) (interval uint64) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		interval = 0
	} else {
		interval = uint64(volumeTopo.config.GetDelInodeInterval())
	}
	return
}

func (f *FetchTopologyManager) GetBitMapAllocatorEnableFlag(name string) (enableState bool, err error) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		err = fmt.Errorf("get vol(%s) enableBitMapAllocator flag failed", name)
	} else {
		enableState = volumeTopo.config.GetEnableBitMapFlag()
	}
	return
}

func (f *FetchTopologyManager) GetCleanTrashItemMaxDurationEachTimeConf(name string) (durationTime int32) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		durationTime = 0
	} else {
		durationTime = volumeTopo.config.GetCleanTrashItemMaxDuration()
	}

	return
}

func (f *FetchTopologyManager) GetCleanTrashItemMaxCountEachTimeConf(name string) (maxCount int32) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		maxCount = 0
	} else {
		maxCount = volumeTopo.config.GetCleanTrashItemMaxCount()
	}

	return
}

func (f *FetchTopologyManager) GetTruncateEKCountConf(name string) (count int) {
	value, _ := f.vols.LoadOrStore(name, NewVolumeTopologyInfo(name))
	volumeTopo := value.(*VolumeTopologyInfo)
	if volumeTopo.config == nil {
		count = 0
	} else {
		count = volumeTopo.config.truncateEKCount
	}
	return
}

func (f *FetchTopologyManager) Stop() {
	close(f.stopCh)
}

func (f *FetchTopologyManager) getAllVolumesName() []string {
	allVolsName := make([]string, 0)
	f.vols.Range(func(key, value interface{}) bool {
		allVolsName = append(allVolsName, key.(string))
		return true
	})
	return allVolsName
}

func (f *FetchTopologyManager) updateDataPartitions(volName string, dpIDs []uint64) {
	partitionsInfo, err := f.fetchDataPartitionsView(volName, dpIDs)
	if err != nil {
		log.LogErrorf("fetch vol(%s) data partition(%v) view failed: %v", volName, dpIDs, err)
		return
	}
	partitions := make([]*DataPartition, 0, len(partitionsInfo.DataPartitions))
	for _, item := range partitionsInfo.DataPartitions {
		partitions = append(partitions, &DataPartition{
			PartitionID:     item.PartitionID,
			Hosts:           item.Hosts,
			EcHosts:         item.EcHosts,
			EcMigrateStatus: item.EcMigrateStatus,
		})
	}
	value, _ := f.vols.LoadOrStore(volName, NewVolumeTopologyInfo(volName))
	volTopologyInfo := value.(*VolumeTopologyInfo)
	volTopologyInfo.updateDataPartitionsView(partitions)
}

func (f *FetchTopologyManager) backGroundFetchDataPartitions() {
	timer := time.NewTimer(0)
	ticker := time.NewTicker(f.forceFetchTimerInterval)
	var needForceFetchDataPartitionsMap = make(map[string]map[uint64]bool, 0)
	for {

		select {
		case <-f.stopCh:
			timer.Stop()
			ticker.Stop()
			return

		case <-timer.C:
			timer.Reset(intervalFetchDataPartitionView)
			if !f.needFetchVolAllDPView {
				continue
			}
			allVolsName := f.getAllVolumesName()
			for _, volName := range allVolsName {
				f.updateDataPartitions(volName, nil)
			}

		case <-ticker.C:
			var forceFetchInfo = make(BatchFetchDataPartitionsMap, 0)
			for volName, dpsIDMap := range needForceFetchDataPartitionsMap {
				dpsID := make([]uint64, 0, len(dpsIDMap))
				for dpID := range dpsIDMap {
					dpsID = append(dpsID, dpID)
				}
				forceFetchInfo[volName] = dpsID
			}

			for volName, dpsID := range forceFetchInfo {
				log.LogDebugf("backGroundFetchDataPartitions start force fetch volume(%s) dpIDs(%v) view", volName, dpsID)
				f.updateDataPartitions(volName, dpsID)
			}
			needForceFetchDataPartitionsMap = make(map[string]map[uint64]bool, 0)

		case fetchInfo := <-f.forceFetchDPViewCh:
			if _, ok := needForceFetchDataPartitionsMap[fetchInfo.volumeName]; !ok {
				needForceFetchDataPartitionsMap[fetchInfo.volumeName] = make(map[uint64]bool, 0)
			}
			needForceFetchDataPartitionsMap[fetchInfo.volumeName][fetchInfo.dataPartitionID] = true
		}
	}
}

func (f *FetchTopologyManager) updateVolumeConf() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("updateVolumeConf panic: err(%v) stack(%v)", r, string(debug.Stack()))
		}
	}()
	var volsConf []*proto.VolInfo

	volsConf, err = f.getVolsConf()
	if err != nil {
		log.LogErrorf("updateVolumeConf, err: %v", err)
		return
	}
	if len(volsConf) == 0 {
		return
	}

	for _, volConf := range volsConf {
		value, ok := f.vols.Load(volConf.Name)
		if !ok {
			continue
		}
		volTopo := value.(*VolumeTopologyInfo)
		volTopo.updateVolConf(&VolumeConfig{
			trashDay:                          int32(volConf.TrashRemainingDays),
			childFileMaxCnt:                   volConf.ChildFileMaxCnt,
			trashCleanInterval:                volConf.TrashCleanInterval,
			batchDelInodeCnt:                  volConf.BatchInodeDelCnt,
			delInodeInterval:                  volConf.DelInodeInterval,
			enableBitMapAllocator:             volConf.EnableBitMapAllocator,
			cleanTrashItemMaxDurationEachTime: volConf.CleanTrashMaxDurationEachTime,
			cleanTrashItemMaxCountEachTime:    volConf.CleanTrashMaxCountEachTime,
			enableRemoveDupReq:                volConf.EnableRemoveDupReq,
			truncateEKCount:                   volConf.TruncateEKCountEveryTime,
		})
		log.LogDebugf("updateVolConf: vol: %v, remaining days: %v, childFileMaxCount: %v, trashCleanInterval: %v, " +
			"enableBitMapAllocator: %v, trashCleanMaxDurationEachTime: %v, cleanTrashItemMaxCountEachTime: %v," +
			" enableRemoveDupReq:%v, batchInodeDelCnt: %v, delInodeInterval: %v, truncateEKCountEveryTime: %v",
			volConf.Name, volConf.TrashRemainingDays, volConf.ChildFileMaxCnt, volConf.TrashCleanInterval,
			strconv.FormatBool(volConf.EnableBitMapAllocator), volConf.CleanTrashMaxDurationEachTime,
			volConf.CleanTrashMaxCountEachTime, strconv.FormatBool(volConf.EnableRemoveDupReq), volConf.BatchInodeDelCnt,
			volConf.DelInodeInterval, volConf.TruncateEKCountEveryTime)
	}
	return
}

func (f *FetchTopologyManager) updateVolumeConfSchedule() (err error) {
	if !f.needUpdateVolsConf {
		return
	}
	for retryCount := 10; retryCount > 0; retryCount-- {
		err = f.updateVolumeConf()
		if err == nil {
			break
		}
		time.Sleep(sleepForUpdateVolConf)
	}
	if err != nil {
		log.LogWarnf("updateVolsConfScheduler, err: %v", err.Error())
		return
	}

	go func() {
		ticker := time.NewTicker(intervalToUpdateVolConf)
		for {
			select {
			case <-f.stopCh:
				ticker.Stop()
				return
			case <-ticker.C:
				_ = f.updateVolumeConf()
			}
		}
	}()
	return
}

func (f *FetchTopologyManager) fetchDataPartitionsView(volumeName string, dpsID []uint64) (dataPartitions *proto.DataPartitionsView, err error) {
	if f.masterDomainClient == nil {
		return f.masterClient.ClientAPI().GetDataPartitions(volumeName, dpsID)
	}

	masterDomainRequestErrorCount := atomic.LoadUint64(&f.masterDomainRequestErrorCount)
	if masterDomainRequestErrorCount > defPostByDomainMaxErrorCount && masterDomainRequestErrorCount%100 != 0 {
		atomic.AddUint64(&f.masterDomainRequestErrorCount, 1)
		return f.masterClient.ClientAPI().GetDataPartitions(volumeName, dpsID)
	}

	dataPartitions, err = f.masterDomainClient.ClientAPI().GetDataPartitions(volumeName, dpsID)
	if err != nil {
		atomic.AddUint64(&f.masterDomainRequestErrorCount, 1)
		return
	}
	atomic.StoreUint64(&f.masterDomainRequestErrorCount, 0)
	return
}

func (f *FetchTopologyManager) getVolsConf() (volsConf []*proto.VolInfo, err error) {
	if f.masterDomainClient == nil {
		return f.masterClient.AdminAPI().ListVols("")
	}

	domainRequestErrorCount := atomic.LoadUint64(&f.masterDomainRequestErrorCount)
	if domainRequestErrorCount > defPostByDomainMaxErrorCount && domainRequestErrorCount%100 != 0 {
		atomic.AddUint64(&f.masterDomainRequestErrorCount, 1)
		return f.masterClient.AdminAPI().ListVols("")
	}

	volsConf, err = f.masterDomainClient.AdminAPI().ListVols("")
	if err != nil {
		atomic.AddUint64(&f.masterDomainRequestErrorCount, 1)
		return
	}
	atomic.StoreUint64(&f.masterDomainRequestErrorCount, 0)
	return
}
