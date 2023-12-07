package metanode

import (
	"github.com/cubefs/cubefs/util/exporter"
	"golang.org/x/time/rate"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/unit"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
)

const (
	UpdateDeleteLimitInfoTicket          = 1 * time.Minute
	UpdateRateLimitInfoTicket            = 5 * time.Minute
	UpdateClusterViewTicket              = 24 * time.Hour
	DefaultDeleteBatchCounts             = 128
	DefaultReqLimitBurst                 = 512
	DefaultDumpWaterLevel                = 100
	DefaultRocksDBModeMaxFsUsedPercent   = 60
	DefaultMemModeMaxFsUsedFactorPercent = 80
	DefaultDelEKLimitBurst               = 10 * 10000 //10w todo:待定
	DefaultDelEKBatchCount 			     = 1000
	DefaultTruncateEKCount               = 128
)

type NodeInfo struct {
	deleteBatchCount uint64
	readDirLimitNum  uint64
	dumpWaterLevel   uint64
	logMaxSize       uint64
	reservedSpace    uint64

	rocksWalFileSize      uint64 //MB
	rocksWalMemSize       uint64 //MB
	rocksLogSize          uint64 //MB
	rocksLogReservedTime  uint64 //day
	rocksLogReservedCnt   uint64
	rocksFlushWalInterval uint64 // min default 30min
	rocksFlushWal         bool   // default true flush
	rocksWalTTL           uint64 //second default 60
	trashCleanInterval    uint64 //min
	delEKFileLocalMaxMB   uint64
	raftLogSizeFromMaster int
	raftLogCapFromMaster  int
	raftLogSizeFromLoc    int
	raftLogCapFromLoc     int

	bitMapAllocatorMaxUsedFactorForAvailable float64
	bitMapAllocatorMinFreeFactorForAvailable float64

	CleanTrashItemMaxDurationEachTime int32 //min
	CleanTrashItemMaxCountEachTime    int32

	DumpSnapCountCluster      uint64
	DumpSnapCountLoc  	      uint64
}

var (
	nodeInfo                   = &NodeInfo{}
	nodeInfoStopC              = make(chan struct{}, 0)
	deleteWorkerSleepMs uint64 = 0

	delExtentRateLimitLocal uint64
	delExtentRateLimiterLocal = rate.NewLimiter(rate.Inf, DefaultDelEKLimitBurst)

	// all cluster internal nodes
	clusterMap = make(map[string]bool)
	limitInfo  *proto.LimitInfo

	RocksDBModeMaxFsUsedPercent uint64 = DefaultRocksDBModeMaxFsUsedPercent
	MemModeMaxFsUsedPercent     uint64 = DefaultMemModeMaxFsUsedFactorPercent

	enableRemoveDupReq = defaultEnableRemoveDupReq
)

func DeleteBatchCount() uint64 {
	val := atomic.LoadUint64(&nodeInfo.deleteBatchCount)
	if val == 0 {
		val = DefaultDeleteBatchCounts
	}
	return val
}

func updateDeleteBatchCount(val uint64) {
	atomic.StoreUint64(&nodeInfo.deleteBatchCount, val)
}

func ReadDirLimitNum() uint64 {
	val := atomic.LoadUint64(&nodeInfo.readDirLimitNum)
	return val
}

func updateReadDirLimitNum(val uint64) {
	atomic.StoreUint64(&nodeInfo.readDirLimitNum, val)
}

func updateDeleteWorkerSleepMs(val uint64) {
	atomic.StoreUint64(&deleteWorkerSleepMs, val)
}

func DeleteWorkerSleepMs() {
	val := atomic.LoadUint64(&deleteWorkerSleepMs)
	if val > 0 {
		time.Sleep(time.Duration(val) * time.Millisecond)
	}
}

func GetDumpWaterLevel() uint64 {
	val := atomic.LoadUint64(&nodeInfo.dumpWaterLevel)
	if val < DefaultDumpWaterLevel {
		val = DefaultDumpWaterLevel
	}
	return val
}

func updateDumpWaterLevel(val uint64) {
	atomic.StoreUint64(&nodeInfo.dumpWaterLevel, val)
}

func updateRocksDBModeMaxFsUsedPercent(val float32) {
	if val <= 0 || val >= 1 {
		return
	}
	atomic.StoreUint64(&RocksDBModeMaxFsUsedPercent, uint64(val*100))
}

func getRocksDBModeMaxFsUsedPercent() uint64 {
	return atomic.LoadUint64(&RocksDBModeMaxFsUsedPercent)
}

func updateMemModeMaxFsUsedPercent(val float32) {
	if val <= 0 || val >= 1 {
		return
	}
	atomic.StoreUint64(&MemModeMaxFsUsedPercent, uint64(val*100))
}

func getMemModeMaxFsUsedPercent() uint64 {
	return atomic.LoadUint64(&MemModeMaxFsUsedPercent)
}

func updateLogMaxSize(val uint64) {
	if val != 0 && val != nodeInfo.logMaxSize {
		oldLogMaxSize := nodeInfo.logMaxSize
		nodeInfo.logMaxSize = val
		log.SetLogMaxSize(int64(nodeInfo.logMaxSize))
		log.LogInfof("[updateLogMaxSize] log max MB(old:%v, new:%v)", oldLogMaxSize, nodeInfo.logMaxSize)
	}
}

func (m *MetaNode) updateRocksDBDiskReservedSpaceSpace(val uint64) {
	if val != nodeInfo.reservedSpace && val != 0 {
		nodeInfo.reservedSpace = val
		for _, disk := range m.getDisks() {
			disk.UpdateReversedSpace(val)
		}
	}
}

func (m *MetaNode) updateRocksDBConf(info *proto.LimitInfo) {
	if info.MetaRockDBWalFileSize != 0 && nodeInfo.rocksWalFileSize != info.MetaRockDBWalFileSize {
		nodeInfo.rocksWalFileSize = info.MetaRockDBWalFileSize
	}

	if info.MetaRocksWalMemSize != 0 && nodeInfo.rocksWalMemSize != info.MetaRocksWalMemSize {
		nodeInfo.rocksWalMemSize = info.MetaRocksWalMemSize
	}

	if info.MetaRocksLogSize != 0 && nodeInfo.rocksLogSize != info.MetaRocksLogSize {
		nodeInfo.rocksLogSize = info.MetaRocksLogSize
	}

	if info.MetaRocksLogReservedTime != 0 && nodeInfo.rocksLogReservedTime != info.MetaRocksLogReservedTime {
		nodeInfo.rocksLogReservedTime = info.MetaRocksLogReservedTime
	}

	if info.MetaRocksLogReservedCnt != 0 && nodeInfo.rocksLogReservedCnt != info.MetaRocksLogReservedCnt {
		nodeInfo.rocksLogReservedCnt = info.MetaRocksLogReservedCnt
	}

	if info.MetaRocksWalTTL != 0 && nodeInfo.rocksWalTTL != info.MetaRocksWalTTL {
		nodeInfo.rocksWalTTL = info.MetaRocksWalTTL
	}

	if info.MetaRocksFlushWalInterval != 0 && nodeInfo.rocksFlushWalInterval != info.MetaRocksFlushWalInterval {
		nodeInfo.rocksFlushWalInterval = info.MetaRocksFlushWalInterval
	}

	if info.MetaRocksDisableFlushFlag != 0 {
		nodeInfo.rocksFlushWal = false
	}

	if info.MetaRocksDisableFlushFlag == 0 {
		nodeInfo.rocksFlushWal = true
	}
}

func (m *MetaNode) updateDeleteEKRecordFilesMaxSize(maxMB uint64) {
	if maxMB == 0 || DeleteEKRecordFilesMaxTotalSize.Load() == maxMB*unit.MB {
		log.LogDebugf("[updateDeleteEKRecordFilesMaxSize] no need update")
		return
	}
	if atomic.LoadUint64(&nodeInfo.delEKFileLocalMaxMB) == 0 {
		DeleteEKRecordFilesMaxTotalSize.Store(maxMB * unit.MB)
		log.LogDebugf("[updateDeleteEKRecordFilesMaxSize] new value:%vMB", maxMB)
	}
}

func (m *MetaNode) updateTrashCleanConfig(limitInfo *proto.LimitInfo) {
	//clean interval
	if limitInfo.MetaTrashCleanInterval != 0 && limitInfo.MetaTrashCleanInterval != nodeInfo.trashCleanInterval {
		log.LogDebugf("[updateTrashCleanConfig] trash clean interval, old value:%v, new value:%v",
			nodeInfo.trashCleanInterval, limitInfo.MetaTrashCleanInterval)
		nodeInfo.trashCleanInterval = limitInfo.MetaTrashCleanInterval
	}

	//trash clean duration
	if limitInfo.TrashCleanDurationEachTime != nodeInfo.CleanTrashItemMaxDurationEachTime {
		log.LogDebugf("[updateTrashCleanConfig] trash clean duration, old value:%v, new value:%v",
			nodeInfo.CleanTrashItemMaxDurationEachTime, limitInfo.TrashCleanDurationEachTime)
		nodeInfo.CleanTrashItemMaxDurationEachTime = limitInfo.TrashCleanDurationEachTime
	}

	//trash clean max count
	if limitInfo.TrashItemCleanMaxCountEachTime != nodeInfo.CleanTrashItemMaxCountEachTime {
		log.LogDebugf("[updateTrashCleanConfig] trash clean max count, old value:%v, new value:%v",
			nodeInfo.CleanTrashItemMaxCountEachTime, limitInfo.TrashItemCleanMaxCountEachTime)
		nodeInfo.CleanTrashItemMaxCountEachTime = limitInfo.TrashItemCleanMaxCountEachTime
	}
	return
}

func (m *MetaNode) updateRaftParamFromMaster(logSize, logCap int) {
	if logSize >= 0 && logSize != nodeInfo.raftLogSizeFromMaster {
		nodeInfo.raftLogSizeFromMaster = logSize
	}

	if logCap >= 0 && logCap != nodeInfo.raftLogCapFromMaster {
		nodeInfo.raftLogCapFromMaster = logCap
	}

	return
}

func (m *MetaNode) updateRaftParamFromLocal(logSize, logCap int) {
	if logSize >= 0 && logSize != nodeInfo.raftLogSizeFromLoc {
		nodeInfo.raftLogSizeFromLoc = logSize
	}

	if logCap >= 0 && logCap != nodeInfo.raftLogCapFromLoc {
		nodeInfo.raftLogCapFromLoc = logCap
	}

	return
}

func (m *MetaNode) updateSyncWALOnUnstableEnableState(enableState bool) {
	if m.raftStore.IsSyncWALOnUnstable() == enableState {
		return
	}
	log.LogInfof("updateSyncWALOnUnstableFlag, enable sync WAL flag: %v -> %v", m.raftStore.IsSyncWALOnUnstable(), enableState)
	m.raftStore.SetSyncWALOnUnstable(enableState)
}

func (m *MetaNode) updateBitMapAllocatorConf(info *proto.LimitInfo) {
	if info.BitMapAllocatorMaxUsedFactor != 0 && nodeInfo.bitMapAllocatorMaxUsedFactorForAvailable != info.BitMapAllocatorMaxUsedFactor {
		nodeInfo.bitMapAllocatorMaxUsedFactorForAvailable = info.BitMapAllocatorMaxUsedFactor
	}

	if info.BitMapAllocatorMinFreeFactor != 0 && nodeInfo.bitMapAllocatorMinFreeFactorForAvailable != info.BitMapAllocatorMinFreeFactor {
		nodeInfo.bitMapAllocatorMinFreeFactorForAvailable = info.BitMapAllocatorMinFreeFactor
	}
}

func (m *MetaNode) getBitMapAllocatorMaxUsedFactor() float64 {
	factor := defBitMapAllocatorMaxUsedFactorForAvailable
	nodeConf := getGlobalConfNodeInfo()
	if nodeConf.bitMapAllocatorMaxUsedFactorForAvailable != 0 {
		factor = nodeConf.bitMapAllocatorMaxUsedFactorForAvailable
	}
	return factor
}

func (m *MetaNode) getBitMapAllocatorMinFreeFactor() float64 {
	factor := defBitMapAllocatorMinFreeFactorForAvailable
	nodeConf := getGlobalConfNodeInfo()
	if nodeConf.bitMapAllocatorMinFreeFactorForAvailable != 0 {
		factor = nodeConf.bitMapAllocatorMinFreeFactorForAvailable
	}
	return factor
}

func (m *MetaNode) setRequestRecordsReservedCount(cnt int32) {
	if cnt == 0 || cnt == reqRecordMaxCount.Load() {
		return
	}
	log.LogInfof("setRequestRecordsReservedCount, old cnt:%v, new cnt:%v", reqRecordMaxCount.Load(), cnt)
	reqRecordMaxCount.Store(cnt)
}

func (m *MetaNode) setRequestRecordsReservedMin(min int32) {
	if min == 0 || min == reqRecordReserveMin.Load() {
		return
	}
	log.LogInfof("setRequestRecordsReservedMin, old min:%v, new min:%v", reqRecordReserveMin.Load(), min)
	reqRecordReserveMin.Store(min)
}

func (m *MetaNode) setRemoveDupReqFlag(enableState bool) {
	if enableState == enableRemoveDupReq {
		return
	}
	log.LogInfof("setRemoveDupReqFlag, enableRemoveDupReq: %v ==> %v", enableRemoveDupReq, enableState)
	enableRemoveDupReq = enableState
}

func (m *MetaNode)GetDumpSnapCount() uint64 {
	dumpCount := uint64(0)
	dumpCount = atomic.LoadUint64(&nodeInfo.DumpSnapCountCluster)

	localCount := atomic.LoadUint64(&nodeInfo.DumpSnapCountLoc)
	if localCount != 0 {
		dumpCount = localCount
	}

	if dumpCount == 0 {
		dumpCount = defParallelismDumpCount
	}
	return dumpCount
}

func (m *MetaNode)GetDumpSnapRunningCount() uint64 {
	Count := uint64(0)
	if m.metadataManager != nil {
		Count = m.metadataManager.GetDumpSnapRunningCount()
	}
	return Count
}

func (m *MetaNode)GetDumpSnapMPID() []uint64 {
	if m.metadataManager != nil {
		return m.metadataManager.GetDumpSnapMPID()
	}
	return nil
}

func (m *MetaNode)SetDumpSnapCount() {
	Count := m.GetDumpSnapCount()
	if m.metadataManager != nil {
		m.metadataManager.ResetDumpSnapShotConfCount(Count)
	}
	return
}

func (m *MetaNode)updateDumpSnapCountCluster(info *proto.LimitInfo) {
	clusterCount, ok := info.MetaNodeDumpSnapCountByZone[m.zoneName]
	if !ok {
		clusterCount = 0
	}
	atomic.StoreUint64(&nodeInfo.DumpSnapCountCluster, clusterCount)
	m.SetDumpSnapCount()
	return
}

func (m *MetaNode)updateDumpSnapCountLoc(dumpCount uint64) {
	atomic.StoreUint64(&nodeInfo.DumpSnapCountLoc, dumpCount)
	m.SetDumpSnapCount()
	return
}

func getGlobalConfNodeInfo() *NodeInfo {
	newInfo := *nodeInfo
	return &newInfo
}

func (m *MetaNode) startUpdateNodeInfo() {
	deleteTicker := time.NewTicker(UpdateDeleteLimitInfoTicket)
	clusterViewTicker := time.NewTicker(UpdateClusterViewTicket)
	defer func() {
		deleteTicker.Stop()
		clusterViewTicker.Stop()
	}()

	// call once on init before first tick
	m.updateClusterMap()
	m.updateDeleteLimitInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo gorutine stopped")
			return
		case <-deleteTicker.C:
			m.updateDeleteLimitInfo()
		case <-clusterViewTicker.C:
			m.updateClusterMap()
		}
	}
}

func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *MetaNode) updateDeleteLimitInfo() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("update node info panic, recover: %v", r)
			exporter.WarningAppendKey(PanicBackGroundKey, "update node info panic")
		}
	}()
	info, err := masterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogErrorf("[updateDeleteLimitInfo] %s", err.Error())
		return
	}

	limitInfo = info
	updateDeleteBatchCount(limitInfo.MetaNodeDeleteBatchCount)
	updateDeleteWorkerSleepMs(limitInfo.MetaNodeDeleteWorkerSleepMs)
	updateReadDirLimitNum(limitInfo.MetaNodeReadDirLimitNum)
	updateDumpWaterLevel(limitInfo.MetaNodeDumpWaterLevel)
	updateMemModeMaxFsUsedPercent(limitInfo.MemModeRocksdbDiskUsageThreshold)
	updateRocksDBModeMaxFsUsedPercent(limitInfo.RocksdbDiskUsageThreshold)
	updateLogMaxSize(limitInfo.LogMaxSize)
	m.updateRocksDBDiskReservedSpaceSpace(limitInfo.RocksDBDiskReservedSpace)
	m.updateRocksDBConf(limitInfo)
	m.updateDeleteEKRecordFilesMaxSize(limitInfo.DeleteEKRecordFileMaxMB)
	m.updateTrashCleanConfig(limitInfo)
	m.updateRaftParamFromMaster(int(limitInfo.MetaRaftLogSize), int(limitInfo.MetaRaftCap))
	m.updateSyncWALOnUnstableEnableState(limitInfo.MetaSyncWALOnUnstableEnableState)
	m.updateBitMapAllocatorConf(limitInfo)
	m.setRequestRecordsReservedCount(limitInfo.ClientReqRecordsReservedCount)
	m.setRequestRecordsReservedMin(limitInfo.ClientReqRecordsReservedMin)
	m.setRemoveDupReqFlag(limitInfo.ClientReqRemoveDupFlag)
	m.updateDumpSnapCountCluster(limitInfo)

	if statistics.StatisticsModule != nil {
		statistics.StatisticsModule.UpdateMonitorSummaryTime(limitInfo.MonitorSummarySec)
		statistics.StatisticsModule.UpdateMonitorReportTime(limitInfo.MonitorReportSec)
	}
}

func (m *MetaNode) updateClusterMap() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("update node info panic, recover: %v", r)
			exporter.WarningAppendKey(PanicBackGroundKey, "update node info panic")
		}
	}()
	cv, err := masterClient.AdminAPI().GetCluster()
	if err != nil {
		return
	}
	addrMap := make(map[string]bool, len(clusterMap))
	var addrSlice []string
	for _, node := range cv.MetaNodes {
		addrSlice = strings.Split(node.Addr, ":")
		addrMap[addrSlice[0]] = true
	}
	for _, node := range cv.DataNodes {
		addrSlice = strings.Split(node.Addr, ":")
		addrMap[addrSlice[0]] = true
	}
	for _, master := range masterClient.Nodes() {
		addrSlice = strings.Split(master, ":")
		addrMap[addrSlice[0]] = true
	}
	clusterMap = addrMap
}
