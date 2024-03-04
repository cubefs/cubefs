package metanode

import (
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	UpdateNodeInfoTicket     = 1 * time.Minute
	DefaultDeleteBatchCounts = 128

	DefaultRocksDBModeMaxFsUsedPercent   = 60
	DefaultMemModeMaxFsUsedFactorPercent = 80
	DefaultDumpWaterLevel                = 100
)

type NodeInfo struct {
	deleteBatchCount uint64
	dumpWaterLevel   uint64
}

var (
	nodeInfo                   = &NodeInfo{}
	nodeInfoStopC              = make(chan struct{}, 0)
	deleteWorkerSleepMs uint64 = 0
	dirChildrenNumLimit uint32 = proto.DefaultDirChildrenNumLimit

	RocksDBModeMaxFsUsedPercent uint64 = DefaultRocksDBModeMaxFsUsedPercent
	MemModeMaxFsUsedPercent     uint64 = DefaultMemModeMaxFsUsedFactorPercent
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

func updateDeleteWorkerSleepMs(val uint64) {
	atomic.StoreUint64(&deleteWorkerSleepMs, val)
}

func updateDirChildrenNumLimit(val uint32) {
	atomic.StoreUint32(&dirChildrenNumLimit, val)
}

func getDeleteWorkerSleepMs() (duration time.Duration) {
	val := atomic.LoadUint64(&deleteWorkerSleepMs)
	duration = time.Millisecond * time.Duration(val)
	const minDuration = 10 * time.Second
	if duration < minDuration {
		duration = minDuration
	}
	return
}

func DeleteWorkerSleepMs() {
	val := atomic.LoadUint64(&deleteWorkerSleepMs)
	if val > 0 {
		time.Sleep(time.Duration(val) * time.Millisecond)
	}
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

func GetDumpWaterLevel() uint64 {
	val := atomic.LoadUint64(&nodeInfo.dumpWaterLevel)
	if val < DefaultDumpWaterLevel {
		val = DefaultDumpWaterLevel
	}
	return val
}

func (m *MetaNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	defer ticker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo gorutine stopped")
			return
		case <-ticker.C:
			m.updateNodeInfo()
			m.metadataManager.checkVolVerList()
		}
	}
}

func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *MetaNode) updateNodeInfo() {
	// clusterInfo, err := getClusterInfo()
	clusterInfo, err := masterClient.AdminAPI().GetClusterInfo()
	if err != nil {
		log.LogErrorf("[updateNodeInfo] %s", err.Error())
		return
	}
	updateDeleteBatchCount(clusterInfo.MetaNodeDeleteBatchCount)
	updateDeleteWorkerSleepMs(clusterInfo.MetaNodeDeleteWorkerSleepMs)

	if clusterInfo.DirChildrenNumLimit < proto.MinDirChildrenNumLimit {
		log.LogWarnf("updateNodeInfo: DirChildrenNumLimit probably not enabled on master, set to default value(%v)",
			proto.DefaultDirChildrenNumLimit)
		atomic.StoreUint32(&dirChildrenNumLimit, proto.DefaultDirChildrenNumLimit)
	} else {
		atomic.StoreUint32(&dirChildrenNumLimit, clusterInfo.DirChildrenNumLimit)
		log.LogInfof("updateNodeInfo: DirChildrenNumLimit(%v)", clusterInfo.DirChildrenNumLimit)
	}

	// updateDirChildrenNumLimit(clusterInfo.DirChildrenNumLimit)
}
