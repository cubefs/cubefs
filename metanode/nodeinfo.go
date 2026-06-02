package metanode

import (
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	// UpdateNodeInfoTicket defines the interval for updating node information
	UpdateNodeInfoTicket = 1 * time.Minute

	// DefaultDeleteBatchCounts defines the default batch count for delete operations
	DefaultDeleteBatchCounts = 128

	// DefaultDumpWaterLevel defines the default dump water level threshold
	DefaultDumpWaterLevel = 100

	// DefaultFollowerReadLeaseTime defines the default follower read lease time in seconds (fallback before cluster info is loaded).
	DefaultFollowerReadLeaseTime = proto.DefaultFollowerReadLeaseTimeSec
)

// NodeInfo holds configuration information for the meta node
type NodeInfo struct {
	deleteBatchCount      uint64
	dumpWaterLevel        uint64
	followerReadLeaseTime uint64
}

var (
	// Global node information instance
	nodeInfo = &NodeInfo{}

	// Channel to signal stopping the node info update goroutine
	nodeInfoStopC = make(chan struct{})

	// Delete worker sleep time in milliseconds
	deleteWorkerSleepMs uint64 = 0

	// Directory children number limit
	dirChildrenNumLimit uint32 = proto.DefaultDirChildrenNumLimit
)

// DeleteBatchCount returns the current delete batch count
func DeleteBatchCount() uint64 {
	val := atomic.LoadUint64(&nodeInfo.deleteBatchCount)
	if val == 0 {
		return DefaultDeleteBatchCounts
	}
	return val
}

// updateDeleteBatchCount updates the delete batch count atomically
func updateDeleteBatchCount(val uint64) {
	atomic.StoreUint64(&nodeInfo.deleteBatchCount, val)
}

// updateDeleteWorkerSleepMs updates the delete worker sleep time atomically
func updateDeleteWorkerSleepMs(val uint64) {
	atomic.StoreUint64(&deleteWorkerSleepMs, val)
}

// DeleteWorkerSleepMs sleeps for the configured duration if sleep time is set
func DeleteWorkerSleepMs() {
	val := atomic.LoadUint64(&deleteWorkerSleepMs)
	if val > 0 {
		time.Sleep(time.Duration(val) * time.Millisecond)
	}
}

// startUpdateNodeInfo starts the node information update goroutine
func (m *MetaNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	defer ticker.Stop()

	log.LogInfo("metanode nodeinfo goroutine started")

	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo goroutine stopped")
			return
		case <-ticker.C:
			if err := m.updateNodeInfo(); err != nil {
				log.LogErrorf("failed to update node info: %v", err)
			}

			// Check volume version list if cluster snapshot is enabled
			if m.clusterEnableSnapshot {
				m.metadataManager.checkVolVerList()
			}
		}
	}
}

// stopUpdateNodeInfo stops the node information update goroutine
func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

// updateNodeInfo updates the node information from cluster configuration
func (m *MetaNode) updateNodeInfo() error {
	clusterInfo, err := masterClient.AdminAPI().GetClusterInfo()
	if err != nil {
		return err
	}

	// Update delete batch count
	updateDeleteBatchCount(clusterInfo.MetaNodeDeleteBatchCount)

	// Update delete worker sleep time
	updateDeleteWorkerSleepMs(clusterInfo.MetaNodeDeleteWorkerSleepMs)

	// Update follower read lease time
	updateFollowerReadLeaseTime(clusterInfo.FollowerReadLeaseTime)

	// Update directory children number limit with validation
	if err := m.updateDirChildrenNumLimit(clusterInfo.DirChildrenNumLimit); err != nil {
		log.LogWarnf("failed to update DirChildrenNumLimit: %v", err)
	}

	return nil
}

// updateDirChildrenNumLimit updates the directory children number limit with validation
func (m *MetaNode) updateDirChildrenNumLimit(limit uint32) error {
	if limit < proto.MinDirChildrenNumLimit {
		log.LogWarnf("DirChildrenNumLimit(%v) is below minimum(%v), using default value(%v)",
			limit, proto.MinDirChildrenNumLimit, proto.DefaultDirChildrenNumLimit)
		atomic.StoreUint32(&dirChildrenNumLimit, proto.DefaultDirChildrenNumLimit)
		return nil
	}

	atomic.StoreUint32(&dirChildrenNumLimit, limit)
	log.LogInfof("DirChildrenNumLimit updated to %v", limit)
	return nil
}

// GetDumpWaterLevel returns the current dump water level with minimum threshold
func GetDumpWaterLevel() uint64 {
	val := atomic.LoadUint64(&nodeInfo.dumpWaterLevel)
	if val < DefaultDumpWaterLevel {
		return DefaultDumpWaterLevel
	}
	return val
}

// GetDirChildrenNumLimit returns the current directory children number limit
func GetDirChildrenNumLimit() uint32 {
	return atomic.LoadUint32(&dirChildrenNumLimit)
}

// SetDumpWaterLevel sets the dump water level atomically
func SetDumpWaterLevel(level uint64) {
	atomic.StoreUint64(&nodeInfo.dumpWaterLevel, level)
}

// FollowerReadLeaseTime returns the current follower read lease time in seconds
func FollowerReadLeaseTime() uint64 {
	val := atomic.LoadUint64(&nodeInfo.followerReadLeaseTime)
	if val == 0 {
		return DefaultFollowerReadLeaseTime
	}
	return val
}

// updateFollowerReadLeaseTime updates the follower read lease time atomically
func updateFollowerReadLeaseTime(val uint64) {
	if val < proto.MinFollowerReadLeaseTimeSec {
		log.LogWarnf("FollowerReadLeaseTime %d from cluster below min %d, capping", val, proto.MinFollowerReadLeaseTimeSec)
		val = proto.MinFollowerReadLeaseTimeSec
	}
	if val > proto.MaxFollowerReadLeaseTimeSec {
		log.LogWarnf("FollowerReadLeaseTime %d from cluster exceeds max %d, capping", val, proto.MaxFollowerReadLeaseTimeSec)
		val = proto.MaxFollowerReadLeaseTimeSec
	}
	atomic.StoreUint64(&nodeInfo.followerReadLeaseTime, val)
}
