package metanode

import (
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	UpdateNodeInfoTicket     = 1 * time.Minute
	DefaultDeleteBatchCounts = 128
)

type NodeInfo struct {
	deleteBatchCount uint64
}

var (
	nodeInfo      = &NodeInfo{}
	nodeInfoStopC = make(chan struct{}, 0)
)

func DeleteBatchCount() uint64 {
	val := atomic.LoadUint64(&nodeInfo.deleteBatchCount)
	if val == 0 {
		val = DefaultDeleteBatchCounts
	}
	return val
}

func SetDeleteBatchCount(val uint64) {
	atomic.StoreUint64(&nodeInfo.deleteBatchCount, val)
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
		}
	}
}

func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *MetaNode) updateNodeInfo() {
	//clusterInfo, err := getClusterInfo()
	clusterInfo, err := masterClient.AdminAPI().GetClusterInfo()
	if err != nil {
		log.LogErrorf("[updateNodeInfo] %s", err.Error())
		return
	}
	SetDeleteBatchCount(clusterInfo.MetaNodeDeleteBatchCount)
}
