package metanode

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
	"golang.org/x/time/rate"
)

const (
	UpdateNodeInfoTicket     = 1 * time.Minute
	UpdateClusterViewTicket  = 24 * time.Hour
	DefaultDeleteBatchCounts = 128
	DefaultReqLimitBurst     = 512
)

type NodeInfo struct {
	deleteBatchCount uint64
}

var (
	nodeInfo                   = &NodeInfo{}
	nodeInfoStopC              = make(chan struct{}, 0)
	deleteWorkerSleepMs uint64 = 0

	// request rate limit for entire data node
	reqRateLimit   uint64
	reqRateLimiter = rate.NewLimiter(rate.Inf, DefaultReqLimitBurst)

	// request rate limit for opcode
	reqOpRateLimitMap   = make(map[uint8]uint64)
	reqOpRateLimiterMap = make(map[uint8]*rate.Limiter)

	reqRateLimiterMapMutex sync.Mutex

	// all cluster internal nodes
	clusterMap = make(map[string]bool)
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

func DeleteWorkerSleepMs() {
	val := atomic.LoadUint64(&deleteWorkerSleepMs)
	if val > 0 {
		time.Sleep(time.Duration(val) * time.Millisecond)
	}
}

func (m *MetaNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	// call once on init before first tick
	m.updateClusterMap()
	clusterViewTicker := time.NewTicker(UpdateClusterViewTicket)
	defer ticker.Stop()
	defer clusterViewTicker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo gorutine stopped")
			return
		case <-ticker.C:
			m.updateNodeInfo()
		case <-clusterViewTicker.C:
			m.updateClusterMap()
		}
	}
}

func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *MetaNode) updateNodeInfo() {
	limitInfo, err := masterClient.AdminAPI().GetLimitInfo()
	if err != nil {
		log.LogErrorf("[updateNodeInfo] %s", err.Error())
		return
	}
	updateDeleteBatchCount(limitInfo.MetaNodeDeleteBatchCount)
	updateDeleteWorkerSleepMs(limitInfo.MetaNodeDeleteWorkerSleepMs)
	reqRateLimit = limitInfo.MetaNodeReqRateLimit
	l := rate.Limit(reqRateLimit)
	if reqRateLimit == 0 {
		l = rate.Inf
	}
	reqRateLimiter.SetLimit(l)

	reqRateLimiterMapMutex.Lock()
	reqOpRateLimitMap = limitInfo.MetaNodeReqOpRateLimitMap
	var deleteOp []uint8
	for op, limiter := range reqOpRateLimiterMap {
		r, ok := reqOpRateLimitMap[op]
		if !ok {
			r, ok = reqOpRateLimitMap[0]
		}
		if !ok {
			deleteOp = append(deleteOp, op)
			continue
		}
		limiter.SetLimit(rate.Limit(r))
	}
	for _, op := range deleteOp {
		delete(reqOpRateLimiterMap, op)
	}
	reqRateLimiterMapMutex.Unlock()
}

func (m *MetaNode) updateClusterMap() {
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
