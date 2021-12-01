package metanode

import (
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"golang.org/x/time/rate"
)

const (
	UpdateDeleteLimitInfoTicket = 1 * time.Minute
	UpdateRateLimitInfoTicket   = 5 * time.Minute
	UpdateClusterViewTicket     = 24 * time.Hour
	DefaultDeleteBatchCounts    = 128
	DefaultReqLimitBurst        = 512
)

type NodeInfo struct {
	deleteBatchCount 	uint64
	readDirLimitNum		uint64
}

var (
	nodeInfo                   = &NodeInfo{}
	nodeInfoStopC              = make(chan struct{}, 0)
	deleteWorkerSleepMs uint64 = 0

	// request rate limiter for entire meta node
	reqRateLimit   uint64
	reqRateLimiter = rate.NewLimiter(rate.Inf, DefaultReqLimitBurst)

	// map[opcode]*rate.Limiter, request rate limiter for opcode
	reqOpRateLimitMap   = make(map[uint8]uint64)
	reqOpRateLimiterMap = make(map[uint8]*rate.Limiter)

	isRateLimitOn bool

	// all cluster internal nodes
	clusterMap     = make(map[string]bool)
	limitInfo      *proto.LimitInfo
	limitOpcodeMap = map[uint8]bool{
		proto.OpMetaCreateInode:     true,
		proto.OpMetaInodeGet:        true,
		proto.OpMetaCreateDentry:    true,
		proto.OpMetaExtentsAdd:      true,
		proto.OpMetaBatchExtentsAdd: true,
		proto.OpMetaExtentsList:     true,
		proto.OpMetaInodeGetV2:      true,
	}
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

func updateReadDirLimitNum(val uint64)  {
	if val > 0 {
		atomic.StoreUint64(&nodeInfo.readDirLimitNum, val)
	}
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
	deleteTicker := time.NewTicker(UpdateDeleteLimitInfoTicket)
	rateLimitTicker := time.NewTicker(UpdateRateLimitInfoTicket)
	clusterViewTicker := time.NewTicker(UpdateClusterViewTicket)
	defer func() {
		deleteTicker.Stop()
		rateLimitTicker.Stop()
		clusterViewTicker.Stop()
	}()

	// call once on init before first tick
	m.updateClusterMap()
	m.updateDeleteLimitInfo()
	m.updateRateLimitInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo gorutine stopped")
			return
		case <-deleteTicker.C:
			m.updateDeleteLimitInfo()
		case <-rateLimitTicker.C:
			m.updateRateLimitInfo()
		case <-clusterViewTicker.C:
			m.updateClusterMap()
		}
	}
}

func (m *MetaNode) enableRocksDbDelExtent() bool{
	if atomic.LoadUint32(&m.clusterMetaVersion) >= MetaNodeVersion01 {
		return true
	}

	return false
}

func (m *MetaNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *MetaNode) updateDeleteLimitInfo() {
	info, err := masterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogErrorf("[updateDeleteLimitInfo] %s", err.Error())
		return
	}

	limitInfo = info
	updateDeleteBatchCount(limitInfo.MetaNodeDeleteBatchCount)
	updateDeleteWorkerSleepMs(limitInfo.MetaNodeDeleteWorkerSleepMs)
	updateReadDirLimitNum(limitInfo.MetaNodeReadDirLimitNum)
	atomic.StoreUint32(&m.clusterMetaVersion, info.MetaVersionRequirements)
}

func (m *MetaNode) updateRateLimitInfo() {
	if limitInfo == nil {
		return
	}

	var (
		r                   uint64
		l                   rate.Limit
		ok                  bool
		tmpOpRateLimiterMap map[uint8]*rate.Limiter
	)

	// update request rate limiter for entire meta node
	reqRateLimit = limitInfo.MetaNodeReqRateLimit
	l = rate.Limit(reqRateLimit)
	if reqRateLimit == 0 {
		l = rate.Inf
	}
	reqRateLimiter.SetLimit(l)

	// update request rate limiter for opcode
	if reflect.DeepEqual(reqOpRateLimitMap, limitInfo.MetaNodeReqOpRateLimitMap) {
		isRateLimitOn = (reqRateLimit > 0 ||
			len(reqOpRateLimitMap) > 0)
		return
	}
	reqOpRateLimitMap = limitInfo.MetaNodeReqOpRateLimitMap
	tmpOpRateLimiterMap = make(map[uint8]*rate.Limiter)
	for op, _ := range limitOpcodeMap {
		r, ok = reqOpRateLimitMap[op]
		if !ok {
			r, ok = reqOpRateLimitMap[0]
		}
		if !ok {
			continue
		}
		tmpOpRateLimiterMap[op] = rate.NewLimiter(rate.Limit(r), DefaultReqLimitBurst)
	}
	reqOpRateLimiterMap = tmpOpRateLimiterMap

	isRateLimitOn = (reqRateLimit > 0 ||
		len(reqOpRateLimitMap) > 0)
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
