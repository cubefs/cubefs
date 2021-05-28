package datanode

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
	"golang.org/x/time/rate"
)

const (
	defaultMarkDeleteLimitRate            = rate.Inf
	defaultMarkDeleteLimitBurst           = 512
	UpdateNodeInfoTicket                  = 1 * time.Minute
	UpdateClusterViewTicket               = 24 * time.Hour
	defaultFixTinyDeleteRecordLimitOnDisk = 50
	defaultRepairTaskLimitOnDisk          = 10
	defaultReqLimitBurst                  = 512
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)

	// request rate limit for entire data node
	reqRateLimit   uint64
	reqRateLimiter = rate.NewLimiter(rate.Inf, defaultReqLimitBurst)

	// request rate limit for opcode
	reqOpRateLimitMap   = make(map[uint8]uint64)
	reqOpRateLimiterMap = make(map[uint8]*rate.Limiter)

	// request rate limit of each data partition for volume
	reqVolPartRateLimitMap   = make(map[string]uint64)
	reqVolPartRateLimiterMap = make(map[string]map[uint64]*rate.Limiter)

	// request rate limit of each data partition for volume & opcode
	reqVolOpPartRateLimitMap   = make(map[string]map[uint8]uint64)
	reqVolOpPartRateLimiterMap = make(map[string]map[uint8]map[uint64]*rate.Limiter)

	isRateLimitOn          bool
	reqRateLimiterMapMutex sync.Mutex

	// all cluster internal nodes
	clusterMap = make(map[string]bool)
)

func (m *DataNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	// call once on init before first tick
	m.updateClusterMap()
	clusterViewTicker := time.NewTicker(UpdateClusterViewTicket)
	defer ticker.Stop()
	defer clusterViewTicker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("datanode nodeinfo goroutine stopped")
			return
		case <-ticker.C:
			m.updateNodeInfo()
		case <-clusterViewTicker.C:
			m.updateClusterMap()
		}
	}
}

func (m *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *DataNode) updateNodeInfo() {
	limitInfo, err := MasterClient.AdminAPI().GetLimitInfo()
	if err != nil {
		log.LogErrorf("[updateDataNodeInfo] %s", err.Error())
		return
	}
	r := limitInfo.DataNodeDeleteLimitRate
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	deleteLimiteRater.SetLimit(l)

	// Request rate limiter design:
	// 1. Rate limit of a given object (volume/opcode/partition) can has a default value,
	//    which is the value of the empty object.
	// 2. When rate limit from master is changed or deleted,
	//    change or delete the corresponding limiter if necessay (the default value doesn't exists).
	// 3. When rate limit is added, delay the add of corresponding limiter to request handling.

	reqRateLimit = limitInfo.DataNodeReqRateLimit
	l = rate.Limit(reqRateLimit)
	if reqRateLimit == 0 {
		l = rate.Inf
	}
	reqRateLimiter.SetLimit(l)

	reqRateLimiterMapMutex.Lock()
	reqOpRateLimitMap = limitInfo.DataNodeReqOpRateLimitMap
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

	reqVolPartRateLimitMap = limitInfo.DataNodeReqVolPartRateLimitMap
	var deleteVol []string
	for vol, m := range reqVolPartRateLimiterMap {
		r, ok := reqVolPartRateLimitMap[vol]
		if !ok {
			r, ok = reqVolPartRateLimitMap[""]
		}
		if !ok {
			deleteVol = append(deleteVol, vol)
			continue
		}
		for _, limiter := range m {
			limiter.SetLimit(rate.Limit(r))
		}
	}
	for _, vol := range deleteVol {
		delete(reqVolPartRateLimiterMap, vol)
	}

	deleteVolOpMap := make(map[string][]uint8)
	reqVolOpPartRateLimitMap = limitInfo.DataNodeReqVolOpPartRateLimitMap
	for vol, opPartRateLimiterMap := range reqVolOpPartRateLimiterMap {
		opPartLimitMap, ok := reqVolOpPartRateLimitMap[vol]
		if !ok {
			opPartLimitMap, ok = reqVolOpPartRateLimitMap[""]
		}
		if !ok {
			deleteVolOpMap[vol] = nil
			continue
		}
		for op, m := range opPartRateLimiterMap {
			r, ok := opPartLimitMap[op]
			if !ok {
				r, ok = opPartLimitMap[0]
			}
			if !ok {
				deleteVolOpMap[vol] = append(deleteVolOpMap[vol], op)
				continue
			}
			for _, limiter := range m {
				limiter.SetLimit(rate.Limit(r))
			}
		}
	}
	for vol, opSlice := range deleteVolOpMap {
		if opSlice == nil {
			delete(reqVolOpPartRateLimiterMap, vol)
		} else {
			for _, op := range opSlice {
				delete(reqVolOpPartRateLimiterMap[vol], op)
			}
		}
	}
	reqRateLimiterMapMutex.Unlock()

	isRateLimitOn = (reqRateLimit > 0 ||
		len(reqOpRateLimitMap) > 0 ||
		len(reqVolPartRateLimitMap) > 0 ||
		len(reqVolOpPartRateLimitMap) > 0)

	m.space.SetDiskFixTinyDeleteRecordLimit(limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	m.space.SetDiskRepairTaskLimit(limitInfo.DataNodeRepairTaskLimitOnDisk)
}

func (m *DataNode) updateClusterMap() {
	cv, err := MasterClient.AdminAPI().GetCluster()
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
	for _, master := range MasterClient.Nodes() {
		addrSlice = strings.Split(master, ":")
		addrMap[addrSlice[0]] = true
	}
	clusterMap = addrMap
}

func DeleteLimiterWait() {
	deleteLimiteRater.Wait(context.Background())
}
