package datanode

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statinfo"
	"github.com/chubaofs/chubaofs/util/statistics"
	"golang.org/x/time/rate"
)

const (
	DefaultMarkDeleteLimitBurst           = 512
	UpdateNodeBaseInfoTicket              = 1 * time.Minute
	UpdateRateLimitInfoTicket             = 5 * time.Minute
	UpdateClusterViewTicket               = 24 * time.Hour
	DefaultFixTinyDeleteRecordLimitOnDisk = 1
	DefaultRepairTaskLimitOnDisk          = 5
	DefaultReqLimitBurst                  = 512
	DefaultNormalExtentDeleteExpireTime   = 4 * 3600
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, DefaultMarkDeleteLimitBurst)

	// request rate limiter for entire data node
	reqRateLimit   uint64
	reqRateLimiter = rate.NewLimiter(rate.Inf, DefaultReqLimitBurst)

	// map[opcode]*rate.Limiter, request rate limiter for opcode
	reqOpRateLimitMap   = make(map[uint8]uint64)
	reqOpRateLimiterMap = make(map[uint8]*rate.Limiter)

	// map[volume]map[opcode]*rate.Limiter, request rate limiter for volume & opcode
	reqVolOpRateLimitMap   = make(map[string]map[uint8]uint64)
	reqVolOpRateLimiterMap = make(map[string]map[uint8]*rate.Limiter)

	// map[volume]map[dp]*rate.Limiter, request rate limiter of each data partition for volume
	reqVolPartRateLimitMap   = make(map[string]uint64)
	reqVolPartRateLimiterMap = make(map[string]map[uint64]*rate.Limiter)

	// map[volume]map[opcode]map[dp]*rate.Limiter, request rate limiter of each data partition for volume & opcode
	reqVolOpPartRateLimitMap   = make(map[string]map[uint8]uint64)
	reqVolOpPartRateLimiterMap = make(map[string]map[uint8]map[uint64]*rate.Limiter)

	isRateLimitOn bool

	// all cluster internal nodes
	clusterMap     = make(map[string]bool)
	limitInfo      *proto.LimitInfo
	limitVolumeMap = make(map[string]bool)
	limitOpcodeMap = map[uint8]bool{
		proto.OpStreamRead:         true,
		proto.OpStreamFollowerRead: true,
		proto.OpWrite:              true,
		proto.OpRandomWrite:        true,
	}
	// all partitions of a given volume
	volumePartMap = make(map[string]map[uint64]bool)

	logMaxSize uint64
)

func (m *DataNode) startUpdateNodeInfo() {
	updateNodeBaseInfoTicker := time.NewTicker(UpdateNodeBaseInfoTicket)
	rateLimitTicker := time.NewTicker(UpdateRateLimitInfoTicket)
	clusterViewTicker := time.NewTicker(UpdateClusterViewTicket)
	defer func() {
		updateNodeBaseInfoTicker.Stop()
		rateLimitTicker.Stop()
		clusterViewTicker.Stop()
	}()

	// call once on init before first tick
	m.updateClusterMap()
	m.updateNodeBaseInfo()
	m.updateRateLimitInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("datanode nodeinfo goroutine stopped")
			return
		case <-updateNodeBaseInfoTicker.C:
			m.updateNodeBaseInfo()
		case <-rateLimitTicker.C:
			m.updateRateLimitInfo()
		case <-clusterViewTicker.C:
			m.updateClusterMap()
		}
	}
}

func (m *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *DataNode) updateNodeBaseInfo() {
	//todo: better using a lightweighter interface
	info, err := MasterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogWarnf("[updateNodeBaseInfo] get limit info err: %s", err.Error())
		return
	}

	limitInfo = info
	r := limitInfo.DataNodeDeleteLimitRate
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	deleteLimiteRater.SetLimit(l)

	m.space.SetDiskFixTinyDeleteRecordLimit(limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	if limitInfo.DataNodeRepairTaskCountZoneLimit != nil {
		if taskLimit, ok := limitInfo.DataNodeRepairTaskCountZoneLimit[m.zoneName]; ok {
			limitInfo.DataNodeRepairTaskLimitOnDisk = taskLimit
		}
	}
	m.space.SetDiskRepairTaskLimit(limitInfo.DataNodeRepairTaskLimitOnDisk)
	m.space.SetForceFlushFDInterval(limitInfo.DataNodeFlushFDInterval)
	m.space.SetSyncWALOnUnstableEnableState(limitInfo.DataSyncWALOnUnstableEnableState)
	m.space.SetForceFlushFDParallelismOnDisk(limitInfo.DataNodeFlushFDParallelismOnDisk)

	m.space.SetNormalExtentDeleteExpireTime(limitInfo.DataNodeNormalExtentDeleteExpire)
	if statistics.StatisticsModule != nil {
		statistics.StatisticsModule.UpdateMonitorSummaryTime(limitInfo.MonitorSummarySec)
		statistics.StatisticsModule.UpdateMonitorReportTime(limitInfo.MonitorReportSec)
	}

	m.updateLogMaxSize(limitInfo.LogMaxSize)
}

func (m *DataNode) updateRateLimitInfo() {
	if limitInfo == nil {
		return
	}
	volInfo, err := MasterClient.AdminAPI().ListVols("")
	if err != nil {
		log.LogWarnf("[updateRateLimitInfo] get volume list err: %s", err.Error())
		return
	}
	volMap := make(map[string]bool)
	for _, vol := range volInfo {
		volMap[vol.Name] = true
	}
	limitVolumeMap = volMap

	// Request rate limiter design:
	// 1. Rate limit of a given object (volume/opcode/partition) can has a default value,
	//    which is the value of the empty object.
	// 2. When rate limit from master is changed or deleted,
	//    change or delete the corresponding limiter if necessay (the default value doesn't exists).
	// 3. Construct all limiter maps at limit info update, to avoid locking at request handling.

	var (
		r                        uint64
		l                        rate.Limit
		ok                       bool
		opRateLimitMap           map[uint8]uint64
		volOpRateLimitMap        map[string]map[uint8]uint64
		partRateLimiterMap       map[uint64]*rate.Limiter
		partMap                  map[uint64]bool
		partitionID              uint64
		tmpOpRateLimiterMap      map[uint8]*rate.Limiter
		tmpVolOpRateLimiterMap   map[string]map[uint8]*rate.Limiter
		tmpVolPartRateLimiterMap map[string]map[uint64]*rate.Limiter
	)

	// update request rate limiter for zone
	r, ok = limitInfo.DataNodeReqZoneRateLimitMap[m.zoneName]
	if !ok {
		r, ok = limitInfo.DataNodeReqZoneRateLimitMap[""]
	}
	if !ok {
		reqRateLimit = 0
		goto reqOpRateLimiterLabel
	} else if reqRateLimit == r {
		goto reqOpRateLimiterLabel
	}
	reqRateLimit = r
	l = rate.Inf
	if reqRateLimit > 0 {
		l = rate.Limit(reqRateLimit)
	}
	reqRateLimiter.SetLimit(l)

reqOpRateLimiterLabel:
	// update request rate limiter for opcode
	opRateLimitMap, ok = limitInfo.DataNodeReqZoneOpRateLimitMap[m.zoneName]
	if !ok {
		opRateLimitMap, ok = limitInfo.DataNodeReqZoneOpRateLimitMap[""]
	}
	if !ok {
		reqOpRateLimitMap = make(map[uint8]uint64)
		reqOpRateLimiterMap = make(map[uint8]*rate.Limiter)
		goto reqVolOpRateLimiterLabel
	} else if reflect.DeepEqual(reqOpRateLimitMap, opRateLimitMap) {
		goto reqVolOpRateLimiterLabel
	}
	reqOpRateLimitMap = opRateLimitMap
	tmpOpRateLimiterMap = make(map[uint8]*rate.Limiter)
	for op := range limitOpcodeMap {
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

reqVolOpRateLimiterLabel:
	// update request rate limiter for vol & opcode
	volOpRateLimitMap, ok = limitInfo.DataNodeReqZoneVolOpRateLimitMap[m.zoneName]
	if !ok {
		volOpRateLimitMap, ok = limitInfo.DataNodeReqZoneVolOpRateLimitMap[""]
	}
	if !ok {
		reqVolOpRateLimitMap = make(map[string]map[uint8]uint64)
		reqVolOpRateLimiterMap = make(map[string]map[uint8]*rate.Limiter)
		goto reqVolPartRateLimiterLabel
	} else if reflect.DeepEqual(reqVolOpRateLimitMap, volOpRateLimitMap) {
		goto reqVolPartRateLimiterLabel
	}
	reqVolOpRateLimitMap = volOpRateLimitMap
	tmpVolOpRateLimiterMap = make(map[string]map[uint8]*rate.Limiter)
	for vol, _ := range limitVolumeMap {
		opRateLimitMap, ok := volOpRateLimitMap[vol]
		if !ok {
			opRateLimitMap, ok = volOpRateLimitMap[""]
		}
		if !ok {
			continue
		}
		opRateLimiterMap := make(map[uint8]*rate.Limiter)
		tmpVolOpRateLimiterMap[vol] = opRateLimiterMap
		for op := range limitOpcodeMap {
			r, ok = opRateLimitMap[op]
			if !ok {
				r, ok = opRateLimitMap[0]
			}
			if !ok {
				continue
			}
			l = rate.Limit(r)
			opRateLimiterMap[op] = rate.NewLimiter(l, DefaultReqLimitBurst)
		}
	}
	reqVolOpRateLimiterMap = tmpVolOpRateLimiterMap

reqVolPartRateLimiterLabel:
	// update request rate limiter of each data partition for volume
	if reflect.DeepEqual(reqVolPartRateLimitMap, limitInfo.DataNodeReqVolPartRateLimitMap) {
		goto reqVolOpPartRateLimiterLabel
	}
	volumePartMap = m.getVolPartMap()
	reqVolPartRateLimitMap = limitInfo.DataNodeReqVolPartRateLimitMap
	tmpVolPartRateLimiterMap = make(map[string]map[uint64]*rate.Limiter)
	for vol, _ := range limitVolumeMap {
		r, ok = reqVolPartRateLimitMap[vol]
		if !ok {
			r, ok = reqVolPartRateLimitMap[""]
		}
		if !ok {
			continue
		}
		partMap, ok = volumePartMap[vol]
		if !ok {
			continue
		}
		l = rate.Limit(r)
		partRateLimiterMap = make(map[uint64]*rate.Limiter)
		tmpVolPartRateLimiterMap[vol] = partRateLimiterMap
		for partitionID, _ = range partMap {
			partRateLimiterMap[partitionID] = rate.NewLimiter(l, DefaultReqLimitBurst)
		}
	}
	reqVolPartRateLimiterMap = tmpVolPartRateLimiterMap

reqVolOpPartRateLimiterLabel:
	// update request rate limiter of each data partition for volume & opcode
	if reflect.DeepEqual(reqVolOpPartRateLimitMap, limitInfo.DataNodeReqVolOpPartRateLimitMap) {
		isRateLimitOn = (reqRateLimit > 0 ||
			len(reqOpRateLimitMap) > 0 ||
			len(reqVolOpRateLimitMap) > 0 ||
			len(reqVolPartRateLimitMap) > 0 ||
			len(reqVolOpPartRateLimitMap) > 0)
		return
	}
	volumePartMap = m.getVolPartMap()
	reqVolOpPartRateLimitMap = limitInfo.DataNodeReqVolOpPartRateLimitMap
	tmpVolOpPartRateLimiterMap := make(map[string]map[uint8]map[uint64]*rate.Limiter)
	for vol, _ := range limitVolumeMap {
		opPartLimitMap, ok := reqVolOpPartRateLimitMap[vol]
		if !ok {
			opPartLimitMap, ok = reqVolOpPartRateLimitMap[""]
		}
		if !ok {
			continue
		}
		partMap, ok = volumePartMap[vol]
		if !ok {
			continue
		}
		opPartRateLimiterMap := make(map[uint8]map[uint64]*rate.Limiter)
		tmpVolOpPartRateLimiterMap[vol] = opPartRateLimiterMap
		for op, _ := range limitOpcodeMap {
			r, ok = opPartLimitMap[op]
			if !ok {
				r, ok = opPartLimitMap[0]
			}
			if !ok {
				continue
			}
			l = rate.Limit(r)
			partRateLimiterMap = make(map[uint64]*rate.Limiter)
			opPartRateLimiterMap[op] = partRateLimiterMap
			for partitionID, _ = range partMap {
				partRateLimiterMap[partitionID] = rate.NewLimiter(l, DefaultReqLimitBurst)
			}
		}
	}
	reqVolOpPartRateLimiterMap = tmpVolOpPartRateLimiterMap

	isRateLimitOn = (reqRateLimit > 0 ||
		len(reqOpRateLimitMap) > 0 ||
		len(reqVolOpRateLimitMap) > 0 ||
		len(reqVolPartRateLimitMap) > 0 ||
		len(reqVolOpPartRateLimitMap) > 0)
}

func (m *DataNode) getVolPartMap() map[string]map[uint64]bool {
	volPartMap := make(map[string]map[uint64]bool)
	var (
		partMap map[uint64]bool
		ok      bool
	)
	m.space.RangePartitions(func(dp *DataPartition) bool {
		partMap, ok = volPartMap[dp.volumeID]
		if !ok {
			partMap = make(map[uint64]bool)
			volPartMap[dp.volumeID] = partMap
		}
		partMap[dp.partitionID] = true
		return true
	})
	return volPartMap
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

func (m *DataNode) updateLogMaxSize(val uint64) {
	if val != 0 && logMaxSize != val {
		oldLogMaxSize := logMaxSize
		logMaxSize = val
		log.SetLogMaxSize(int64(val))
		log.LogInfof("updateLogMaxSize, logMaxSize(old:%v, new:%v)", oldLogMaxSize, logMaxSize)
	}
}

func DeleteLimiterWait() {
	deleteLimiteRater.Wait(context.Background())
}

func (m *DataNode) startUpdateProcessStatInfo() {
	m.processStatInfo = statinfo.NewProcessStatInfo()
	m.processStatInfo.ProcessStartTime = time.Now().Format("2006-01-02 15:04:05")
	go m.processStatInfo.UpdateStatInfoSchedule()
}
