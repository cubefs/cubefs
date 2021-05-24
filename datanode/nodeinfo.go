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
	nodeInfoStopC                = make(chan struct{}, 0)
	deleteLimiteRater            = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
	reqLimitRater                = rate.NewLimiter(rate.Inf, defaultReqLimitBurst)
	reqVolPartLimitRateMap       = make(map[string]uint64)
	reqVolPartLimitRaterMap      = make(map[string]map[uint64]*rate.Limiter)
	reqVolPartLimitRaterMapMutex sync.Mutex
	clusterMap                   = make(map[string]bool)
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
	clusterInfo, err := MasterClient.AdminAPI().GetClusterInfo()
	if err != nil {
		log.LogErrorf("[updateDataNodeInfo] %s", err.Error())
		return
	}
	r := clusterInfo.DataNodeDeleteLimitRate
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	deleteLimiteRater.SetLimit(l)

	r = clusterInfo.DataNodeReqLimitRate
	l = rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	reqLimitRater.SetLimit(l)

	// change data node per partition request limit rater for a given volume
	// 1. if the limit rate is changed for a given volume, change the limit rater
	// 2. if the limit rate is deleted for a given volums, set the default limit rate(if exists) to the limit rater
	// 3. if the default limit rate is deleted, delete all the limit rater without specified limit rate
	// the empty volume represent all other unspecified volumes
	reqVolPartLimitRateMap := clusterInfo.DataNodeReqVolPartLimitRateMap
	reqVolPartLimitRaterMapMutex.Lock()
	for vol, m := range reqVolPartLimitRaterMap {
		r, ok := reqVolPartLimitRateMap[vol]
		if ok {
			for _, rater := range m {
				rater.SetLimit(rate.Limit(r))
			}
		} else {
			r, ok = reqVolPartLimitRateMap[""]
			if ok {
				for _, rater := range m {
					rater.SetLimit(rate.Limit(r))
				}
			} else {
				for _, rater := range m {
					rater.SetLimit(rate.Inf)
				}
			}
		}
	}
	reqVolPartLimitRaterMapMutex.Unlock()

	m.space.SetDiskFixTinyDeleteRecordLimit(clusterInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	m.space.SetDiskRepairTaskLimit(clusterInfo.DataNodeRepairTaskLimitOnDisk)
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
