package datanode

import (
	"context"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
	"golang.org/x/time/rate"
)

const (
	defaultMarkDeleteLimitRate            = rate.Inf
	defaultMarkDeleteLimitBurst           = 512
	UpdateNodeInfoTicket                  = 1 * time.Minute
	defaultFixTinyDeleteRecordLimitOnDisk = 50
	defaultRepairTaskLimitOnDisk          = 10
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
)

func (m *DataNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	defer ticker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("metanode nodeinfo goroutine stopped")
			return
		case <-ticker.C:
			m.updateNodeInfo()
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
	m.space.SetDiskFixTinyDeleteRecordLimit(clusterInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	m.space.SetDiskRepairTaskLimit(clusterInfo.DataNodeRepairTaskLimitOnDisk)
}

func DeleteLimiterWait() {
	deleteLimiteRater.Wait(context.Background())
}
