package datanode

import (
	"time"

	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

const (
	defaultMarkDeleteLimitRate  = rate.Inf
	defaultMarkDeleteLimitBurst = 512
	UpdateNodeInfoTicket        = 1 * time.Minute
)

var (
	nodeInfoStopC = make(chan struct{}, 0)
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
	setLimiter(deleteLimiteRater, clusterInfo.DataNodeDeleteLimitRate)
	setDoExtentRepair(int(clusterInfo.DataNodeAutoRepairLimitRate))
	log.LogInfof("updateNodeInfo from master:"+
		"deleteLimite(%v),autoRepairLimit(%v)", clusterInfo.DataNodeDeleteLimitRate,
		clusterInfo.DataNodeAutoRepairLimitRate)
}
