package datanode

import (
	"time"

	"github.com/chubaofs/chubaofs/util/log"
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
			log.LogInfo("datanode nodeinfo goroutine stopped")
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

	if clusterInfo.LoadFactor > 0 && clusterInfo.LoadFactor != loadFactor {
		log.LogWarnf("[updateNodeInfo] update load factor from [%v] to [%v]", loadFactor, clusterInfo.LoadFactor)
		loadFactor = clusterInfo.LoadFactor
	}

	setLimiter(deleteLimiteRater, clusterInfo.DataNodeDeleteLimitRate)

	setDoExtentRepair(int(clusterInfo.DataNodeAutoRepairLimitRate))
	log.LogInfof("updateNodeInfo from master:"+
		"deleteLimite(%v),autoRepairLimit(%v)", clusterInfo.DataNodeDeleteLimitRate,
		clusterInfo.DataNodeAutoRepairLimitRate)
}
