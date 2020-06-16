package datanode

import (
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	UpdateNodeInfoTicket = 1 * time.Minute
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
		log.LogErrorf("[register] %s", err.Error())
		return
	}
	m.SetMarkDeleteRate(clusterInfo.DataNodeDeleteLimitRate)
}
