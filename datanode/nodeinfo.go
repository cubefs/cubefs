package datanode

import (
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"golang.org/x/time/rate"
)

const (
	defaultMarkDeleteLimitRate  = rate.Inf
	defaultMarkDeleteLimitBurst = 512
	defaultIOLimitBurst         = 512
	UpdateNodeInfoTicket        = 1 * time.Minute

	RepairTimeOut   = time.Hour * 24
	MaxRepairErrCnt = 1000
)

var nodeInfoStopC = make(chan struct{})

func (m *DataNode) startUpdateNodeInfo() {
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	defer ticker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			log.Info("datanode nodeinfo goroutine stopped")
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
		log.Errorf("[updateDataNodeInfo] %s", err.Error())
		return
	}

	setLimiter(deleteLimiteRater, clusterInfo.DataNodeDeleteLimitRate)

	setDoExtentRepair(int(clusterInfo.DataNodeAutoRepairLimitRate))

	atomic.StoreUint64(&m.dpMaxRepairErrCnt, clusterInfo.DpMaxRepairErrCnt)

	log.Infof("updateNodeInfo from master:"+
		"deleteLimite(%v), autoRepairLimit(%v), dpMaxRepairErrCnt(%v)",
		clusterInfo.DataNodeDeleteLimitRate, clusterInfo.DataNodeAutoRepairLimitRate,
		clusterInfo.DpMaxRepairErrCnt)
}

func (m *DataNode) GetDpMaxRepairErrCnt() uint64 {
	dpMaxRepairErrCnt := atomic.LoadUint64(&m.dpMaxRepairErrCnt)
	if dpMaxRepairErrCnt == 0 {
		return MaxRepairErrCnt
	}
	return dpMaxRepairErrCnt
}
