package datanode

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	defaultMarkDeleteLimitBurst = 512
	defaultIOLimitBurst         = 512
	UpdateNodeInfoTicket        = 1 * time.Minute

	RepairTimeOut   = time.Hour * 24
	MaxRepairErrCnt = 1000
)

var nodeInfoStopC = make(chan struct{})

func (m *DataNode) startUpdateNodeInfo(ctx_ context.Context) {
	ctx := proto.ContextWithOperation(ctx_, "startUpdateNodeInfo")
	span := proto.SpanFromContext(ctx)
	ticker := time.NewTicker(UpdateNodeInfoTicket)
	defer ticker.Stop()
	for {
		select {
		case <-nodeInfoStopC:
			span.Info("datanode nodeinfo goroutine stopped")
			return
		case <-ticker.C:
			m.updateNodeInfo(ctx)
		}
	}
}

func (m *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *DataNode) updateNodeInfo(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	clusterInfo, err := MasterClient.AdminAPI().GetClusterInfo(ctx)
	if err != nil {
		span.Errorf("[updateDataNodeInfo] %s", err.Error())
		return
	}

	setLimiter(deleteLimiteRater, clusterInfo.DataNodeDeleteLimitRate)

	setDoExtentRepair(int(clusterInfo.DataNodeAutoRepairLimitRate))

	atomic.StoreUint64(&m.dpMaxRepairErrCnt, clusterInfo.DpMaxRepairErrCnt)

	span.Infof("updateNodeInfo from master:"+
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
