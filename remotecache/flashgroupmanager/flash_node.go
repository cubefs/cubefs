package flashgroupmanager

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

const (
	defaultIntervalToCheckHeartbeat = 6
	noHeartBeatTimes                = 3
	defaultNodeTimeOutSec           = noHeartBeatTimes * defaultIntervalToCheckHeartbeat
	DefaultNodeTimeoutDuration      = defaultNodeTimeOutSec * time.Second
)

type flashNodeValue struct {
	// immutable
	ID       uint64
	Addr     string
	ZoneName string
	Version  string
	// mutable
	FlashGroupID   uint64 // 0: have not allocated to flash group
	IsEnable       bool
	TaskCountLimit int
}

type FlashNode struct {
	TaskManager *AdminTaskManager

	sync.RWMutex
	flashNodeValue
	DiskStat      []*proto.FlashNodeDiskCacheStat
	ReportTime    time.Time
	IsActive      bool
	LimiterStatus *proto.FlashNodeLimiterStatusInfo
	WorkRole      string
}

func NewFlashNode(addr, zoneName, clusterID, version string, isEnable bool) *FlashNode {
	node := new(FlashNode)
	node.Addr = addr
	node.ZoneName = zoneName
	node.Version = version
	node.IsEnable = isEnable
	node.ReportTime = time.Now()
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	return node
}

func (flashNode *FlashNode) GetFlashNodeViewInfo() (info *proto.FlashNodeViewInfo) {
	flashNode.RLock()
	info = &proto.FlashNodeViewInfo{
		ID:            flashNode.ID,
		Addr:          flashNode.Addr,
		ReportTime:    flashNode.ReportTime,
		IsActive:      flashNode.IsActive,
		Version:       flashNode.Version,
		ZoneName:      flashNode.ZoneName,
		FlashGroupID:  flashNode.FlashGroupID,
		IsEnable:      flashNode.IsEnable,
		DiskStat:      flashNode.DiskStat,
		LimiterStatus: flashNode.LimiterStatus,
	}
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) isActiveAndEnable() (ok bool) {
	flashNode.RLock()
	ok = flashNode.IsActive && flashNode.IsEnable
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) clean() {
	flashNode.TaskManager.exitCh <- struct{}{}
}

func (flashNode *FlashNode) isWriteable() (ok bool) {
	flashNode.RLock()
	if flashNode.FlashGroupID == UnusedFlashNodeFlashGroupID &&
		time.Since(flashNode.ReportTime) < DefaultNodeTimeoutDuration {
		ok = true
	}
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) setActive() {
	flashNode.Lock()
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	flashNode.Unlock()
}

func (flashNode *FlashNode) updateFlashNodeStatHeartbeat(resp *proto.FlashNodeHeartbeatResponse) {
	log.LogInfof("updateFlashNodeStatHeartbeat, flashNode:%v, resp[%v], time:%v", flashNode.Addr, resp, time.Now().Format("2006-01-02 15:04:05"))
	flashNode.Lock()
	flashNode.DiskStat = resp.Stat
	flashNode.LimiterStatus = resp.LimiterStatus
	flashNode.TaskCountLimit = resp.FlashNodeTaskCountLimit
	flashNode.Unlock()
}

func (flashNode *FlashNode) checkLiveliness() {
	flashNode.Lock()
	if time.Since(flashNode.ReportTime) > DefaultNodeTimeoutDuration {
		msg := fmt.Sprintf("flashnode[%v] heartbeat lost, last heartbeat time %v", flashNode.Addr, flashNode.ReportTime)
		auditlog.LogMasterOp("checkLiveliness", msg, nil)
		flashNode.IsActive = false
	}
	flashNode.Unlock()
}

func (flashNode *FlashNode) createHeartbeatTask(masterAddr string, flashNodeHandleReadTimeout int, flashNodeReadDataNodeTimeout int, flashHotKeyMissCount int, flashReadFlowLimit int64, flashWriteFlowLimit int64) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	request.FlashNodeHandleReadTimeout = flashNodeHandleReadTimeout
	request.FlashNodeReadDataNodeTimeout = flashNodeReadDataNodeTimeout
	request.FlashHotKeyMissCount = flashHotKeyMissCount
	request.FlashReadFlowLimit = flashReadFlowLimit
	request.FlashWriteFlowLimit = flashWriteFlowLimit

	task = proto.NewAdminTask(proto.OpFlashNodeHeartbeat, flashNode.Addr, request)
	return
}

func (flashNode *FlashNode) createSetIOLimitsTask(flow, iocc, factor int, opCode uint8) (task *proto.AdminTask) {
	request := &proto.FlashNodeSetIOLimitsRequest{
		Flow:   flow,
		Iocc:   iocc,
		Factor: factor,
	}
	task = proto.NewAdminTask(opCode, flashNode.Addr, request)
	return
}
