package flashgroupmanager

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
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

func (flashNode *FlashNode) isEnable() (ok bool) {
	flashNode.RLock()
	ok = flashNode.IsEnable
	flashNode.RUnlock()
	return
}

func (flashNode *FlashNode) isActiveAndEnable() (ok bool) {
	flashNode.RLock()
	ok = flashNode.IsActive && flashNode.IsEnable
	flashNode.RUnlock()
	return
}
