package flashgroupmanager

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

const (
	defaultIntervalToCheckHeartbeat = 6
	noHeartBeatTimes                = 3
	defaultNodeTimeOutSec           = noHeartBeatTimes * defaultIntervalToCheckHeartbeat
	DefaultNodeTimeoutDuration      = defaultNodeTimeOutSec * time.Second
)

type FlashNodeValue struct {
	// immutable
	ID       uint64
	Addr     string
	ZoneName string
	Version  string
	// mutable
	FlashGroupID      uint64 // 0: have not allocated to flash group
	IsEnable          bool
	TaskCountLimit    int
	FlashNodeTopoName string `json:"FlashNodeTopoName,omitempty"` // empty means default topology
	Region            string
}

type FlashNode struct {
	TaskManager *AdminTaskManager

	sync.RWMutex
	FlashNodeValue
	DiskStat      []*proto.FlashNodeDiskCacheStat
	ReportTime    time.Time
	IsActive      bool
	LimiterStatus *proto.FlashNodeLimiterStatusInfo
	WorkRole      string
	CacheVols     []string
}

type FlashNodeBadDiskInfo struct {
	Addr     string
	DiskPath string
}

func NewFlashNodeFromFnv(clusterID string, fnv *FlashNodeValue) *FlashNode {
	node := new(FlashNode)
	node.ID = fnv.ID
	node.FlashGroupID = fnv.FlashGroupID
	node.Addr = fnv.Addr
	// avoid panic
	if fnv.ZoneName == "" {
		fnv.ZoneName = proto.DefaultZoneName
		log.LogWarnf("action[NewFlashNodeFromFnv], flashNode[flashNodeId:%v addr:%s flashGroupId:%v] zone is empty",
			fnv.ID, fnv.Addr, fnv.FlashGroupID)
	}
	node.ZoneName = fnv.ZoneName
	node.Version = fnv.Version
	node.IsEnable = fnv.IsEnable
	node.ReportTime = time.Now()
	node.TaskManager = newAdminTaskManager(fnv.Addr, clusterID)
	node.TaskManager.connPool = util.NewConnectPoolWithTimeout(idleConnTimeout, connectTimeout, false)
	topoName := fnv.FlashNodeTopoName
	if topoName == "" {
		topoName = proto.DefaultTopoName
		if fnv.FlashGroupID == UnusedFlashNodeFlashGroupID {
			topoName = proto.IdleTopoName
		}
	}
	node.FlashNodeTopoName = topoName
	region := fnv.Region
	// For backward compatibility: if region is empty (from old version without region concept),
	// set it to default region regardless of topoName
	if region == "" {
		region = proto.DefaultRegionName
	}
	node.Region = region
	return node
}

func NewFlashNode(addr, zoneName, clusterID, version, topoName, region string, isEnable bool) *FlashNode {
	node := new(FlashNode)
	node.Addr = addr
	node.ZoneName = zoneName
	node.Version = version
	node.IsEnable = isEnable
	node.ReportTime = time.Now()
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	node.TaskManager.connPool = util.NewConnectPoolWithTimeout(idleConnTimeout, connectTimeout, false)
	if topoName == "" {
		topoName = proto.IdleTopoName
	}
	node.FlashNodeTopoName = topoName
	node.Region = region
	return node
}

func (flashNode *FlashNode) GetFlashNodeViewInfo() (info *proto.FlashNodeViewInfo) {
	flashNode.RLock()
	info = &proto.FlashNodeViewInfo{
		ID:                flashNode.ID,
		Addr:              flashNode.Addr,
		ReportTime:        flashNode.ReportTime,
		IsActive:          flashNode.IsActive,
		Version:           flashNode.Version,
		ZoneName:          flashNode.ZoneName,
		FlashGroupID:      flashNode.FlashGroupID,
		IsEnable:          flashNode.IsEnable,
		DiskStat:          flashNode.DiskStat,
		LimiterStatus:     flashNode.LimiterStatus,
		FlashNodeTopoName: flashNode.FlashNodeTopoName,
		Region:            flashNode.Region,
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

func (flashNode *FlashNode) SetActive() {
	flashNode.Lock()
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	flashNode.Unlock()
}

func (flashNode *FlashNode) UpdateFlashNodeStatHeartbeat(resp *proto.FlashNodeHeartbeatResponse) {
	log.LogInfof("UpdateFlashNodeStatHeartbeat, flashNode:%v, resp[%v], time:%v", flashNode.Addr, resp, time.Now().Format("2006-01-02 15:04:05"))
	flashNode.Lock()
	flashNode.DiskStat = resp.Stat
	flashNode.LimiterStatus = resp.LimiterStatus
	flashNode.TaskCountLimit = resp.FlashNodeTaskCountLimit
	flashNode.CacheVols = resp.Vols
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

func (flashNode *FlashNode) createHeartbeatTask(masterAddr string, flashNodeHandleReadTimeout int,
	flashNodeReadDataNodeTimeout int, flashHotKeyMissCount int,
	flashReadFlowLimit int64, flashWriteFlowLimit int64, flashKeyFlowLimit int64, slots []uint32,
	remoteCacheDisableTTLMap map[string]bool,
) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	request.FlashNodeHandleReadTimeout = flashNodeHandleReadTimeout
	request.FlashNodeReadDataNodeTimeout = flashNodeReadDataNodeTimeout
	request.FlashHotKeyMissCount = flashHotKeyMissCount
	request.FlashReadFlowLimit = flashReadFlowLimit
	request.FlashWriteFlowLimit = flashWriteFlowLimit
	request.FlashKeyFlowLimit = flashKeyFlowLimit
	request.FlashNodeSlots = slots
	request.FlashNodeID = flashNode.ID
	request.TopoName = flashNode.FlashNodeTopoName
	request.RemoteCacheDisableTTL = remoteCacheDisableTTLMap
	log.LogDebugf("createHeartbeatTask, flashNode:%v, topo:%v", flashNode.Addr, flashNode.FlashNodeTopoName)
	task = proto.NewAdminTask(proto.OpFlashNodeHeartbeat, flashNode.Addr, request)
	return
}

func (flashNode *FlashNode) CreateSetIOLimitsTask(flow, iocc, factor int, opCode uint8) (task *proto.AdminTask) {
	request := &proto.FlashNodeSetIOLimitsRequest{
		Flow:   flow,
		Iocc:   iocc,
		Factor: factor,
	}
	task = proto.NewAdminTask(opCode, flashNode.Addr, request)
	task.TopoName = flashNode.FlashNodeTopoName
	return
}

func (flashNode *FlashNode) SyncSendAdminTask(task *proto.AdminTask) (packet *proto.Packet, err error) {
	return flashNode.TaskManager.SyncSendAdminTask(task)
}

func (flashNode *FlashNode) CreateFnScanTask(masterAddr string, manualTask *proto.FlashManualTask) (task *proto.AdminTask) {
	request := &proto.FlashNodeManualTaskRequest{
		MasterAddr: masterAddr,
		FnNodeAddr: flashNode.Addr,
		Task:       manualTask,
	}
	task = proto.NewAdminTaskEx(proto.OpFlashNodeScan, flashNode.Addr, request, manualTask.Id)
	task.TopoName = manualTask.TopoName
	return
}

func (flashNode *FlashNode) SetToUnused(addr string, flashGroupID uint64, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != flashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] not equal to target flash group:%v",
			flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}
	oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = UnusedFlashNodeFlashGroupID

	if err = syncUpdateFlashNodeFunc(flashNode); err != nil {
		flashNode.FlashGroupID = oldFgID
		return
	}
	go func() {
		time.Sleep(time.Duration(DefaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr = fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()
	return nil
}

func (flashNode *FlashNode) UpdateZoneName(t *FlashNodeTopology, newZoneName string, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	// may be from old version flash node
	if newZoneName == "" {
		newZoneName = proto.DefaultZoneName
	}
	needUpdate := false
	flashNode.RLock()
	if flashNode.ZoneName != newZoneName {
		needUpdate = true
	}
	flashNode.RUnlock()
	if !needUpdate {
		return
	}
	var oldZone, newZone *FlashNodeZone
	// create new zone if absent
	newZone, err = t.GetZone(newZoneName)
	if err != nil {
		newZone = t.PutZoneIfAbsent(NewFlashNodeZone(newZoneName))
		err = nil
	}
	// find old zone
	oldZone, err = t.GetZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	// update zone name
	flashNode.Lock()
	flashNode.ZoneName = newZoneName
	flashNode.Unlock()
	err = syncUpdateFlashNodeFunc(flashNode)
	if err != nil {
		return
	}
	// delete from old zone
	oldZone.flashNode.Delete(flashNode.Addr)
	// put in new zone
	newZone.putFlashNode(flashNode)
	log.LogDebugf("fn %v zoneName %v -> %v", flashNode.Addr, oldZone.name, newZone.name)
	return
}

func (flashNode *FlashNode) TryUpdateInfos(region string, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	needUpdate := false
	flashNode.RLock()
	if flashNode.Region != region {
		needUpdate = true
	}
	flashNode.RUnlock()
	if !needUpdate {
		return
	}
	flashNode.Lock()
	flashNode.Region = region
	flashNode.Unlock()

	err = syncUpdateFlashNodeFunc(flashNode)
	if err != nil {
		return
	}
	log.LogDebugf("TryUpdateInfos: update region %v for fn[%v]", region, flashNode.Addr)
	return
}

func (flashNode *FlashNode) String() string {
	return fmt.Sprintf("flashNodeId:%v addr:%v zone %v flashGroupId:%v topoName: %v region: %v",
		flashNode.ID, flashNode.Addr, flashNode.ZoneName, flashNode.FlashGroupID, flashNode.FlashNodeTopoName, flashNode.Region)
}
