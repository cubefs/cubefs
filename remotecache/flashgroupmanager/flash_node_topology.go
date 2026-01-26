package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

const (
	defaultFlashGroupSlotsCount = 32
	twoDaysInSeconds            = 2 * 24 * 60 * 60
)

type (
	AllocateCommonIDFunc     func() (id uint64, err error)
	SyncAddFlashNodeFunc     func(flashNode *FlashNode) (err error)
	SyncUpdateFlashNodeFunc  func(flashNode *FlashNode) (err error)
	SyncDeleteFlashNodeFunc  func(flashNode *FlashNode) (err error)
	SyncMoveFlashNodeFunc    func(oldAddr string, newValue *FlashNodeValue) (err error)
	SyncAddFlashGroupFunc    func(flashGroup *FlashGroup) (err error)
	SyncDeleteFlashGroupFunc func(flashGroup *FlashGroup) (err error)
	SyncUpdateFlashGroupFunc func(flashGroup *FlashGroup) (err error)
	SyncUpdateFlashTopoFunc  func(flashGroup *FlashNodeTopology) (err error)
)

type FlashNodeZone struct {
	mu        sync.RWMutex
	name      string
	flashNode sync.Map // key: FlashNodeAddr, value: *FlashNode
}

func NewFlashNodeZone(name string) (zone *FlashNodeZone) {
	return &FlashNodeZone{name: name}
}

func (zone *FlashNodeZone) putFlashNode(flashNode *FlashNode) {
	zone.flashNode.Store(flashNode.Addr, flashNode)
}

func (zone *FlashNodeZone) selectFlashNodes(count int, excludeHosts []string, region string) (newHosts []string, err error) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.flashNode.Range(func(_, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if contains(excludeHosts, flashNode.Addr) {
			return true
		}
		if len(newHosts) >= count {
			return false
		}
		if flashNode.isWriteable() && flashNode.Region == region {
			newHosts = append(newHosts, flashNode.Addr)
		}
		return true
	})
	if len(newHosts) != count {
		return nil, fmt.Errorf("expect count:%v newHostsCount:%v,detail:%v,excludeHosts:%v", count, len(newHosts), newHosts, excludeHosts)
	}
	return
}

type FlashNodeTopologyValue struct {
	ID                uint64
	Name              string
	Region            string
	Status            uint32
	DeleteExecTime    time.Time
	DeleteGradualFlag bool
	DeleteStep        uint32
}

type FlashNodeTopology struct {
	mu sync.RWMutex

	createFlashGroupLock sync.RWMutex      // create/delete flashGroup
	slotsMap             map[uint32]uint64 // key:slot, value: FlashGroupID

	flashGroupMap  sync.Map // key: FlashGroupID, value: *FlashGroup
	flashNodeMap   sync.Map // key: FlashNodeAddr, value: *FlashNode
	flashNodeIDMap sync.Map // key: FlashNodeID, value: *FlashNode
	zoneMap        sync.Map // key: zoneName, value: *FlashNodeZone

	clientEmpty        []byte       // empty response cache
	clientOff          atomic.Value // []byte, default nil (on)
	clientCache        atomic.Value // []byte te client response cache
	SyncFlashGroupFunc SyncUpdateFlashGroupFunc

	FlashNodeTopologyValue // support multi-region
}

func NewFlashNodeTopology(name, region string, id uint64, status uint32) (t *FlashNodeTopology) {
	empty, err := json.Marshal(newSuccessHTTPReply(proto.FlashGroupView{}))
	if err != nil {
		panic(fmt.Sprintf("action[NewFlashNodeTopology] json marshal %v", err))
	}
	t = &FlashNodeTopology{
		slotsMap:    make(map[uint32]uint64),
		clientEmpty: empty,
	}
	t.ID = id
	t.Name = name
	t.Region = region
	t.Status = status
	t.DeleteExecTime = time.Time{}
	t.clientOff.Store([]byte(nil))
	t.clientCache.Store([]byte(nil))
	return t
}

func (t *FlashNodeTopology) gradualCreateFlashGroup(fgID uint64, syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	setSlots []uint32, setWeight uint32, step uint32,
) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	var addedSlotsNum uint32
	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots, setWeight)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	remainingSlotsNum := uint32(len(slots)) - step
	if remainingSlotsNum > 0 {
		addedSlotsNum = step
		flashGroup = newFlashGroup(fgID, slots[:step], proto.SlotStatus_Creating, slots[step:], step,
			proto.FlashGroupStatus_Inactive, setWeight, t.Name, t.Region)
	} else {
		addedSlotsNum = uint32(len(slots))
		flashGroup = newFlashGroup(fgID, slots, proto.SlotStatus_Completed, make([]uint32, 0), 0,
			proto.FlashGroupStatus_Inactive, setWeight, t.Name, t.Region)
	}
	if err = syncUpdateFlashGroupFunc(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}

	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	for _, slot := range slots[:addedSlotsNum] {
		t.slotsMap[slot] = flashGroup.ID
	}
	return
}

func (t *FlashNodeTopology) createFlashGroup(fgID uint64, syncAddFlashGroupFunc SyncAddFlashGroupFunc,
	setSlots []uint32, setWeight uint32,
) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots, setWeight)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	flashGroup = newFlashGroup(fgID, slots, proto.SlotStatus_Completed, make([]uint32, 0), 0,
		proto.FlashGroupStatus_Inactive, setWeight, t.Name, t.Region)

	if err = syncAddFlashGroupFunc(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}
	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	for _, slot := range slots {
		t.slotsMap[slot] = flashGroup.ID
	}
	return
}

func (t *FlashNodeTopology) DettachFlashGroup(flashGroup *FlashGroup, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	flashGroup.lock.Lock()
	slots := flashGroup.Slots
	oldStatus := flashGroup.Status
	flashGroup.Status = proto.FlashGroupStatus_Inactive
	if err = syncDeleteFlashGroupFunc(flashGroup); err != nil {
		flashGroup.Status = oldStatus
		flashGroup.lock.Unlock()
		return
	}
	flashGroup.lock.Unlock()

	t.removeSlots(slots)
	t.flashGroupMap.Delete(flashGroup.ID)
	return
}

func (t *FlashNodeTopology) GetClientResponse() []byte {
	if cache := t.clientOff.Load().([]byte); len(cache) > 0 {
		return cache
	}
	if cache := t.clientCache.Load().([]byte); len(cache) > 0 {
		return cache
	}
	return t.UpdateClientResponse()
}

func (t *FlashNodeTopology) UpdateClientResponse() []byte {
	cache, err := json.Marshal(newSuccessHTTPReply(t.getFlashGroupView()))
	if err != nil {
		log.LogError("action[updateClientResponse] json marshal", err)
		return nil
	}
	t.clientCache.Store(cache)
	return cache[:]
}

func (t *FlashNodeTopology) getFlashGroupView() (fgv *proto.FlashGroupView) {
	fgv = new(proto.FlashGroupView)
	fgv.TopoName = t.Name
	fgv.Enable = true
	fgCount := 0
	t.flashGroupMap.Range(func(_, _ interface{}) bool {
		fgCount++
		return true
	})
	disableFlashGroupNum := 0
	maxDisableFlashGroupCount := fgCount * 2 / 3

	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if fg.GetStatus().IsActive() {
			var oldSlots, oldReservedSlots []uint32
			hosts := fg.getFlashNodeHostsEnableAndActive()
			if len(hosts) == 0 {
				if log.EnableInfo() {
					log.LogInfof("fg(%v) lost all flashnodes", fg)
				}
				atomic.StoreInt32(&fg.LostAllFlashNode, 1)
				fg.ReduceSlot(t.SyncFlashGroupFunc)
			} else {
				atomic.StoreInt32(&fg.LostAllFlashNode, 0)
				if len(fg.ReservedSlots) > 0 {
					if len(fg.Slots) == 0 && fg.ReduceAllTime != 0 && (time.Now().Unix()-fg.ReduceAllTime >= twoDaysInSeconds) {
						fg.IncreaseSlot(t.SyncFlashGroupFunc)
					} else if atomic.LoadInt32(&fg.IncreasingSlots) == 0 {
						fg.lock.Lock()
						oldSlots = append(oldSlots, fg.Slots...)
						oldReservedSlots = append(oldReservedSlots, fg.ReservedSlots...)
						if log.EnableInfo() {
							log.LogInfof("recover fg(%v) oldSlots(%v) oldReservedSlots(%v)", fg.ID, oldSlots, oldReservedSlots)
						}
						fg.Slots = append(fg.Slots, fg.ReservedSlots...)
						fg.ReservedSlots = make([]uint32, 0)
						fg.ReduceAllTime = 0
						if err := t.SyncFlashGroupFunc(fg); err != nil {
							fg.Slots = oldSlots
							fg.ReservedSlots = oldReservedSlots
						}
						fg.lock.Unlock()
					}
				}
			}

			if len(fg.Slots) == 0 {
				disableFlashGroupNum++
				if disableFlashGroupNum >= maxDisableFlashGroupCount && len(fg.ReservedSlots) > 0 {
					fg.lock.Lock()
					oldSlots = append(oldSlots, fg.Slots...)
					oldReservedSlots = append(oldReservedSlots, fg.ReservedSlots...)
					if log.EnableInfo() {
						log.LogInfof("recover fg(%v) oldSlots(%v) oldReservedSlots(%v)", fg.ID, oldSlots, oldReservedSlots)
					}
					fg.Slots = append(fg.Slots, fg.ReservedSlots...)
					fg.ReservedSlots = make([]uint32, 0)
					fg.ReduceAllTime = 0
					if err := t.SyncFlashGroupFunc(fg); err != nil {
						fg.Slots = oldSlots
						fg.ReservedSlots = oldReservedSlots
						fg.ReduceAllTime = 0
					}
					fg.lock.Unlock()
				} else {
					return true
				}
			}
			fgv.FlashGroups = append(fgv.FlashGroups, &proto.FlashGroupInfo{
				ID:    fg.ID,
				Slot:  fg.Slots,
				Hosts: hosts,
			})

		}
		return true
	})
	return
}

func (t *FlashNodeTopology) GetFlashGroup(fgID uint64) (flashGroup *FlashGroup, err error) {
	value, ok := t.flashGroupMap.Load(fgID)
	if !ok {
		return nil, fmt.Errorf("flashGroup[%v] is not found in topo %v region %v", fgID, t.Name, t.Region)
	}
	flashGroup = value.(*FlashGroup)
	if flashGroup == nil {
		return nil, fmt.Errorf("flashGroup[%v] is not found in topo %v region %v", fgID, t.Name, t.Region)
	}
	return
}

func (t *FlashNodeTopology) allocateNewSlotsForCreateFlashGroup(fgID uint64, setSlots []uint32, weight uint32) (slots []uint32) {
	slots = make([]uint32, 0, len(setSlots))
	for _, slot := range setSlots {
		if _, ok := t.slotsMap[slot]; !ok {
			slots = append(slots, slot)
		}
	}
	if len(slots) > 0 {
		return
	}

	for len(slots) < int(weight)*defaultFlashGroupSlotsCount {
		slot := allocateNewSlot()
		if _, ok := t.slotsMap[slot]; ok {
			continue
		}
		slots = append(slots, slot)
	}
	return
}

func (t *FlashNodeTopology) GetFlashGroupsAdminView(fgStatus proto.FlashGroupStatus, allStatus bool) (fgv *proto.FlashGroupsAdminView) {
	fgv = new(proto.FlashGroupsAdminView)
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if allStatus || fg.GetStatus() == fgStatus {
			fgv.FlashGroups = append(fgv.FlashGroups, fg.GetAdminView())
		}
		return true
	})
	return
}

func (t *FlashNodeTopology) GradualRemoveFlashGroup(flashGroup *FlashGroup,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, step uint32,
) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	return t.gradualExpandOrShrinkFlashGroupSlots(flashGroup, syncUpdateFlashGroupFunc, proto.SlotStatus_Deleting, flashGroup.GetSlots(), step)
}

func (t *FlashNodeTopology) gradualExpandOrShrinkFlashGroupSlots(flashGroup *FlashGroup,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, newSlotStatus proto.SlotStatus,
	pendingSlots []uint32, step uint32,
) (err error) {
	flashGroup.lock.Lock()
	oldSlotStatus := flashGroup.SlotStatus
	oldStep := flashGroup.Step
	oldPendingSlots := flashGroup.PendingSlots
	flashGroup.SlotStatus = newSlotStatus
	flashGroup.PendingSlots = pendingSlots
	flashGroup.Step = step
	if err = syncUpdateFlashGroupFunc(flashGroup); err != nil {
		flashGroup.SlotStatus = oldSlotStatus
		flashGroup.PendingSlots = oldPendingSlots
		flashGroup.Step = oldStep
		flashGroup.lock.Unlock()
		return
	}

	flashGroup.lock.Unlock()
	return
}

func (t *FlashNodeTopology) removeSlots(slots []uint32) {
	for _, slot := range slots {
		delete(t.slotsMap, slot)
	}
}

func allocateNewSlot() (slot uint32) {
	bytes, _ := uuid.New().MarshalBinary()
	slot = crc32.ChecksumIEEE(bytes)
	return
}

func (t *FlashNodeTopology) UpdateClientCache() {
	// update clientCache directly, do not care leader or not
	t.UpdateClientResponse()
}

func (t *FlashNodeTopology) GetZone(name string) (zone *FlashNodeZone, err error) {
	if name == "" {
		return nil, fmt.Errorf("zone name is empty")
	}
	value, ok := t.zoneMap.Load(name)
	if !ok {
		return nil, fmt.Errorf("zone[%s] not found", name)
	}
	if zone = value.(*FlashNodeZone); zone == nil {
		return nil, fmt.Errorf("zone[%s] not found", name)
	}
	return
}

func (t *FlashNodeTopology) PutZoneIfAbsent(zone *FlashNodeZone) (old *FlashNodeZone) {
	oldZone, loaded := t.zoneMap.LoadOrStore(zone.name, zone)
	if loaded {
		return oldZone.(*FlashNodeZone)
	}
	return zone
}

func (t *FlashNodeTopology) PutFlashNode(flashNode *FlashNode) (err error) {
	if t.Name != proto.IdleTopoName && t.Region != flashNode.Region {
		err = fmt.Errorf("top %v region[%v] is not equal to fn[%v] region[%v]", t.Name, t.Region, flashNode.Addr, flashNode.Region)
		log.LogWarnf("PutFlashNode: err %v", err)
		return err
	}
	if _, loaded := t.flashNodeMap.LoadOrStore(flashNode.Addr, flashNode); loaded {
		t.flashNodeIDMap.LoadOrStore(flashNode.ID, flashNode)
		return
	}
	t.flashNodeIDMap.LoadOrStore(flashNode.ID, flashNode)
	_, err = t.GetZone(flashNode.ZoneName)
	if err != nil {
		t.PutZoneIfAbsent(NewFlashNodeZone(flashNode.ZoneName))
		err = nil
	}
	zone, err := t.GetZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.putFlashNode(flashNode)
	return
}

func (t *FlashNodeTopology) Clear() {
	t.flashGroupMap.Range(func(key, _ interface{}) bool {
		t.flashGroupMap.Delete(key)
		return true
	})
	t.zoneMap.Range(func(key, _ interface{}) bool {
		t.zoneMap.Delete(key)
		return true
	})
	t.flashNodeIDMap.Range(func(key, _ interface{}) bool {
		t.flashNodeIDMap.Delete(key)
		return true
	})
	t.flashNodeMap.Range(func(key, node interface{}) bool {
		t.flashNodeMap.Delete(key)
		flashNode := node.(*FlashNode)
		flashNode.clean()
		return true
	})
	t.clientCache.Store([]byte(nil))
}

func (t *FlashNodeTopology) resetFlashNodeTaskManagerTargetAddr(fn *FlashNode, targetAddr string, conflict bool) {
	if fn == nil || fn.TaskManager == nil {
		return
	}
	fn.TaskManager.Lock()
	fn.TaskManager.targetAddr = targetAddr
	if fn.TaskManager.connPool != nil {
		fn.TaskManager.connPool.Close()
	}
	if conflict {
		fn.TaskManager.connPool = util.NewConnectPoolWithTimeoutAndCap(0, 1, idleConnTimeout, connectTimeout, true)
	} else {
		fn.TaskManager.connPool = util.NewConnectPoolWithTimeout(idleConnTimeout, connectTimeout, false)
	}
	fn.TaskManager.Unlock()
}

func addrPortPlus1024(addr string) (string, error) {
	arr := strings.SplitN(addr, ":", 2)
	if len(arr) != 2 || arr[0] == "" || arr[1] == "" {
		return "", fmt.Errorf("invalid addr %q", addr)
	}
	p, err := strconv.ParseUint(arr[1], 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid addr %q: parse port: %w", addr, err)
	}
	return fmt.Sprintf("%s:%d", arr[0], p+1024), nil
}

func (t *FlashNodeTopology) moveFlashNodeAddr(
	flashNode *FlashNode,
	newAddr, newZoneName, newVersion string,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc, conflict bool,
) (err error) {
	if flashNode == nil {
		return fmt.Errorf("moveFlashNodeAddr: flashNode is nil")
	}
	if newAddr == "" {
		return fmt.Errorf("moveFlashNodeAddr: newAddr is empty")
	}
	if syncMoveFlashNodeFunc == nil {
		return fmt.Errorf("moveFlashNodeAddr: syncMoveFlashNodeFunc is nil")
	}

	flashNode.Lock()
	defer flashNode.Unlock()

	if flashNode.Addr == newAddr && flashNode.ZoneName == newZoneName && flashNode.Version == newVersion {
		return nil
	}

	oldAddr := flashNode.Addr
	oldZoneName := flashNode.ZoneName

	newValue := flashNode.FlashNodeValue
	newValue.Addr = newAddr
	newValue.ZoneName = newZoneName
	newValue.Version = newVersion

	if err = syncMoveFlashNodeFunc(oldAddr, &newValue); err != nil {
		return err
	}

	if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
		if fg, err1 := t.GetFlashGroup(flashNode.FlashGroupID); err1 == nil {
			fg.RemoveFlashNode(oldAddr)
		}
	}
	t.flashNodeMap.Delete(oldAddr)
	if oldZone, err1 := t.GetZone(oldZoneName); err1 == nil {
		oldZone.flashNode.Delete(oldAddr)
	}

	flashNode.Addr = newAddr
	flashNode.ZoneName = newZoneName
	flashNode.Version = newVersion

	t.resetFlashNodeTaskManagerTargetAddr(flashNode, newAddr, conflict)

	t.flashNodeMap.Store(newAddr, flashNode)

	if !conflict {
		if _, err1 := t.GetZone(newZoneName); err1 != nil {
			t.PutZoneIfAbsent(NewFlashNodeZone(newZoneName))
		}
		if z, err1 := t.GetZone(newZoneName); err1 == nil {
			z.putFlashNode(flashNode)
		}
		if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
			if fg, err1 := t.GetFlashGroup(flashNode.FlashGroupID); err1 == nil {
				fg.putFlashNode(flashNode)
			}
		}
	}
	return nil
}

func (t *FlashNodeTopology) AddFlashNode(clusterName, nodeAddr, zoneName, version, region string,
	id uint64, allocateCommonIDFunc AllocateCommonIDFunc,
	syncAddFlashNodeFunc SyncAddFlashNodeFunc, syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (nodeID uint64, err error) {
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
		if err != nil {
			log.LogErrorf("action[addFlashNode],clusterID[%v] Addr:%v err:%v ", clusterName, nodeAddr, err.Error())
		}
	}()
	var flashNode *FlashNode
	log.LogInfof("action[addFlashNode] Addr:%v topo %v, ZoneName:%v region %v nodeID %v ", nodeAddr, t.Name, zoneName, region, id)
	if id > 0 {
		if value, ok := t.flashNodeIDMap.Load(id); ok {
			flashNode = value.(*FlashNode)
			if flashNode.Addr != nodeAddr {
				log.LogWarnf("FlashNode ID[%d] IP changed from %s to %s", id, flashNode.Addr, nodeAddr)
				if v, ok2 := t.flashNodeMap.Load(nodeAddr); ok2 {
					if conflictNode, ok3 := v.(*FlashNode); ok3 && conflictNode != nil && conflictNode.ID != id {
						newAddr, err1 := addrPortPlus1024(conflictNode.Addr)
						if err1 != nil {
							return 0, err1
						}
						if err = t.moveFlashNodeAddr(conflictNode, newAddr, conflictNode.ZoneName, conflictNode.Version, syncMoveFlashNodeFunc, true); err != nil {
							return 0, err
						}
					}
				}
				if err = t.moveFlashNodeAddr(flashNode, nodeAddr, zoneName, version, syncMoveFlashNodeFunc, false); err != nil {
					return 0, err
				}
			}
			return flashNode.ID, nil
		}
	}

	flashNode, err = t.PeekFlashNode(nodeAddr)
	if err == nil {
		return flashNode.ID, nil
	}
	flashNode = NewFlashNode(nodeAddr, zoneName, clusterName, version, t.Name, region, true)
	_, err = t.GetZone(zoneName)
	if err != nil {
		t.PutZoneIfAbsent(NewFlashNodeZone(zoneName))
	}
	if nodeID, err = allocateCommonIDFunc(); err != nil {
		return
	}
	flashNode.ID = nodeID
	if err = syncAddFlashNodeFunc(flashNode); err != nil {
		return
	}
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	if err = t.PutFlashNode(flashNode); err != nil {
		return
	}
	log.LogInfof("action[addFlashNode],clusterID[%v] Addr:%v topo %v, ZoneName:%v region %v nodeID %v success", clusterName, nodeAddr, flashNode.FlashNodeTopoName, flashNode.ZoneName, flashNode.Region, flashNode.ID)
	return
}

func (t *FlashNodeTopology) PeekFlashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := t.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(notFoundMsg(fmt.Sprintf("flashnode[%v] from topo[%v] region %v",
			addr, t.Name, t.Region)), "")
		return
	}
	flashNode = value.(*FlashNode)
	return
}

func (t *FlashNodeTopology) PeekFlashNodeById(id uint64) (flashNode *FlashNode, err error) {
	value, ok := t.flashNodeIDMap.Load(id)
	if !ok {
		err = errors.Trace(notFoundMsg(fmt.Sprintf("flashnode[%v] from topo[%v]", id, t.Name)), "")
		return
	}
	flashNode = value.(*FlashNode)
	return
}

func (t *FlashNodeTopology) ListFlashNodes(showAll, active bool) map[string][]*proto.FlashNodeViewInfo {
	zoneFlashNodes := make(map[string][]*proto.FlashNodeViewInfo)
	t.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		log.LogDebugf("ListFlashNodes: ListFlashNodes topo %v key %v fn %v, showAll %v active %v",
			t.Name, key, flashNode, showAll, active)
		if showAll || flashNode.isActiveAndEnable() == active {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.GetFlashNodeViewInfo())
		}
		return true
	})
	return zoneFlashNodes
}

func (t *FlashNodeTopology) UpdateFlashNode(flashNode *FlashNode, enable bool,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (err error) {
	var needUpdateClientCache bool
	flashNode.Lock()
	if flashNode.IsEnable != enable {
		oldState := flashNode.IsEnable
		flashNode.IsEnable = enable
		if err = syncUpdateFlashNodeFunc(flashNode); err != nil {
			flashNode.IsEnable = oldState
			flashNode.Unlock()
			return
		}
		if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
			needUpdateClientCache = true
		}
	}
	flashNode.Unlock()
	if needUpdateClientCache {
		t.UpdateClientCache()
	}
	return
}

func (t *FlashNodeTopology) RemoveFlashNode(clusterName string, flashNode *FlashNode,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
) (err error) {
	log.LogDebugf("action[removeFlashNode], ZoneName[%s] Node[%s] offline", flashNode.ZoneName, flashNode.Addr)
	var flashGroupID uint64
	if flashGroupID, err = t.deleteFlashNode(flashNode, syncDeleteFlashNodeFunc); err != nil {
		return
	}
	if flashGroupID != UnusedFlashNodeFlashGroupID {
		var flashGroup *FlashGroup
		if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
			return
		}
		flashGroup.RemoveFlashNode(flashNode.Addr)
		t.UpdateClientCache()
	}
	go func() {
		time.Sleep(time.Duration(DefaultWaitClientUpdateFgTimeSec) * time.Second)
		arr := strings.SplitN(flashNode.Addr, ":", 2)
		p, _ := strconv.ParseUint(arr[1], 10, 64)
		addr := fmt.Sprintf("%s:%d", arr[0], p+1)
		if err = httpclient.New().Addr(addr).FlashNode().EvictAll(); err != nil {
			log.LogErrorf("flashNode[%v] evict all failed, err:%v", flashNode.Addr, err)
			return
		}
	}()

	log.LogInfof("action[removeFlashNode], clusterID[%s] node[%s] flashGroupID[%d] offline success",
		clusterName, flashNode.Addr, flashGroupID)
	return
}

func (t *FlashNodeTopology) deleteFlashNode(flashNode *FlashNode,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
) (oldFlashGroupID uint64, err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	oldFlashGroupID = flashNode.FlashGroupID
	flashNode.FlashGroupID = UnusedFlashNodeFlashGroupID
	if err = syncDeleteFlashNodeFunc(flashNode); err != nil {
		log.LogErrorf("action[deleteFlashNode] node[%v] update failed,err[%v]", flashNode.Addr, err)
		flashNode.FlashGroupID = oldFlashGroupID
		return
	}
	// delFlashNodeFromCache
	t.flashNodeMap.Delete(flashNode.Addr)
	t.flashNodeIDMap.Delete(flashNode.ID)
	var zone *FlashNodeZone
	zone, err = t.GetZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.flashNode.Delete(flashNode.Addr)
	go flashNode.clean()
	return
}

func (t *FlashNodeTopology) GetAllInactiveFlashNodes() (removeNodes []*FlashNode) {
	t.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if !flashNode.isActiveAndEnable() && flashNode.FlashGroupID == UnusedFlashNodeFlashGroupID {
			removeNodes = append(removeNodes, flashNode)
		}
		return true
	})
	return
}

func (t *FlashNodeTopology) GetAllActiveFlashNodes() (removeNodes []*FlashNode) {
	t.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if flashNode.isActiveAndEnable() {
			removeNodes = append(removeNodes, flashNode)
		}
		return true
	})
	return
}

func (t *FlashNodeTopology) TurnFlashGroup(enabled bool) {
	if enabled {
		t.clientOff.Store([]byte(nil))
	} else {
		t.clientOff.Store(t.clientEmpty)
	}
}

func (t *FlashNodeTopology) CreateFlashGroup(id uint64, syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	syncAddFlashGroupFunc SyncAddFlashGroupFunc, setSlots []uint32, setWeight uint32, gradualFlag bool,
	step uint32,
) (fg *FlashGroup, err error) {
	if gradualFlag {
		if fg, err = t.gradualCreateFlashGroup(id, syncUpdateFlashGroupFunc, setSlots, setWeight, step); err != nil {
			return
		}
	} else {
		if fg, err = t.createFlashGroup(id, syncAddFlashGroupFunc, setSlots, setWeight); err != nil {
			return
		}
	}
	t.UpdateClientCache()
	return
}

func (t *FlashNodeTopology) RemoveFlashGroup(clusterName string, idleTopo *FlashNodeTopology, id uint64, gradualFlag bool,
	step uint32, syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
	syncAddFlashNodeFunc SyncAddFlashNodeFunc,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(id); err != nil {
		return
	}
	if flashGroup.GetSlotStatus() == proto.SlotStatus_Deleting {
		err = fmt.Errorf("the flashGroup(%v) is in slotDeleting status, it cannot be deleted repeatedly", flashGroup.ID)
		return
	}
	remainingSlotsNum := uint32(flashGroup.GetSlotsCount()) - step
	if gradualFlag && remainingSlotsNum > 0 {
		err = t.GradualRemoveFlashGroup(flashGroup, syncUpdateFlashGroupFunc, step)
		return
	}
	// remove flash nodes then del the flash group
	err = t.removeAllFlashNodeFromFlashGroup(clusterName, idleTopo, flashGroup, syncUpdateFlashNodeFunc, syncDeleteFlashNodeFunc,
		syncAddFlashNodeFunc, syncMoveFlashNodeFunc)
	if err != nil {
		return
	}
	err = t.DettachFlashGroup(flashGroup, syncDeleteFlashGroupFunc)
	if err != nil {
		return
	}
	t.UpdateClientCache()
	return
}

func (t *FlashNodeTopology) removeAllFlashNodeFromFlashGroup(clusterName string, idleTopo *FlashNodeTopology, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc, syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
	syncAddFlashNodeFunc SyncAddFlashNodeFunc, syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (err error) {
	flashNodeHosts := flashGroup.GetFlashNodeHosts(false)
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		// TODO: flashgroupmanager doesn't have idle topo
		if idleTopo == nil {
			if err = t.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup, syncUpdateFlashNodeFunc); err != nil {
				log.LogErrorf("remove flashNode from flashGroup failed, successHost:%v, flashNodeHosts:%v err:%v",
					successHost, flashNodeHosts, err)
				return
			}
		} else {
			// move all fn to idle
			var fn *FlashNode
			if fn, err = t.PeekFlashNode(flashNodeHost); err != nil {
				return
			}
			if err = t.ChangeFlashNodeTopo(clusterName, idleTopo, fn, syncDeleteFlashNodeFunc, syncAddFlashNodeFunc,
				syncMoveFlashNodeFunc); err != nil {
				err = fmt.Errorf("successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
				return
			}
		}
		successHost = append(successHost, flashNodeHost)
	}
	log.LogInfof("action[RemoveAllFlashNodeFromFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost)
	return
}

func (t *FlashNodeTopology) removeFlashNodeFromFlashGroup(addr string, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (err error) {
	var flashNode *FlashNode
	if flashNode, err = t.setFlashNodeToUnused(addr, flashGroup.ID, syncUpdateFlashNodeFunc); err != nil {
		return
	}
	flashGroup.RemoveFlashNode(flashNode.Addr)
	log.LogInfo(fmt.Sprintf("action[removeFlashNodeFromFlashGroup] node:%v flashGroup:%v, success",
		flashNode.Addr, flashGroup.ID))
	return
}

func (t *FlashNodeTopology) setFlashNodeToUnused(addr string, flashGroupID uint64, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (flashNode *FlashNode, err error) {
	if flashNode, err = t.PeekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != flashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] not equal to target flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
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

	return
}

func (t *FlashNodeTopology) AddFlashNodeToFlashGroupWithTargetTopo(targetTopo *FlashNodeTopology,
	flashGroup *FlashGroup, addr string, zoneName string,
	count int, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (err error) {
	log.LogDebugf("action[AddFlashNodeToFlashGroupWithTargetTopo] add fn[%v] to flashGroup %v[%v] by topo %v",
		addr, flashGroup.ID, flashGroup.FlashNodeTopoName, t.Name)
	defer func() {
		if err != nil {
			log.LogWarnf("action[AddFlashNodeToFlashGroupWithTargetTopo] add fn[%v] to flashGroup %v[%v] by topo %v:err %v",
				addr, flashGroup.ID, flashGroup.FlashNodeTopoName, t.Name, err.Error())
		}
	}()
	if addr != "" {
		err = t.addFlashNodeToFlashGroup(addr, flashGroup, syncUpdateFlashNodeFunc, targetTopo, true)
	} else {
		err = t.selectFlashNodesFromZoneAddToFlashGroup(zoneName, count, nil, flashGroup, syncUpdateFlashNodeFunc,
			targetTopo, true)
	}
	if err != nil {
		log.LogWarnf("action[AddFlashNodeToFlashGroupWithTargetTopo] add addr %v to flashGroup %v from topo %v by topo %v failed %v",
			addr, flashGroup.ID, flashGroup.FlashNodeTopoName, t.Name, err.Error())
		return
	}
	t.UpdateClientCache()
	log.LogDebugf("action[AddFlashNodeToFlashGroupWithTargetTopo] add addr %v to flashGroup %v from topo %v by topo %v success",
		addr, flashGroup.ID, flashGroup.FlashNodeTopoName, t.Name)
	return
}

func (t *FlashNodeTopology) FlashGroupAddFlashNode(flashGroupID uint64, addr string, zoneName string,
	count int, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
		return
	}
	if addr != "" {
		err = t.addFlashNodeToFlashGroup(addr, flashGroup, syncUpdateFlashNodeFunc, t, false)
	} else {
		err = t.selectFlashNodesFromZoneAddToFlashGroup(zoneName, count, nil, flashGroup, syncUpdateFlashNodeFunc,
			t, false)
	}
	if err != nil {
		return
	}
	t.UpdateClientCache()
	return
}

func (t *FlashNodeTopology) addFlashNodeToFlashGroup(addr string, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc, targetTopo *FlashNodeTopology, removeFlashNodes bool,
) (err error) {
	var flashNode *FlashNode
	if flashNode, err = t.tryAttachFlashNodeToFlashGroup(addr, flashGroup.ID, syncUpdateFlashNodeFunc, targetTopo.Name, flashGroup.Region); err != nil {
		return
	}
	err = flashGroup.putFlashNode(flashNode)
	if err != nil {
		log.LogWarnf("action[AddFlashNodeToFlashGroupWithTargetTopo] flashGroup %v put addr %v failed:err %v", flashGroup.ID, addr, err.Error())
		return
	}
	if removeFlashNodes {
		t.flashNodeMap.Delete(flashNode.Addr)
		t.flashNodeIDMap.Delete(flashNode.ID)
		var zone *FlashNodeZone
		zone, err = t.GetZone(flashNode.ZoneName)
		if err != nil {
			log.LogWarnf("action[AddFlashNodeToFlashGroupWithTargetTopo] addr %v zone not found in flashGroup %v[%v] by topo %v:err %v",
				addr, flashGroup.ID, flashGroup.FlashNodeTopoName, t.Name, err.Error())
			return
		}
		zone.flashNode.Delete(flashNode.Addr)
		// add to targetTopo
		_, err = targetTopo.GetZone(flashNode.ZoneName)
		if err != nil {
			targetTopo.PutZoneIfAbsent(NewFlashNodeZone(flashNode.ZoneName))
			err = nil
		}
		err = targetTopo.PutFlashNode(flashNode)
		if err != nil {
			log.LogWarnf("action[AddFlashNodeToFlashGroupWithTargetTopo] addr %v add to  topo %v:err %v",
				addr, t.Name, err.Error())
			return
		}
		targetTopo.UpdateClientCache()
	}
	return
}

func (t *FlashNodeTopology) tryAttachFlashNodeToFlashGroup(addr string, flashGroupID uint64,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc, topoName, fgRegion string,
) (flashNode *FlashNode, err error) {
	if flashNode, err = t.PeekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.Region != fgRegion {
		err = fmt.Errorf("fg %v region[%v] not equal to fn[%v] region[%v]", flashGroupID, fgRegion, flashNode.Addr, flashNode.Region)
		return
	}

	if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] can not add to flash group:%v topo :%v",
			flashNode.Addr, flashNode.FlashGroupID, flashGroupID, t.Name)
		return
	}
	if time.Since(flashNode.ReportTime) > DefaultNodeTimeoutDuration {
		flashNode.IsActive = false
		err = fmt.Errorf("flashNode[%v] is inactive lastReportTime:%v", flashNode.Addr, flashNode.ReportTime)
		return
	}
	oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = flashGroupID
	flashNode.FlashNodeTopoName = topoName
	if err = syncUpdateFlashNodeFunc(flashNode); err != nil {
		flashNode.FlashGroupID = oldFgID
		return
	}
	log.LogInfof("action[setFlashNodeToFlashGroup] add flash node:%v to flashGroup:%v topo :%v success",
		addr, flashGroupID, t.Name)
	return
}

func (t *FlashNodeTopology) selectFlashNodesFromZoneAddToFlashGroup(zoneName string, count int, excludeHosts []string,
	flashGroup *FlashGroup, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc, targetTopo *FlashNodeTopology, removeFlashNodes bool,
) (err error) {
	flashNodeZone, err := t.GetZone(zoneName)
	if err != nil {
		return
	}
	newHosts, err := flashNodeZone.selectFlashNodes(count, excludeHosts, targetTopo.Region)
	if err != nil {
		return
	}
	log.LogDebugf("action[selectFlashNodesFromZoneAddToFlashGroup] select newHosts:%v from zone %v topo %v",
		newHosts, zoneName, t.Name)
	successHost := make([]string, 0)
	for _, newHost := range newHosts {
		if err = t.addFlashNodeToFlashGroup(newHost, flashGroup, syncUpdateFlashNodeFunc, targetTopo, removeFlashNodes); err != nil {
			err = fmt.Errorf("successHost:%v, newHosts:%v err:%v", successHost, newHosts, err)
			return
		}
		successHost = append(successHost, newHost)
	}
	log.LogInfof("action[selectFlashNodesFromZoneAddToFlashGroup] flashGroup:%v successHost:%v",
		flashGroup.ID, successHost)
	return
}

func (t *FlashNodeTopology) FlashGroupRemoveFlashNode(flashGroupID uint64, addr string, zoneName string,
	count int, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
		return
	}
	if addr != "" {
		err = t.removeFlashNodeFromFlashGroup(addr, flashGroup, syncUpdateFlashNodeFunc)
	} else {
		err = t.removeFlashNodesFromTargetZone(zoneName, count, flashGroup, syncUpdateFlashNodeFunc)
	}
	if err != nil {
		log.LogWarnf("action[FlashGroupRemoveFlashNode] remove flash node:%v from flashGroup:%v topo :%v success",
			addr, flashGroupID, t.Name)
		return
	}
	t.UpdateClientCache()
	log.LogInfof("action[FlashGroupRemoveFlashNode] remove flash node:%v from flashGroup:%v topo :%v success",
		addr, flashGroupID, t.Name)
	return
}

func (t *FlashNodeTopology) removeFlashNodesFromTargetZone(zoneName string, count int,
	flashGroup *FlashGroup, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
) (err error) {
	flashNodeHosts := flashGroup.GetTargetZoneFlashNodeHosts(zoneName)
	if len(flashNodeHosts) < count {
		return fmt.Errorf("flashNodeHostsCount:%v less than expectCount:%v,flashNodeHosts:%v", len(flashNodeHosts), count, flashNodeHosts)
	}
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = t.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup, syncUpdateFlashNodeFunc); err != nil {
			err = fmt.Errorf("successHost:%v, flashNodeHosts:%v err:%v", successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
		if len(successHost) >= count {
			break
		}
	}
	log.LogInfo(fmt.Sprintf("action[removeFlashNodesFromTargetZone] flashGroup:%v successHost:%v", flashGroup.ID, successHost))
	return
}

func (t *FlashNodeTopology) GetAllFlashNodesView() (flashNodes []proto.NodeView) {
	flashNodes = make([]proto.NodeView, 0)
	t.flashNodeMap.Range(func(addr, node interface{}) bool {
		flashNode := node.(*FlashNode)
		isWritable := flashNode.isWriteable()
		flashNode.RLock()
		flashNodes = append(flashNodes, proto.NodeView{
			ID:         flashNode.ID,
			Addr:       flashNode.Addr,
			Status:     flashNode.IsActive,
			IsWritable: isWritable,
		})
		flashNode.RUnlock()
		return true
	})
	return
}

func (t *FlashNodeTopology) SaveFlashGroup(group *FlashGroup) (err error) {
	if t.Region != group.Region {
		err = fmt.Errorf("top %v region[%v] is not equal to group[%v] region[%v]", t.Name, t.Region, group.ID, group.Region)
		log.LogWarnf("PutFlashNode: err %v", err)
		return
	}
	t.flashGroupMap.Store(group.ID, group)
	for _, slot := range group.Slots {
		t.slotsMap[slot] = group.ID
	}
	return
}

func (t *FlashNodeTopology) CreateFlashNodeHeartBeatTasks(leader string, handleReadTimeout, readDataNodeTimeout,
	hotKeyMissCount int, flashReadFlowLimit int64, flashWriteFlowLimit int64, flashKeyFlowLimit int64,
) []*proto.AdminTask {
	tasks := make([]*proto.AdminTask, 0)
	t.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		slots := make([]uint32, 0)
		if node.FlashGroupID != UnusedFlashNodeFlashGroupID {
			if valGroup, ok := t.flashGroupMap.Load(node.FlashGroupID); ok {
				slots = valGroup.(*FlashGroup).GetSlots()
			}
		}

		task := node.createHeartbeatTask(leader, handleReadTimeout, readDataNodeTimeout, hotKeyMissCount,
			flashReadFlowLimit, flashWriteFlowLimit, flashKeyFlowLimit, slots)
		tasks = append(tasks, task)
		return true
	})
	return tasks
}

func (t *FlashNodeTopology) FindLowLoadNode() ([]string, map[string]int) {
	var (
		scanNodes = make([]string, 0)
		allNodes  = make(map[string]int)
	)

	t.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		allNodes[node.Addr] = node.TaskCountLimit
		if node.WorkRole == proto.FlashNodeTaskWorker {
			scanNodes = append(scanNodes, node.Addr)
		}
		return true
	})
	return scanNodes, allNodes
}

func (t *FlashNodeTopology) GetFlashNode(addr string) (*FlashNode, bool) {
	value, ok := t.flashNodeMap.Load(addr)
	if !ok {
		return nil, false
	}
	return value.(*FlashNode), ok
}

func (t *FlashNodeTopology) CheckForActiveNode() (exists bool) {
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if fg.GetStatus().IsActive() {
			hosts := fg.GetFlashNodeHosts(true)
			if len(hosts) > 0 {
				exists = true
				return false
			}
		}
		return true
	})
	return
}

func (t *FlashNodeTopology) Load() (err error) {
	t.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.Lock()
		if gid := node.FlashGroupID; gid != UnusedFlashNodeFlashGroupID {
			if g, e := t.GetFlashGroup(gid); e == nil {
				err = g.putFlashNode(node)
				if err != nil {
					return false
				}
				log.LogInfof("action[loadFlashTopology] load FlashNode[%s] -> FlashGroup[%d]", node.Addr, gid)
			} else {
				node.FlashGroupID = UnusedFlashNodeFlashGroupID
				log.LogErrorf("action[loadFlashTopology] FlashNode[flashNodeId:%v addr:%s flashGroupId:%v] err:%v", node.ID, node.Addr, node.FlashGroupID, e.Error())
			}
		}
		node.Unlock()
		return true
	})
	if err != nil {
		log.LogErrorf("action[loadFlashTopology]topo %v load failed err:%v", t.Name, err.Error())
		return
	}
	return
}

func (t *FlashNodeTopology) UpdateFlashGroupSlots(clusterName string, idleTopo *FlashNodeTopology, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc, syncAddFlashNodeFunc SyncAddFlashNodeFunc,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) {
	isNotUpdated := true
	t.flashGroupMap.Range(func(key, value interface{}) bool {
		flashGroup := value.(*FlashGroup)
		t.createFlashGroupLock.Lock()
		defer t.createFlashGroupLock.Unlock()
		slotStatus := flashGroup.GetSlotStatus()
		if slotStatus == proto.SlotStatus_Creating && (!flashGroup.GetStatus().IsActive() || len(flashGroup.flashNodes) == 0) {
			return true
		}
		if slotStatus == proto.SlotStatus_Completed {
			return true
		} else if slotStatus == proto.SlotStatus_Creating || slotStatus == proto.SlotStatus_Deleting {
			if err := t.updateFlashGroupSlots(clusterName, idleTopo, flashGroup, syncDeleteFlashGroupFunc, syncUpdateFlashGroupFunc,
				syncUpdateFlashNodeFunc, syncDeleteFlashNodeFunc, syncAddFlashNodeFunc, syncMoveFlashNodeFunc); err == nil {
				isNotUpdated = false
			}
			return true
		} else {
			log.LogWarnf("scheduleToUpdateFlashGroupSlots failed, flashGroup(%v) has unknown SlotStatus(%v)", flashGroup.ID, flashGroup.SlotStatus)
			return true
		}
	})
	if !isNotUpdated {
		t.UpdateClientCache()
	}
}

func (t *FlashNodeTopology) updateFlashGroupSlots(clusterName string, idleTopo *FlashNodeTopology, flashGroup *FlashGroup, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc, syncAddFlashNodeFunc SyncAddFlashNodeFunc,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (err error) {
	var needDeleteFgFlag bool

	if flashGroup.GetSlotStatus() == proto.SlotStatus_Deleting {
		if needDeleteFgFlag, err = t.checkShrinkOrDeleteFlashGroup(clusterName, idleTopo, flashGroup, syncUpdateFlashNodeFunc,
			syncDeleteFlashNodeFunc, syncAddFlashNodeFunc, syncMoveFlashNodeFunc); err != nil {
			return
		}
	}
	return flashGroup.UpdateSlots(t, needDeleteFgFlag, syncDeleteFlashGroupFunc, syncUpdateFlashGroupFunc)
}

func (t *FlashNodeTopology) checkShrinkOrDeleteFlashGroup(clusterName string, idleTopo *FlashNodeTopology, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc, syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
	syncAddFlashNodeFunc SyncAddFlashNodeFunc, syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (needDeleteFgFlag bool, err error) {
	leftPendingSlotsNum := uint32(flashGroup.GetPendingSlotsCount()) - flashGroup.Step
	if (leftPendingSlotsNum <= 0) && (flashGroup.GetPendingSlotsCount() == flashGroup.GetSlotsCount()) {
		needDeleteFgFlag = true
		// if slots num is reduced to 0, the fn of fg need to be removed
		if err = t.removeAllFlashNodeFromFlashGroup(clusterName, idleTopo, flashGroup, syncUpdateFlashNodeFunc,
			syncDeleteFlashNodeFunc, syncAddFlashNodeFunc, syncMoveFlashNodeFunc); err != nil {
			return
		}
	}
	return
}

func (t *FlashNodeTopology) BadDiskInfos() []*FlashNodeBadDiskInfo {
	infos := make([]*FlashNodeBadDiskInfo, 0)
	t.flashNodeMap.Range(func(addr, node interface{}) bool {
		flashNode, ok := node.(*FlashNode)
		if !ok {
			return true
		}
		for _, disk := range flashNode.DiskStat {
			if disk.Status == proto.Unavailable {
				infos = append(infos, &FlashNodeBadDiskInfo{Addr: flashNode.Addr, DiskPath: disk.DataPath})
			}
		}
		return true
	})
	return infos
}

func (t *FlashNodeTopology) GetFlashTopoAdminView() (ftv *proto.FlashTopologyAdminView) {
	var (
		volsMap = make(map[string]struct{})
		vols    = make([]string, 0)
	)
	t.flashNodeMap.Range(func(addr, node interface{}) bool {
		flashNode, ok := node.(*FlashNode)
		if !ok {
			return true
		}
		for _, volName := range flashNode.CacheVols {
			if _, ok := volsMap[volName]; !ok {
				volsMap[volName] = struct{}{}
			}
		}
		return true
	})
	for volName := range volsMap {
		vols = append(vols, volName)
	}
	ddt := ""
	if !t.DeleteExecTime.IsZero() {
		ddt = t.DeleteExecTime.Format(proto.TimeFormat) // 或 .String()
	}
	ftv = &proto.FlashTopologyAdminView{
		ID: t.ID, Name: t.Name, CacheVols: vols, Region: t.Region,
		Status: t.GetTopoStatusMsg(), DelayDeleteTime: ddt,
	}
	return ftv
}

func (t *FlashNodeTopology) DeleteAllFlashGroups(clusterName string, idleTopo *FlashNodeTopology, gradualFlag bool, step uint32,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc,
	syncAddFlashNodeFunc SyncAddFlashNodeFunc,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (err error) {
	t.flashGroupMap.Range(func(addr, value interface{}) bool {
		flashGroup, ok := value.(*FlashGroup)
		if !ok {
			return true
		}
		_, err = t.RemoveFlashGroup(clusterName, idleTopo, flashGroup.ID, gradualFlag, step, syncUpdateFlashGroupFunc,
			syncUpdateFlashNodeFunc, syncDeleteFlashGroupFunc, syncDeleteFlashNodeFunc, syncAddFlashNodeFunc,
			syncMoveFlashNodeFunc)
		if err != nil {
			log.LogWarnf("DeleteAllFlashGroups: topo(%v) delete flashGroup failed(%v)", t.Name, err.Error())
			return false
		}
		return true
	})
	return err
}

func (t *FlashNodeTopology) GetFlashNodes() (flashNodes []*FlashNode) {
	flashNodes = make([]*FlashNode, 0)
	t.flashNodeMap.Range(func(_, value interface{}) bool {
		flashNode, ok := value.(*FlashNode)
		if !ok {
			return true
		}
		flashNodes = append(flashNodes, flashNode)
		return true
	})
	return
}

func (t *FlashNodeTopology) Rename(newName string, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
) (err error) {
	// 1. change the topo name of flashnode
	t.flashNodeMap.Range(func(_, value interface{}) bool {
		flashNode, ok := value.(*FlashNode)
		if !ok {
			return true
		}
		flashNode.FlashNodeTopoName = newName
		err = syncUpdateFlashNodeFunc(flashNode)
		return err == nil
	})

	if err != nil {
		log.LogWarnf("Rename: update topo name for flash node  failed(%v)", err.Error())
		return
	}
	// 2. change the topo name of fg
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		flashGroup, ok := value.(*FlashGroup)
		if !ok {
			return true
		}
		flashGroup.FlashNodeTopoName = newName
		err = syncUpdateFlashGroupFunc(flashGroup)
		return err == nil
	})
	if err != nil {
		log.LogWarnf("Rename: update topo name for flash group  failed(%v)", err.Error())
		return
	}
	return
}

func (t *FlashNodeTopology) ChangeFlashNodeTopo(clusterName string, dstTop *FlashNodeTopology, fn *FlashNode,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc, syncAddFlashNodeFunc SyncAddFlashNodeFunc,
	syncMoveFlashNodeFunc SyncMoveFlashNodeFunc,
) (err error) {
	err = t.RemoveFlashNode(clusterName, fn, syncDeleteFlashNodeFunc)
	if err != nil {
		log.LogWarnf("ChangeFlashNodeTopo remove fn %v from topo %v failed: %v", fn.Addr, t.Name, err.Error())
		return
	}
	// keep node id
	allocateCommonIDFunc := func() (id uint64, err error) {
		return fn.ID, nil
	}
	_, err = dstTop.AddFlashNode(clusterName, fn.Addr, fn.ZoneName, fn.Version, fn.Region, 0,
		allocateCommonIDFunc, syncAddFlashNodeFunc, syncMoveFlashNodeFunc)
	if err != nil {
		log.LogWarnf("ChangeFlashNodeTopo add fn %v to topo %v failed: %v", fn.Addr, dstTop.Name, err.Error())
		return
	}
	return nil
}

func (t *FlashNodeTopology) MarkDelete(syncUpdateFlashTopoFunc SyncUpdateFlashTopoFunc, delayHour int64,
	gradualFlag bool, step uint32,
) (err error) {
	if !atomic.CompareAndSwapUint32(&t.Status, proto.TopoStatusNormal, proto.TopoStatusMarkDelete) {
		err = fmt.Errorf("wrong status: topo is now(%v)", t.GetTopoStatusMsg())
		return
	}
	t.DeleteExecTime = time.Now().Add(time.Duration(delayHour) * time.Hour)
	t.DeleteGradualFlag = gradualFlag
	t.DeleteStep = step
	err = syncUpdateFlashTopoFunc(t)
	log.LogDebugf("MarkDelete: mark topo %v markDeleted: err[%v]", t.Name, err)
	return
}

func (t *FlashNodeTopology) GetTopoStatusMsg() string {
	switch atomic.LoadUint32(&t.Status) {
	case proto.TopoStatusNormal:
		return "normal"
	case proto.TopoStatusMarkDelete:
		return "markDeleted"
	default:
		return "unkown"
	}
}

// IsMarkDelete reports whether the topology has been marked for delayed deletion.
func (t *FlashNodeTopology) IsMarkDelete() bool {
	return atomic.LoadUint32(&t.Status) == proto.TopoStatusMarkDelete
}
