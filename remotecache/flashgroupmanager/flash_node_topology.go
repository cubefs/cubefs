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
	SyncAddFlashGroupFunc    func(flashGroup *FlashGroup) (err error)
	SyncDeleteFlashGroupFunc func(flashGroup *FlashGroup) (err error)
	SyncUpdateFlashGroupFunc func(flashGroup *FlashGroup) (err error)
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

func (zone *FlashNodeZone) selectFlashNodes(count int, excludeHosts []string) (newHosts []string, err error) {
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
		if flashNode.isWriteable() {
			newHosts = append(newHosts, flashNode.Addr)
		}
		return true
	})
	if len(newHosts) != count {
		return nil, fmt.Errorf("expect count:%v newHostsCount:%v,detail:%v,excludeHosts:%v", count, len(newHosts), newHosts, excludeHosts)
	}
	return
}

type FlashNodeTopology struct {
	mu sync.RWMutex

	createFlashGroupLock sync.RWMutex      // create/delete flashGroup
	slotsMap             map[uint32]uint64 // key:slot, value: FlashGroupID

	flashGroupMap sync.Map // key: FlashGroupID, value: *FlashGroup
	flashNodeMap  sync.Map // key: FlashNodeAddr, value: *FlashNode
	zoneMap       sync.Map // key: zoneName, value: *FlashNodeZone

	clientEmpty        []byte        // empty response cache
	clientOff          atomic.Value  // []byte, default nil (on)
	clientCache        atomic.Value  // []byte
	clientUpdateCh     chan struct{} // update client response cache
	SyncFlashGroupFunc SyncUpdateFlashGroupFunc
}

func NewFlashNodeTopology() (t *FlashNodeTopology) {
	empty, err := json.Marshal(newSuccessHTTPReply(proto.FlashGroupView{}))
	if err != nil {
		panic(fmt.Sprintf("action[NewFlashNodeTopology] json marshal %v", err))
	}
	t = &FlashNodeTopology{
		slotsMap:       make(map[uint32]uint64),
		clientEmpty:    empty,
		clientUpdateCh: make(chan struct{}, 1),
	}
	t.clientOff.Store([]byte(nil))
	t.clientCache.Store([]byte(nil))
	return t
}

func (t *FlashNodeTopology) gradualCreateFlashGroup(fgID uint64, syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	setSlots []uint32, setWeight uint32, step uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	var addedSlotsNum uint32
	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots, setWeight)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	remainingSlotsNum := uint32(len(slots)) - step
	if remainingSlotsNum > 0 {
		addedSlotsNum = step
		flashGroup = newFlashGroup(fgID, slots[:step], proto.SlotStatus_Creating, slots[step:], step, proto.FlashGroupStatus_Inactive, setWeight)
	} else {
		addedSlotsNum = uint32(len(slots))
		flashGroup = newFlashGroup(fgID, slots, proto.SlotStatus_Completed, make([]uint32, 0), 0, proto.FlashGroupStatus_Inactive, setWeight)
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
	setSlots []uint32, setWeight uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots, setWeight)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	flashGroup = newFlashGroup(fgID, slots, proto.SlotStatus_Completed, make([]uint32, 0), 0, proto.FlashGroupStatus_Inactive, setWeight)

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

func (t *FlashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc) (err error) {
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
		return nil, fmt.Errorf("flashGroup[%v] is not found", fgID)
	}
	flashGroup = value.(*FlashGroup)
	if flashGroup == nil {
		return nil, fmt.Errorf("flashGroup[%v] is not found", fgID)
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

func (t *FlashNodeTopology) gradualRemoveFlashGroup(flashGroup *FlashGroup,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, step uint32) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	return t.gradualExpandOrShrinkFlashGroupSlots(flashGroup, syncUpdateFlashGroupFunc, proto.SlotStatus_Deleting, flashGroup.GetSlots(), step)
}

func (t *FlashNodeTopology) gradualExpandOrShrinkFlashGroupSlots(flashGroup *FlashGroup,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, newSlotStatus proto.SlotStatus,
	pendingSlots []uint32, step uint32) (err error) {
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

func (t *FlashNodeTopology) updateClientCache() {
	select {
	case t.clientUpdateCh <- struct{}{}:
	default:
	}
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
	if _, loaded := t.flashNodeMap.LoadOrStore(flashNode.Addr, flashNode); loaded {
		return
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
	t.flashNodeMap.Range(func(key, node interface{}) bool {
		t.flashNodeMap.Delete(key)
		flashNode := node.(*FlashNode)
		flashNode.clean()
		return true
	})
	t.clientCache.Store([]byte(nil))
}

func (t *FlashNodeTopology) AddFlashNode(clusterName, nodeAddr, zoneName, version string,
	allocateCommonIDFunc AllocateCommonIDFunc, syncAddFlashNodeFunc SyncAddFlashNodeFunc) (id uint64, err error) {
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
		if err != nil {
			log.LogErrorf("action[addFlashNode],clusterID[%v] Addr:%v err:%v ", clusterName, nodeAddr, err.Error())
		}
	}()

	var flashNode *FlashNode
	flashNode, err = t.PeekFlashNode(nodeAddr)
	if err == nil {
		return flashNode.ID, nil
	}
	flashNode = NewFlashNode(nodeAddr, zoneName, clusterName, version, true)
	_, err = t.GetZone(zoneName)
	if err != nil {
		t.PutZoneIfAbsent(NewFlashNodeZone(zoneName))
	}
	if id, err = allocateCommonIDFunc(); err != nil {
		return
	}
	flashNode.ID = id
	if err = syncAddFlashNodeFunc(flashNode); err != nil {
		return
	}
	flashNode.ReportTime = time.Now()
	flashNode.IsActive = true
	if err = t.PutFlashNode(flashNode); err != nil {
		return
	}
	log.LogInfof("action[addFlashNode],clusterID[%v] Addr:%v ZoneName:%v success", clusterName, nodeAddr, zoneName)
	return
}

func (t *FlashNodeTopology) PeekFlashNode(addr string) (flashNode *FlashNode, err error) {
	value, ok := t.flashNodeMap.Load(addr)
	if !ok {
		err = errors.Trace(notFoundMsg(fmt.Sprintf("flashnode[%v]", addr)), "")
		return
	}
	flashNode = value.(*FlashNode)
	return
}

func (t *FlashNodeTopology) ListFlashNodes(showAll, active bool) map[string][]*proto.FlashNodeViewInfo {
	zoneFlashNodes := make(map[string][]*proto.FlashNodeViewInfo)
	t.flashNodeMap.Range(func(key, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if showAll || flashNode.isActiveAndEnable() == active {
			zoneFlashNodes[flashNode.ZoneName] = append(zoneFlashNodes[flashNode.ZoneName], flashNode.GetFlashNodeViewInfo())
		}
		return true
	})
	return zoneFlashNodes
}

func (t *FlashNodeTopology) UpdateFlashNode(flashNode *FlashNode, enable bool,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.IsEnable != enable {
		oldState := flashNode.IsEnable
		flashNode.IsEnable = enable
		if err = syncUpdateFlashNodeFunc(flashNode); err != nil {
			flashNode.IsEnable = oldState
			return
		}
		if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
			t.updateClientCache()
		}
	}
	return
}

func (t *FlashNodeTopology) RemoveFlashNode(clusterName string, flashNode *FlashNode,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc) (err error) {
	log.LogWarnf("action[removeFlashNode], ZoneName[%s] Node[%s] offline", flashNode.ZoneName, flashNode.Addr)
	var flashGroupID uint64
	if flashGroupID, err = t.deleteFlashNode(clusterName, flashNode, syncDeleteFlashNodeFunc); err != nil {
		return
	}
	if flashGroupID != UnusedFlashNodeFlashGroupID {
		var flashGroup *FlashGroup
		if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
			return
		}
		flashGroup.RemoveFlashNode(flashNode.Addr)
		t.updateClientCache()
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

func (t *FlashNodeTopology) deleteFlashNode(clusterName string, flashNode *FlashNode,
	syncDeleteFlashNodeFunc SyncDeleteFlashNodeFunc) (oldFlashGroupID uint64, err error) {
	flashNode.Lock()
	defer flashNode.Unlock()
	oldFlashGroupID = flashNode.FlashGroupID
	flashNode.FlashGroupID = UnusedFlashNodeFlashGroupID
	if err = syncDeleteFlashNodeFunc(flashNode); err != nil {
		log.LogErrorf("action[deleteFlashNode],clusterID[%v] node[%v] offline failed,err[%v]",
			clusterName, flashNode.Addr, err)
		flashNode.FlashGroupID = oldFlashGroupID
		return
	}
	// delFlashNodeFromCache
	t.flashNodeMap.Delete(flashNode.Addr)
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
	step uint32) (fg *FlashGroup, err error) {
	if gradualFlag {
		if fg, err = t.gradualCreateFlashGroup(id, syncUpdateFlashGroupFunc, setSlots, setWeight, step); err != nil {
			return
		}
	} else {
		if fg, err = t.createFlashGroup(id, syncAddFlashGroupFunc, setSlots, setWeight); err != nil {
			return
		}
	}
	t.updateClientCache()
	return
}

func (t *FlashNodeTopology) RemoveFlashGroup(id uint64, gradualFlag bool,
	step uint32, syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc,
	syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(id); err != nil {
		return
	}
	if flashGroup.GetSlotStatus() == proto.SlotStatus_Deleting {
		err = fmt.Errorf("the flashGroup(%v) is in slotDeleting status, it cannot be deleted repeatedly", flashGroup.ID)
		return
	}
	remainingSlotsNum := uint32(flashGroup.GetSlotsCount()) - step
	if gradualFlag && remainingSlotsNum > 0 {
		err = t.gradualRemoveFlashGroup(flashGroup, syncUpdateFlashGroupFunc, step)
		return
	}
	// remove flash nodes then del the flash group
	err = t.removeAllFlashNodeFromFlashGroup(flashGroup, syncUpdateFlashNodeFunc)
	if err != nil {
		return
	}
	err = t.removeFlashGroup(flashGroup, syncDeleteFlashGroupFunc)
	if err != nil {
		return
	}
	t.updateClientCache()
	return
}

func (t *FlashNodeTopology) removeAllFlashNodeFromFlashGroup(flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	flashNodeHosts := flashGroup.GetFlashNodeHosts(false)
	successHost := make([]string, 0)
	for _, flashNodeHost := range flashNodeHosts {
		if err = t.removeFlashNodeFromFlashGroup(flashNodeHost, flashGroup, syncUpdateFlashNodeFunc); err != nil {
			log.LogErrorf("remove flashNode from flashGroup failed, successHost:%v, flashNodeHosts:%v err:%v",
				successHost, flashNodeHosts, err)
			return
		}
		successHost = append(successHost, flashNodeHost)
	}
	log.LogInfof("action[RemoveAllFlashNodeFromFlashGroup] flashGroup:%v successHost:%v", flashGroup.ID, successHost)
	return
}

func (t *FlashNodeTopology) removeFlashNodeFromFlashGroup(addr string, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
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

func (t *FlashNodeTopology) FlashGroupAddFlashNode(flashGroupID uint64, addr string, zoneName string,
	count int, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
		return
	}
	if addr != "" {
		err = t.addFlashNodeToFlashGroup(addr, flashGroup, syncUpdateFlashNodeFunc)
	} else {
		err = t.selectFlashNodesFromZoneAddToFlashGroup(zoneName, count, nil, flashGroup, syncUpdateFlashNodeFunc)
	}
	if err != nil {
		return
	}
	t.updateClientCache()
	return
}

func (t *FlashNodeTopology) addFlashNodeToFlashGroup(addr string, flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	var flashNode *FlashNode
	if flashNode, err = t.setFlashNodeToFlashGroup(addr, flashGroup.ID, syncUpdateFlashNodeFunc); err != nil {
		return
	}
	flashGroup.putFlashNode(flashNode)
	return
}

func (t *FlashNodeTopology) setFlashNodeToFlashGroup(addr string, flashGroupID uint64,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (flashNode *FlashNode, err error) {
	if flashNode, err = t.PeekFlashNode(addr); err != nil {
		return
	}
	flashNode.Lock()
	defer flashNode.Unlock()
	if flashNode.FlashGroupID != UnusedFlashNodeFlashGroupID {
		err = fmt.Errorf("flashNode[%v] FlashGroupID[%v] can not add to flash group:%v", flashNode.Addr, flashNode.FlashGroupID, flashGroupID)
		return
	}
	if time.Since(flashNode.ReportTime) > DefaultNodeTimeoutDuration {
		flashNode.IsActive = false
		err = fmt.Errorf("flashNode[%v] is inactive lastReportTime:%v", flashNode.Addr, flashNode.ReportTime)
		return
	}
	oldFgID := flashNode.FlashGroupID
	flashNode.FlashGroupID = flashGroupID
	if err = syncUpdateFlashNodeFunc(flashNode); err != nil {
		flashNode.FlashGroupID = oldFgID
		return
	}
	log.LogInfo(fmt.Sprintf("action[setFlashNodeToFlashGroup] add flash node:%v to flashGroup:%v success", addr, flashGroupID))
	return
}

func (t *FlashNodeTopology) selectFlashNodesFromZoneAddToFlashGroup(zoneName string, count int, excludeHosts []string,
	flashGroup *FlashGroup, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	flashNodeZone, err := t.GetZone(zoneName)
	if err != nil {
		return
	}
	newHosts, err := flashNodeZone.selectFlashNodes(count, excludeHosts)
	if err != nil {
		return
	}
	successHost := make([]string, 0)
	for _, newHost := range newHosts {
		if err = t.addFlashNodeToFlashGroup(newHost, flashGroup, syncUpdateFlashNodeFunc); err != nil {
			err = fmt.Errorf("successHost:%v, newHosts:%v err:%v", successHost, newHosts, err)
			return
		}
		successHost = append(successHost, newHost)
	}
	log.LogInfo(fmt.Sprintf("action[selectFlashNodesFromZoneAddToFlashGroup] flashGroup:%v successHost:%v",
		flashGroup.ID, successHost))
	return
}

func (t *FlashNodeTopology) FlashGroupRemoveFlashNode(flashGroupID uint64, addr string, zoneName string,
	count int, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (flashGroup *FlashGroup, err error) {
	if flashGroup, err = t.GetFlashGroup(flashGroupID); err != nil {
		return
	}
	if addr != "" {
		err = t.removeFlashNodeFromFlashGroup(addr, flashGroup, syncUpdateFlashNodeFunc)
	} else {
		err = t.removeFlashNodesFromTargetZone(zoneName, count, flashGroup, syncUpdateFlashNodeFunc)
	}
	t.updateClientCache()
	return
}

func (t *FlashNodeTopology) removeFlashNodesFromTargetZone(zoneName string, count int,
	flashGroup *FlashGroup, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	flashNodeHosts := flashGroup.getTargetZoneFlashNodeHosts(zoneName)
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

func (t *FlashNodeTopology) GetAllFlashNodes() (flashNodes []proto.NodeView) {
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

func (t *FlashNodeTopology) ClientUpdateChannel() <-chan struct{} {
	return t.clientUpdateCh
}

func (t *FlashNodeTopology) SaveFlashGroup(group *FlashGroup) {
	t.flashGroupMap.Store(group.ID, group)
	for _, slot := range group.Slots {
		t.slotsMap[slot] = group.ID
	}
}

func (t *FlashNodeTopology) CreateFlashNodeHeartBeatTasks(leader string, handleReadTimeout, readDataNodeTimeout,
	hotKeyMissCount int, flashReadFlowLimit int64, flashWriteFlowLimit int64) []*proto.AdminTask {
	tasks := make([]*proto.AdminTask, 0)
	t.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.checkLiveliness()
		task := node.createHeartbeatTask(leader, handleReadTimeout, readDataNodeTimeout, hotKeyMissCount,
			flashReadFlowLimit, flashWriteFlowLimit)
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
				g.putFlashNode(node)
				log.LogInfof("action[loadFlashTopology] load FlashNode[%s] -> FlashGroup[%d]", node.Addr, gid)
			} else {
				node.FlashGroupID = UnusedFlashNodeFlashGroupID
				log.LogErrorf("action[loadFlashTopology] FlashNode[flashNodeId:%v addr:%s flashGroupId:%v] err:%v", node.ID, node.Addr, node.FlashGroupID, e.Error())
			}
		}
		node.Unlock()
		return true
	})
	return
}

func (t *FlashNodeTopology) UpdateFlashGroupSlots(syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) {
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
			if err := t.updateFlashGroupSlots(flashGroup, syncDeleteFlashGroupFunc, syncUpdateFlashGroupFunc, syncUpdateFlashNodeFunc); err == nil {
				isNotUpdated = false
			}
			return true
		} else {
			log.LogWarnf("scheduleToUpdateFlashGroupSlots failed, flashGroup(%v) has unknown SlotStatus(%v)", flashGroup.ID, flashGroup.SlotStatus)
			return true
		}
	})
	if !isNotUpdated {
		t.updateClientCache()
	}
}

func (t *FlashNodeTopology) updateFlashGroupSlots(flashGroup *FlashGroup, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (err error) {
	var needDeleteFgFlag bool

	if flashGroup.GetSlotStatus() == proto.SlotStatus_Deleting {
		if needDeleteFgFlag, err = t.checkShrinkOrDeleteFlashGroup(flashGroup, syncUpdateFlashNodeFunc); err != nil {
			return
		}
	}
	return flashGroup.UpdateSlots(t, needDeleteFgFlag, syncDeleteFlashGroupFunc, syncUpdateFlashGroupFunc)
}

func (t *FlashNodeTopology) checkShrinkOrDeleteFlashGroup(flashGroup *FlashGroup,
	syncUpdateFlashNodeFunc SyncUpdateFlashNodeFunc) (needDeleteFgFlag bool, err error) {
	leftPendingSlotsNum := uint32(flashGroup.GetPendingSlotsCount()) - flashGroup.Step
	if (leftPendingSlotsNum <= 0) && (flashGroup.GetPendingSlotsCount() == flashGroup.GetSlotsCount()) {
		needDeleteFgFlag = true
		// if slots num is reduced to 0, the fn of fg need to be removed
		if err = t.removeAllFlashNodeFromFlashGroup(flashGroup, syncUpdateFlashNodeFunc); err != nil {
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
