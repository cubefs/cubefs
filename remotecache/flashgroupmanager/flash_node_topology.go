package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

const (
	defaultFlashGroupSlotsCount = 32
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

	clientEmpty    []byte        // empty response cache
	clientOff      atomic.Value  // []byte, default nil (on)
	clientCache    atomic.Value  // []byte
	clientUpdateCh chan struct{} // update client response cache
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

func (t *FlashNodeTopology) gradualCreateFlashGroup(fgID uint64, c *Cluster, setSlots []uint32, setWeight uint32, step uint32) (flashGroup *FlashGroup, err error) {
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
	if err = c.syncAddFlashGroup(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}

	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	for _, slot := range slots[:addedSlotsNum] {
		t.slotsMap[slot] = flashGroup.ID
	}
	return
}

func (t *FlashNodeTopology) createFlashGroup(fgID uint64, c *Cluster, setSlots []uint32, setWeight uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots, setWeight)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	flashGroup = newFlashGroup(fgID, slots, proto.SlotStatus_Completed, make([]uint32, 0), 0, proto.FlashGroupStatus_Inactive, setWeight)

	if err = c.syncAddFlashGroup(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}
	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	for _, slot := range slots {
		t.slotsMap[slot] = flashGroup.ID
	}
	return
}

func (t *FlashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, c *Cluster) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	flashGroup.lock.Lock()
	slots := flashGroup.Slots
	oldStatus := flashGroup.Status
	flashGroup.Status = proto.FlashGroupStatus_Inactive
	if err = c.syncDeleteFlashGroup(flashGroup); err != nil {
		flashGroup.Status = oldStatus
		flashGroup.lock.Unlock()
		return
	}
	flashGroup.lock.Unlock()

	t.removeSlots(slots)
	t.flashGroupMap.Delete(flashGroup.ID)
	return
}

func (t *FlashNodeTopology) getClientResponse() []byte {
	if cache := t.clientOff.Load().([]byte); len(cache) > 0 {
		return cache
	}
	if cache := t.clientCache.Load().([]byte); len(cache) > 0 {
		return cache
	}
	return t.updateClientResponse()
}

func (t *FlashNodeTopology) updateClientResponse() []byte {
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
			hosts := fg.getFlashNodeHostsEnableAndActive()
			if len(hosts) == 0 {
				if log.EnableInfo() {
					log.LogInfof("fg(%v) lost all flashnodes", fg)
				}
				atomic.StoreInt32(&fg.LostAllFlashNode, 1)
				fg.ReduceSlot()
			} else if len(fg.ReservedSlots) > 0 {
				fg.lock.Lock()
				fg.Slots = append(fg.Slots, fg.ReservedSlots...)
				fg.ReservedSlots = make([]uint32, 0)
				fg.lock.Unlock()
				atomic.StoreInt32(&fg.LostAllFlashNode, 0)
				atomic.StoreInt32(&fg.SlotChanged, 1)
			}

			if len(fg.Slots) == 0 {
				disableFlashGroupNum++
				if disableFlashGroupNum >= maxDisableFlashGroupCount && len(fg.ReservedSlots) > 0 {
					fg.lock.Lock()
					fg.Slots = append(fg.Slots, fg.ReservedSlots...)
					fg.ReservedSlots = make([]uint32, 0)
					fg.lock.Unlock()
					atomic.StoreInt32(&fg.SlotChanged, 1)
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

func (t *FlashNodeTopology) getFlashGroup(fgID uint64) (flashGroup *FlashGroup, err error) {
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

func (t *FlashNodeTopology) getFlashGroupsAdminView(fgStatus proto.FlashGroupStatus, allStatus bool) (fgv *proto.FlashGroupsAdminView) {
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

func (t *FlashNodeTopology) gradualRemoveFlashGroup(flashGroup *FlashGroup, c *Cluster, step uint32) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	return t.gradualExpandOrShrinkFlashGroupSlots(flashGroup, c, proto.SlotStatus_Deleting, flashGroup.getSlots(), step)
}

func (t *FlashNodeTopology) gradualExpandOrShrinkFlashGroupSlots(flashGroup *FlashGroup, c *Cluster, newSlotStatus proto.SlotStatus, pendingSlots []uint32, step uint32) (err error) {
	flashGroup.lock.Lock()
	oldSlotStatus := flashGroup.SlotStatus
	oldStep := flashGroup.Step
	oldPendingSlots := flashGroup.PendingSlots
	flashGroup.SlotStatus = newSlotStatus
	flashGroup.PendingSlots = pendingSlots
	flashGroup.Step = step
	if err = c.syncUpdateFlashGroup(flashGroup); err != nil {
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

func (t *FlashNodeTopology) getZone(name string) (zone *FlashNodeZone, err error) {
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

func (t *FlashNodeTopology) putZoneIfAbsent(zone *FlashNodeZone) (old *FlashNodeZone) {
	oldZone, loaded := t.zoneMap.LoadOrStore(zone.name, zone)
	if loaded {
		return oldZone.(*FlashNodeZone)
	}
	return zone
}

func (t *FlashNodeTopology) putFlashNode(flashNode *FlashNode) (err error) {
	if _, loaded := t.flashNodeMap.LoadOrStore(flashNode.Addr, flashNode); loaded {
		return
	}
	zone, err := t.getZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.putFlashNode(flashNode)
	return
}

func (t *FlashNodeTopology) deleteFlashNode(flashNode *FlashNode) {
	t.flashNodeMap.Delete(flashNode.Addr)
	zone, err := t.getZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.flashNode.Delete(flashNode.Addr)
}

func (t *FlashNodeTopology) clear() {
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
