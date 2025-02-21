// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

type flashNodeTopology struct {
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

type FlashNodeZone struct {
	mu        sync.RWMutex
	name      string
	flashNode sync.Map // key: FlashNodeAddr, value: *FlashNode
}

func (zone *FlashNodeZone) putFlashNode(flashNode *FlashNode) {
	zone.flashNode.Store(flashNode.Addr, flashNode)
}

// the function caller should use lock of FlashNodeZone
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

func newFlashNodeZone(name string) (zone *FlashNodeZone) {
	return &FlashNodeZone{name: name}
}

func newFlashNodeTopology() (t *flashNodeTopology) {
	empty, err := json.Marshal(newSuccessHTTPReply(proto.FlashGroupView{}))
	if err != nil {
		panic(fmt.Sprintf("action[newFlashNodeTopology] json marshal %v", err))
	}
	t = &flashNodeTopology{
		slotsMap:       make(map[uint32]uint64),
		clientEmpty:    empty,
		clientUpdateCh: make(chan struct{}, 1),
	}
	t.clientOff.Store([]byte(nil))
	t.clientCache.Store([]byte(nil))
	return t
}

func (t *flashNodeTopology) clear() {
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

func (t *flashNodeTopology) getZone(name string) (zone *FlashNodeZone, err error) {
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

func (t *flashNodeTopology) putZoneIfAbsent(zone *FlashNodeZone) (old *FlashNodeZone) {
	oldZone, loaded := t.zoneMap.LoadOrStore(zone.name, zone)
	if loaded {
		return oldZone.(*FlashNodeZone)
	}
	return zone
}

func (t *flashNodeTopology) putFlashNode(flashNode *FlashNode) (err error) {
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

func (t *flashNodeTopology) deleteFlashNode(flashNode *FlashNode) {
	t.flashNodeMap.Delete(flashNode.Addr)
	zone, err := t.getZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.flashNode.Delete(flashNode.Addr)
}

// the function caller should use createFlashGroupLock
func (t *flashNodeTopology) allocateNewSlotsForCreateFlashGroup(fgID uint64, setSlots []uint32, weight uint32) (slots []uint32) {
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

func allocateNewSlot() (slot uint32) {
	bytes, _ := uuid.New().MarshalBinary()
	slot = crc32.ChecksumIEEE(bytes)
	return
}

func (t *flashNodeTopology) removeSlots(slots []uint32) {
	for _, slot := range slots {
		delete(t.slotsMap, slot)
	}
}

func (t *flashNodeTopology) createFlashGroup(fgID uint64, c *Cluster, setSlots []uint32, setWeight uint32) (flashGroup *FlashGroup, err error) {
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

func (t *flashNodeTopology) gradualCreateFlashGroup(fgID uint64, c *Cluster, setSlots []uint32, setWeight uint32, step uint32) (flashGroup *FlashGroup, err error) {
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

func (t *flashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, c *Cluster) (err error) {
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

func (t *flashNodeTopology) gradualRemoveFlashGroup(flashGroup *FlashGroup, c *Cluster, step uint32) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()

	return t.gradualShrinkFlashGroupSlots(flashGroup, c, flashGroup.getSlots(), step)
}

func (t *flashNodeTopology) gradualExpandFlashGroupSlots(flashGroup *FlashGroup, c *Cluster, pendingSlots []uint32, step uint32) (err error) { //nolint:unused
	flashGroup.lock.Lock()
	oldSlotStatus := flashGroup.SlotStatus
	oldStep := flashGroup.Step
	oldPendingSlots := flashGroup.PendingSlots
	flashGroup.SlotStatus = proto.SlotStatus_Creating
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

func (t *flashNodeTopology) gradualShrinkFlashGroupSlots(flashGroup *FlashGroup, c *Cluster, pendingSlots []uint32, step uint32) (err error) {
	flashGroup.lock.Lock()
	oldSlotStatus := flashGroup.SlotStatus
	oldStep := flashGroup.Step
	oldPendingSlots := flashGroup.PendingSlots
	flashGroup.SlotStatus = proto.SlotStatus_Deleting
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

func (t *flashNodeTopology) getFlashGroup(fgID uint64) (flashGroup *FlashGroup, err error) {
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

func (t *flashNodeTopology) getFlashGroupView() (fgv *proto.FlashGroupView) {
	fgv = new(proto.FlashGroupView)
	fgv.Enable = true
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if fg.GetStatus().IsActive() {
			hosts := fg.getFlashNodeHosts(true)
			if len(hosts) == 0 {
				return true
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

func (t *flashNodeTopology) getFlashGroupsAdminView(fgStatus proto.FlashGroupStatus, allStatus bool) (fgv *proto.FlashGroupsAdminView) {
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

func (t *flashNodeTopology) updateClientCache() {
	select {
	case t.clientUpdateCh <- struct{}{}:
	default:
	}
}

func (t *flashNodeTopology) getClientResponse() []byte {
	if cache := t.clientOff.Load().([]byte); len(cache) > 0 {
		return cache
	}
	if cache := t.clientCache.Load().([]byte); len(cache) > 0 {
		return cache
	}
	return t.updateClientResponse()
}

func (t *flashNodeTopology) updateClientResponse() []byte {
	cache, err := json.Marshal(newSuccessHTTPReply(t.getFlashGroupView()))
	if err != nil {
		log.LogError("action[updateClientResponse] json marshal", err)
		return nil
	}
	t.clientCache.Store(cache)
	return cache[:]
}

func (c *Cluster) loadFlashNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashNodes],err:%v", err.Error())
		return
	}

	for _, value := range result {
		fnv := &flashNodeValue{}
		if err = json.Unmarshal(value, fnv); err != nil {
			err = fmt.Errorf("action[loadFlashNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashNode := newFlashNode(fnv.Addr, fnv.ZoneName, c.Name, fnv.Version, fnv.IsEnable)
		flashNode.ID = fnv.ID
		// load later in loadFlashTopology
		flashNode.FlashGroupID = fnv.FlashGroupID

		_, err = c.flashNodeTopo.getZone(flashNode.ZoneName)
		if err != nil {
			c.flashNodeTopo.putZoneIfAbsent(newFlashNodeZone(flashNode.ZoneName))
			err = nil
		}
		c.flashNodeTopo.putFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes], flashNode[flashNodeId:%v addr:%s flashGroupId:%v]", flashNode.ID, flashNode.Addr, flashNode.FlashGroupID)
	}
	return
}

func (c *Cluster) loadFlashGroups() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashGroupPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashGroups],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		var fgv flashGroupValue
		if err = json.Unmarshal(value, &fgv); err != nil {
			err = fmt.Errorf("action[loadFlashGroups],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashGroup := newFlashGroup(fgv.ID, fgv.Slots, fgv.SlotStatus, fgv.PendingSlots, fgv.Step, fgv.Status, fgv.Weight)
		c.flashNodeTopo.flashGroupMap.Store(flashGroup.ID, flashGroup)
		for _, slot := range flashGroup.Slots {
			c.flashNodeTopo.slotsMap[slot] = flashGroup.ID
		}
		log.LogInfof("action[loadFlashGroups],flashGroup[%v]", flashGroup.ID)
	}
	return
}

func (c *Cluster) loadFlashTopology() (err error) {
	c.flashNodeTopo.flashNodeMap.Range(func(addr, flashNode interface{}) bool {
		node := flashNode.(*FlashNode)
		node.Lock()
		if gid := node.FlashGroupID; gid != unusedFlashNodeFlashGroupID {
			if g, e := c.flashNodeTopo.getFlashGroup(gid); e == nil {
				g.putFlashNode(node)
				log.LogInfof("action[loadFlashTopology] load FlashNode[%s] -> FlashGroup[%d]", node.Addr, gid)
			} else {
				node.FlashGroupID = unusedFlashNodeFlashGroupID
				log.LogErrorf("action[loadFlashTopology] FlashNode[flashNodeId:%v addr:%s flashGroupId:%v] err:%v", node.ID, node.Addr, node.FlashGroupID, e.Error())
			}
		}
		node.Unlock()
		return true
	})
	return
}

func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		dur := time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.flashNodeTopo.updateClientResponse()
			}
			select {
			case <-c.stopc:
				return
			case <-c.flashNodeTopo.clientUpdateCh:
				ticker.Reset(dur)
			case <-ticker.C:
			}
		}
	}()
}

func getNewSlots(slots []uint32, pendingSlots []uint32, flag proto.SlotStatus) (newSlots []uint32) {
	if flag == proto.SlotStatus_Creating { // expand flashGroup slots
		newSlots = append(slots, pendingSlots...)
		sort.Slice(newSlots, func(i, j int) bool { return newSlots[i] < newSlots[j] })
		return
	} else { // shrink flashGroup slots
		slotMap := make(map[uint32]struct{})
		for _, val := range pendingSlots {
			if _, ok := slotMap[val]; !ok {
				slotMap[val] = struct{}{}
			}
		}
		for _, val := range slots {
			if _, ok := slotMap[val]; !ok {
				newSlots = append(newSlots, val)
			}
		}
		return
	}
}

func (c *Cluster) checkShrinkOrDeleteFlashGroup(flashGroup *FlashGroup) (needDeleteFgFlag bool, err error) {
	leftPendingSlotsNum := uint32(flashGroup.getPendingSlotsCount()) - flashGroup.Step
	if (leftPendingSlotsNum <= 0) && (flashGroup.getPendingSlotsCount() == flashGroup.getSlotsCount()) {
		needDeleteFgFlag = true
		// if slots num is reduced to 0, the fn of fg need to be removed
		if err = c.removeAllFlashNodeFromFlashGroup(flashGroup); err != nil {
			return
		}
	}
	return
}

func (c *Cluster) scheduleToUpdateFlashGroupSlots() {
	go func() {
		dur := time.Minute
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopc:
				return
			case <-ticker.C:
				if c.partition != nil && c.partition.IsRaftLeader() {
					isNotUpdated := true
					c.flashNodeTopo.flashGroupMap.Range(func(key, value interface{}) bool {
						flashGroup := value.(*FlashGroup)
						c.flashNodeTopo.createFlashGroupLock.Lock()
						defer c.flashNodeTopo.createFlashGroupLock.Unlock()
						slotStatus := flashGroup.getSlotStatus()
						if slotStatus == proto.SlotStatus_Completed {
							return true
						} else if slotStatus == proto.SlotStatus_Creating {
							var addedSlotsNum uint32
							var newSlotStatus proto.SlotStatus
							var newPendingSlots []uint32

							flashGroup.lock.Lock()
							leftPendingSlotsNum := uint32(len(flashGroup.PendingSlots)) - flashGroup.Step
							oldSlots := flashGroup.Slots
							oldPendingSlots := flashGroup.PendingSlots
							oldSlotStatus := flashGroup.SlotStatus
							if leftPendingSlotsNum > 0 { // previous steps
								addedSlotsNum = flashGroup.Step
								newPendingSlots = oldPendingSlots[addedSlotsNum:]
								newSlotStatus = proto.SlotStatus_Creating
							} else { // final step
								addedSlotsNum = uint32(len(flashGroup.PendingSlots))
								newPendingSlots = nil
								newSlotStatus = proto.SlotStatus_Completed
							}
							newSlots := getNewSlots(flashGroup.Slots, flashGroup.PendingSlots[:addedSlotsNum], proto.SlotStatus_Creating)
							flashGroup.Slots = newSlots
							flashGroup.PendingSlots = newPendingSlots
							flashGroup.SlotStatus = newSlotStatus
							if err := c.syncUpdateFlashGroup(flashGroup); err != nil {
								flashGroup.Slots = oldSlots
								flashGroup.PendingSlots = oldPendingSlots
								flashGroup.SlotStatus = oldSlotStatus
								flashGroup.lock.Unlock()
								return true
							}
							flashGroup.lock.Unlock()
							for _, slot := range oldPendingSlots[:addedSlotsNum] {
								c.flashNodeTopo.slotsMap[slot] = flashGroup.ID
							}
							isNotUpdated = false
							return true
						} else if slotStatus == proto.SlotStatus_Deleting {
							var deletedSlotsNum uint32
							var newSlotStatus proto.SlotStatus
							var newPendingSlots []uint32
							var needDeleteFgFlag bool
							var err error

							if needDeleteFgFlag, err = c.checkShrinkOrDeleteFlashGroup(flashGroup); err != nil {
								return true
							}
							flashGroup.lock.Lock()
							leftPendingSlotsNum := uint32(len(flashGroup.PendingSlots)) - flashGroup.Step
							oldSlots := flashGroup.Slots
							oldPendingSlots := flashGroup.PendingSlots
							oldSlotStatus := flashGroup.SlotStatus
							oldStatus := flashGroup.Status
							if leftPendingSlotsNum > 0 { // previous steps
								deletedSlotsNum = flashGroup.Step
								newPendingSlots = oldPendingSlots[deletedSlotsNum:]
								newSlotStatus = proto.SlotStatus_Deleting
							} else { // final step
								deletedSlotsNum = uint32(len(flashGroup.PendingSlots))
								newPendingSlots = nil
								newSlotStatus = proto.SlotStatus_Completed
							}
							newSlots := getNewSlots(flashGroup.Slots, flashGroup.PendingSlots[:deletedSlotsNum], proto.SlotStatus_Deleting)
							flashGroup.Slots = newSlots
							flashGroup.PendingSlots = newPendingSlots
							flashGroup.SlotStatus = newSlotStatus
							if needDeleteFgFlag {
								flashGroup.Status = proto.FlashGroupStatus_Inactive
							}
							if err := c.syncUpdateFlashGroup(flashGroup); err != nil {
								flashGroup.Slots = oldSlots
								flashGroup.PendingSlots = oldPendingSlots
								flashGroup.SlotStatus = oldSlotStatus
								flashGroup.PendingSlots = oldPendingSlots
								if needDeleteFgFlag {
									flashGroup.Status = oldStatus
								}
								flashGroup.lock.Unlock()
								return true
							}
							flashGroup.lock.Unlock()
							c.flashNodeTopo.removeSlots(oldPendingSlots[:deletedSlotsNum])
							if needDeleteFgFlag {
								c.flashNodeTopo.flashGroupMap.Delete(flashGroup.ID)
							}
							isNotUpdated = false
							return true
						} else {
							log.LogWarnf("scheduleToUpdateFlashGroupSlots failed, flashGroup(%v) has unknown SlotStatus(%v)", flashGroup.ID, flashGroup.SlotStatus)
							return true
						}
					})
					if !isNotUpdated {
						c.flashNodeTopo.updateClientCache()
					}
				}
			}
		}
	}()
}
