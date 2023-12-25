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
		if flashNode.isWriteable() {
			newHosts = append(newHosts, flashNode.Addr)
		}
		if len(newHosts) >= count {
			return false
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
	return &flashNodeTopology{slotsMap: make(map[uint32]uint64)}
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
func (t *flashNodeTopology) allocateNewSlotsForCreateFlashGroup(fgID uint64, setSlots []uint32) (slots []uint32) {
	slots = make([]uint32, 0, len(setSlots))
	for _, slot := range setSlots {
		if _, ok := t.slotsMap[slot]; !ok {
			slots = append(slots, slot)
		}
	}
	if len(slots) > 0 {
		return
	}

	for len(slots) < defaultFlashGroupSlotsCount {
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

func (t *flashNodeTopology) createFlashGroup(fgID uint64, c *Cluster, setSlots []uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	flashGroup = newFlashGroup(fgID, slots, proto.FlashGroupStatus_Inactive)
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

func (t *flashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, c *Cluster) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := flashGroup.Slots

	flashGroup.lock.Lock()
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
		}
		c.flashNodeTopo.putFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes], flashNode[%s]", flashNode.Addr)
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
		flashGroup := newFlashGroup(fgv.ID, fgv.Slots, fgv.Status)
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
				log.LogErrorf("action[loadFlashTopology] FlashNode:%s err:%v", node.Addr, e.Error())
			}
		}
		node.Unlock()
		return true
	})
	return
}
