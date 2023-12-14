// Copyright 2018 The CubeFS Authors.
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
	zone.flashNode.Range(func(_, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if contains(excludeHosts, flashNode.Addr) {
			return true
		}
		if flashNode.isWriteAble() {
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
	t.flashNodeMap.Range(func(key, _ interface{}) bool {
		t.flashNodeMap.Delete(key)
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

// TODO: remove from group.
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
	slots = make([]uint32, 0, defaultFlashGroupSlotsCount)
	if len(setSlots) != 0 {
		slots = append(slots, setSlots...)
	}
	for len(slots) < defaultFlashGroupSlotsCount {
		slot := allocateNewSlot()
		if _, ok := t.slotsMap[slot]; ok {
			continue
		}
		slots = append(slots, slot)
		t.slotsMap[slot] = fgID
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
	return
}

func (t *flashNodeTopology) createFlashGroup(fgID uint64, c *Cluster, setSlots []uint32) (flashGroup *FlashGroup, err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := t.allocateNewSlotsForCreateFlashGroup(fgID, setSlots)
	flashGroup = newFlashGroup(fgID, slots, proto.FlashGroupStatus_Inactive)
	if err = c.syncAddFlashGroup(flashGroup); err != nil {
		t.removeSlots(slots)
		return
	}
	t.flashGroupMap.Store(flashGroup.ID, flashGroup)
	return
}

func (t *flashNodeTopology) removeFlashGroup(flashGroup *FlashGroup, c *Cluster) (err error) {
	t.createFlashGroupLock.Lock()
	defer t.createFlashGroupLock.Unlock()
	slots := flashGroup.Slots
	oldStatus := flashGroup.Status

	flashGroup.Status = proto.FlashGroupStatus_Inactive
	if err = c.syncDeleteFlashGroup(flashGroup); err != nil {
		flashGroup.Status = oldStatus
		return
	}
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
		if fg.Status.IsActive() {
			hosts := fg.getFlashNodeHosts(true)
			if len(hosts) == 0 {
				return true
			}
			fgv.FlashGroups = append(fgv.FlashGroups, &proto.FlashGroupInfo{
				ID:    fg.ID,
				Slot:  fg.Slots,
				Hosts: hosts,
			})
			//for _, slot := range fg.Slots {
			//	fgv.FlashGroups = append(fgv.FlashGroups, proto.FlashGroupInfo{
			//		ID:    fg.ID,
			//		Slot:  slot,
			//		Hosts: hosts,
			//	})
			//}
		}
		return true
	})
	//sort.Slice(fgv.FlashGroups, func(i, j int) bool {
	//	return fgv.FlashGroups[i].Slot < fgv.FlashGroups[j].Slot
	//})
	return
}

func (t *flashNodeTopology) getFlashGroupsAdminView(fgStatus proto.FlashGroupStatus, allStatus bool) (fgv *proto.FlashGroupsAdminView) {
	fgv = new(proto.FlashGroupsAdminView)
	t.flashGroupMap.Range(func(_, value interface{}) bool {
		fg := value.(*FlashGroup)
		if allStatus || fg.Status == fgStatus {
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
		flashNode.FlashGroupID = fnv.FlashGroupID

		if !flashNode.isFlashNodeUnused() {
			if flashGroup, err1 := c.flashNodeTopo.getFlashGroup(flashNode.FlashGroupID); err1 == nil {
				flashGroup.putFlashNode(flashNode)
			} else {
				log.LogErrorf("action[loadFlashNodes]fnv:%v err:%v", *fnv, err1.Error())
			}
		}

		_, err = c.flashNodeTopo.getZone(flashNode.ZoneName)
		if err != nil {
			c.flashNodeTopo.putZoneIfAbsent(newFlashNodeZone(flashNode.ZoneName))
		}
		c.flashNodeTopo.putFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes],flashNode[%v],FlashGroupID[%v]", flashNode.Addr, flashNode.FlashGroupID)
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
