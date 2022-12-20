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
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
	"hash/crc32"
	"sync"
)

type flashNodeTopology struct {
	flashNodesMutex sync.RWMutex
	flashNodeMap    *sync.Map //key: FlashNodeAddr, value: *FlashNode
	zoneMap         *sync.Map //key: zoneName, value: *FlashNodeZone

	flashGroupMap        *sync.Map         //key: FlashGroupID, value: *FlashGroup
	slotsMap             map[uint32]uint64 //key:slot, value: FlashGroupID
	createFlashGroupLock sync.RWMutex      //create/delete flashGroup
}

type FlashNodeZone struct {
	name           string
	flashNode      *sync.Map
	flashNodesLock sync.RWMutex
}

func newFlashNodeZone(name string) (zone *FlashNodeZone) {
	zone = &FlashNodeZone{name: name}
	zone.flashNode = new(sync.Map)
	return
}

func newFlashNodeTopology() (t *flashNodeTopology) {
	t = new(flashNodeTopology)
	t.zoneMap = new(sync.Map)
	t.flashNodeMap = new(sync.Map)
	t.flashGroupMap = new(sync.Map)
	t.slotsMap = make(map[uint32]uint64, 0)
	return
}

func (t *flashNodeTopology) clear() {
	t.flashGroupMap.Range(func(key, value interface{}) bool {
		t.flashGroupMap.Delete(key)
		return true
	})
	t.zoneMap.Range(func(key, value interface{}) bool {
		t.zoneMap.Delete(key)
		return true
	})
	t.flashNodeMap.Range(func(key, value interface{}) bool {
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
		return nil, fmt.Errorf("zone name is empty")
	}
	zone = value.(*FlashNodeZone)
	if zone == nil {
		return nil, fmt.Errorf("zone[%v] is not found", name)
	}
	return
}

func (t *flashNodeTopology) putZoneIfAbsent(zone *FlashNodeZone) (beStoredZone *FlashNodeZone) {
	oldZone, ok := t.zoneMap.Load(zone.name)
	if ok {
		return oldZone.(*FlashNodeZone)
	}
	t.zoneMap.Store(zone.name, zone)
	beStoredZone = zone
	return
}

func (t *flashNodeTopology) putFlashNode(flashNode *FlashNode) (err error) {
	if _, ok := t.flashNodeMap.Load(flashNode.Addr); ok {
		return
	}
	t.flashNodeMap.Store(flashNode.Addr, flashNode)
	zone, err := t.getZone(flashNode.ZoneName)
	if err != nil {
		return
	}
	zone.putFlashNode(flashNode)
	return
}

func (zone *FlashNodeZone) putFlashNode(flashNode *FlashNode) {
	zone.flashNode.Store(flashNode.Addr, flashNode)
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

func (t *flashNodeTopology) adjustFlashNode(flashNode *FlashNode, c *Cluster) {
	t.flashNodesMutex.Lock()
	defer t.flashNodesMutex.Unlock()
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogError(fmt.Sprintf("action[adjustFlashNode] err:%v", err))
		}
	}()
	_, err = t.getZone(flashNode.ZoneName)
	if err != nil {
		t.putZoneIfAbsent(newFlashNodeZone(flashNode.ZoneName))
	}
	if err = c.syncUpdateFlashNode(flashNode); err != nil {
		return
	}
	err = t.putFlashNode(flashNode)
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

// the function caller should use flashNodesLock of FlashNodeZone
func (zone *FlashNodeZone) selectFlashNodes(count int, excludeHosts []string) (newHosts []string, err error) {
	zone.flashNode.Range(func(_, value interface{}) bool {
		flashNode := value.(*FlashNode)
		if contains(excludeHosts, flashNode.Addr) == true {
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
