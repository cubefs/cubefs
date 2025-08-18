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
	"time"

	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) loadFlashNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashNodes],err:%v", err.Error())
		return
	}

	for _, value := range result {
		fnv := &flashgroupmanager.FlashNodeValue{}
		if err = json.Unmarshal(value, fnv); err != nil {
			err = fmt.Errorf("action[loadFlashNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashNode := flashgroupmanager.NewFlashNode(fnv.Addr, fnv.ZoneName, c.Name, fnv.Version, fnv.IsEnable)
		flashNode.ID = fnv.ID
		// load later in loadFlashTopology
		flashNode.FlashGroupID = fnv.FlashGroupID

		_, err = c.flashNodeTopo.GetZone(flashNode.ZoneName)
		if err != nil {
			c.flashNodeTopo.PutZoneIfAbsent(flashgroupmanager.NewFlashNodeZone(flashNode.ZoneName))
			err = nil
		}
		c.flashNodeTopo.PutFlashNode(flashNode)
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
		var fgv flashgroupmanager.FlashGroupValue
		if err = json.Unmarshal(value, &fgv); err != nil {
			err = fmt.Errorf("action[loadFlashGroups],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashGroup := flashgroupmanager.NewFlashGroupFromFgv(fgv)
		c.flashNodeTopo.SaveFlashGroup(flashGroup)
		log.LogInfof("action[loadFlashGroups],flashGroup[%v]", flashGroup.ID)
	}
	return
}

func (c *Cluster) loadFlashTopology() (err error) {
	return c.flashNodeTopo.Load()
}

func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		dur := time.Second * time.Duration(5)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		clientUpdateCh := c.flashNodeTopo.ClientUpdateChannel()
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.flashNodeTopo.UpdateClientResponse()
				// sync fg if slots changed
				c.flashNodeTopo.UpdateFlashGroup(c.syncUpdateFlashGroup)
			}
			select {
			case <-c.stopc:
				return
			case <-clientUpdateCh:
				ticker.Reset(dur)
			case <-ticker.C:
			}
		}
	}()
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
					c.flashNodeTopo.UpdateFlashGroupSlots(c.syncDeleteFlashGroup, c.syncUpdateFlashGroup, c.syncUpdateFlashNode)
				}
			}
		}
	}()
}
