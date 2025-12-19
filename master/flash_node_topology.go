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
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	"github.com/cubefs/cubefs/util/errors"
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
		flashNode := flashgroupmanager.NewFlashNodeFromFnv(c.Name, fnv)
		flashNode.ID = fnv.ID
		flashNode.FlashGroupID = fnv.FlashGroupID

		var topo *flashgroupmanager.FlashNodeTopology
		topo, err = c.PeekFlashTopo(flashNode.FlashNodeTopoName)
		if err != nil {
			log.LogErrorf("action[loadFlashNodes],flashNode[%v] topo %v not found", flashNode.Addr, flashNode.FlashNodeTopoName)
			// If the topo cannot be found, the fn will not be loaded.
			// You have to restart the fn service to perform operations, and the restart will trigger re-registration
			// to the default node automatically. Therefore, when the topo is missing, you can directly add it to the default node.
			topo, err = c.PeekFlashTopo(proto.DefaultTopoName)
			if err != nil {
				return
			}
		}
		_, err = topo.GetZone(flashNode.ZoneName)
		if err != nil {
			topo.PutZoneIfAbsent(flashgroupmanager.NewFlashNodeZone(flashNode.ZoneName))
			err = nil
		}
		topo.PutFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes], flashNode[flashNodeId:%v addr:%s flashGroupId:%v topoName: %v]",
			flashNode.ID, flashNode.Addr, flashNode.FlashGroupID, flashNode.FlashNodeTopoName)
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
		fgv := &flashgroupmanager.FlashGroupValue{}
		if err = json.Unmarshal(value, &fgv); err != nil {
			err = fmt.Errorf("action[loadFlashGroups],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashGroup := flashgroupmanager.NewFlashGroupFromFgv(fgv)
		var topo *flashgroupmanager.FlashNodeTopology
		topo, err = c.PeekFlashTopo(flashGroup.FlashNodeTopoName)
		if err != nil {
			log.LogErrorf("action[loadFlashGroups],flashGroup[%v] topo %v not found", flashGroup.ID, flashGroup.FlashNodeTopoName)
			continue
		}
		topo.SaveFlashGroup(flashGroup)
		log.LogInfof("action[loadFlashGroups],flashGroup[%v] topo %v", flashGroup.ID, flashGroup.FlashNodeTopoName)
	}
	return
}

func (c *Cluster) loadFlashTopos() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashTopoPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashTopos],err:%v", err.Error())
		return
	}
	if len(result) == 0 {
		// forward compatibility: create default FlashNodeTopology
		name := proto.DefaultTopoName
		// TODO: chi-test add to rocksDP, may hang. if hung , use go c.syncAddFlashTopo
		if err = c.AddFlashTopo(name); err != nil {
			return
		}
		log.LogInfof("action[loadFlashTopos] load default topo")
	} else {
		// TODO: chi-test
		findDefault := false
		for _, value := range result {
			ftv := &flashgroupmanager.FlashNodeTopologyValue{}
			if err = json.Unmarshal(value, &ftv); err != nil {
				err = fmt.Errorf("action[loadFlashTopos],value:%v,unmarshal err:%v", string(value), err)
				return
			}
			topo := flashgroupmanager.NewFlashNodeTopology(ftv.Name, ftv.ID)
			topo.SyncFlashGroupFunc = c.syncUpdateFlashGroup
			c.flashNodeTopo.Store(ftv.Name, topo)
			if ftv.Name == proto.DefaultTopoName {
				findDefault = true
			}
			log.LogInfof("action[loadFlashTopos] load topo %v", ftv.Name)
		}
		if !findDefault {
			panic("default topo is not found")
		}
	}
	return
}

func (c *Cluster) loadFlashTopology() (err error) {
	c.flashNodeTopo.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
		if !ok {
			err = errors.New("[loadFlashTopology]cannot convert to FlashNodeTopology")
			return true
		}
		topo.Load()
		return true
	})
	return
}

func (c *Cluster) scheduleToUpdateFlashGroupRespCache() {
	go func() {
		dur := time.Second * time.Duration(5)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.flashNodeTopo.Range(func(key, value interface{}) bool {
					if value == nil {
						return true
					}
					topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
					if !ok {
						return true
					}
					topo.UpdateClientResponse()
					return true
				})
			}
			select {
			case <-c.stopc:
				return
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
					c.flashNodeTopo.Range(func(key, value interface{}) bool {
						topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
						if !ok {
							log.LogErrorf("action[scheduleToUpdateFlashGroupSlots] cannot convert to FlashNodeTopology")
							return true
						}
						topo.UpdateFlashGroupSlots(c.syncDeleteFlashGroup, c.syncUpdateFlashGroup, c.syncUpdateFlashNode)
						return true
					})
				}
			}
		}
	}()
}

func (c *Cluster) syncAddFlashTopo(flashTopo *flashgroupmanager.FlashNodeTopology) (err error) {
	return c.syncPutFlashTopoInfo(opSyncAddFlashTopo, flashTopo)
}

func (c *Cluster) syncUpdateFlashTopo(flashTopo *flashgroupmanager.FlashNodeTopology) (err error) {
	return c.syncPutFlashTopoInfo(opSyncUpdateFlashTopo, flashTopo)
}

func (c *Cluster) syncDeleteFlashTopo(flashTopo *flashgroupmanager.FlashNodeTopology) (err error) {
	return c.syncPutFlashTopoInfo(opSyncDeleteFlashTopo, flashTopo)
}

func (c *Cluster) syncPutFlashTopoInfo(opType uint32, flashTopo *flashgroupmanager.FlashNodeTopology) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = flashTopoPrefix + strconv.FormatUint(flashTopo.ID, 10) + keySeparator
	metadata.V, err = json.Marshal(flashTopo.FlashNodeTopologyValue)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}
