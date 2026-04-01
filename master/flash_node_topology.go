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
		var topo *flashgroupmanager.FlashNodeTopology
		topo, err = c.PeekFlashTopo(flashNode.FlashNodeTopoName)
		if err != nil {
			log.LogWarnf("action[loadFlashNodes],flashNode[%v] topo %v not found", flashNode.String(), flashNode.FlashNodeTopoName)
			// If the topo cannot be found, the fn will not be loaded.
			// You have to restart the fn service to perform operations, and the restart will trigger re-registration
			// to the default node automatically. Therefore, when the topo is missing, you can directly add it to the default node.
			topo, err = c.PeekFlashTopo(proto.DefaultTopoName)
			if err != nil {
				return
			}
		}
		err = topo.PutFlashNode(flashNode)
		if err != nil {
			log.LogWarnf("action[loadFlashNodes], flashNode[%v] put topo %v failed %v", flashNode.String(), topo.Name, err.Error())
			topo, err = c.PeekFlashTopo(proto.IdleTopoName)
			if err != nil {
				return
			}
			err = topo.PutFlashNode(flashNode)
			if err != nil {
				return
			}
			flashNode.FlashNodeTopoName = proto.IdleTopoName
		}
		log.LogInfof("action[loadFlashNodes] load %v success", flashNode.String())
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
		err = topo.SaveFlashGroup(flashGroup)
		if err != nil {
			log.LogWarnf("action[loadFlashGroups], flashGroup%v] put topo %v failed %v", flashGroup.ID, topo.Name, err.Error())
			return
		}
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
		if err = c.AddFlashTopo(proto.DefaultTopoName, proto.DefaultRegionName); err != nil {
			return
		}
		log.LogInfof("action[loadFlashTopos] load default topo")
		if err = c.AddFlashTopo(proto.IdleTopoName, proto.DefaultRegionName); err != nil {
			return
		}
		log.LogInfof("action[loadFlashTopos] load default idle")
	} else {
		// TODO: chi-test
		findIdle := false
		// collect markDeleted topos for delayed deletion enqueue
		marked := make([]*flashgroupmanager.FlashNodeTopology, 0)
		// load all topos from store
		for _, value := range result {
			ftv := &flashgroupmanager.FlashNodeTopologyValue{}
			if err = json.Unmarshal(value, &ftv); err != nil {
				err = fmt.Errorf("action[loadFlashTopos],value:%v,unmarshal err:%v", string(value), err)
				return
			}
			topo := flashgroupmanager.NewFlashNodeTopology(ftv.Name, ftv.Region, ftv.ID, ftv.Status)
			// restore delete info
			topo.DeleteExecTime = ftv.DeleteExecTime
			topo.DeleteStep = ftv.DeleteStep
			topo.DeleteGradualFlag = ftv.DeleteGradualFlag
			if ftv.RemoteCacheReadFlowMap != nil {
				topo.RemoteCacheReadFlowMap = ftv.RemoteCacheReadFlowMap
			}
			if ftv.RemoteCacheWriteFlowMap != nil {
				topo.RemoteCacheWriteFlowMap = ftv.RemoteCacheWriteFlowMap
			}
			topo.SyncFlashGroupFunc = c.syncUpdateFlashGroup
			c.flashNodeTopo.Store(ftv.Name, topo)
			// collect markDeleted topos
			if ftv.Status == proto.TopoStatusMarkDelete {
				marked = append(marked, topo)
			}
			if ftv.Name == proto.IdleTopoName {
				findIdle = true
			}
			log.LogInfof("action[loadFlashTopos] load topo %v", ftv.Name)
		}
		// always have idle topo
		if !findIdle {
			if err = c.AddFlashTopo(proto.IdleTopoName, proto.DefaultRegionName); err != nil {
				return
			}
		}
		// enqueue mark-deleted topos to delay delete map (single pass)
		idleTopo, _ := c.PeekFlashTopo(proto.IdleTopoName)
		if len(marked) > 0 {
			c.deleteFlashTopoMutex.Lock()
			for _, topo := range marked {
				c.delayDeleteFlashTopoInfo[topo.Name] = &DelayDeleteFlashTopoInfo{
					idleTopo:    idleTopo,
					gradualFlag: topo.DeleteGradualFlag,
					step:        topo.DeleteStep,
				}
				log.LogInfof("action[loadFlashTopos] enqueue markDeleted topo %v for delayed deletion at %v", topo.Name, topo.DeleteExecTime)
			}
			c.deleteFlashTopoMutex.Unlock()
		}
	}
	c.syncMaxDisableFlashGroupPercentToFlashTopos()
	return
}

func (c *Cluster) syncMaxDisableFlashGroupPercentToFlashTopos() {
	if c == nil || c.cfg == nil {
		return
	}
	p := c.cfg.maxDisableFlashGroupPercent
	c.flashNodeTopo.Range(func(_, value interface{}) bool {
		topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
		if !ok {
			return true
		}
		topo.SetMaxDisableFlashGroupPercent(p)
		return true
	})
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
		err = topo.Load()
		return err == nil
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
					idleTopo, _ := c.PeekFlashTopo(proto.IdleTopoName)
					c.flashNodeTopo.Range(func(key, value interface{}) bool {
						topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
						if !ok {
							log.LogErrorf("action[scheduleToUpdateFlashGroupSlots] cannot convert to FlashNodeTopology")
							return true
						}
						topo.UpdateFlashGroupSlots(c.Name, idleTopo, c.syncDeleteFlashGroup, c.syncUpdateFlashGroup, c.syncUpdateFlashNode,
							c.syncDeleteFlashNode, c.syncAddFlashNode, c.syncMoveFlashNode)
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
