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
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"math"
)

type nodeStatInfo = proto.NodeStatInfo

type volStatInfo = proto.VolStatInfo

func newVolStatInfo(name string, total, used uint64, ratio string, enableToken, enableWriteCache bool) *volStatInfo {
	return &volStatInfo{
		Name:             name,
		TotalSize:        total,
		UsedSize:         used,
		UsedRatio:        ratio,
		EnableToken:      enableToken,
		EnableWriteCache: enableWriteCache,
	}
}

func newZoneStatInfo() *proto.ZoneStat {
	return &proto.ZoneStat{DataNodeStat: new(proto.ZoneNodesStat), MetaNodeStat: new(proto.ZoneNodesStat)}
}

// Check the total space, available space, and daily-used space in data nodes,  meta nodes, and volumes
func (c *Cluster) updateStatInfo() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("updateStatInfo occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"updateStatInfo occurred panic")
		}
	}()
	c.updateDataNodeStatInfo()
	c.updateMetaNodeStatInfo()
	c.updateEcNodeStatInfo()
	c.updateVolStatInfo()
	c.updateZoneStatInfo()
}

func (c *Cluster) updateZoneStatInfo() {
	var (
		highUsedRatioDataNodes int
		highUsedRatioMetaNodes int
	)
	for _, zone := range c.t.zones {
		highUsedRatioDataNodes = 0
		highUsedRatioMetaNodes = 0
		zs := newZoneStatInfo()
		c.zoneStatInfos[zone.name] = zs
		zone.dataNodes.Range(func(key, value interface{}) bool {
			zs.DataNodeStat.TotalNodes++
			node := value.(*DataNode)
			if node.isActive && node.isWriteAble() {
				zs.DataNodeStat.WritableNodes++
			}
			zs.DataNodeStat.Total += float64(node.Total) / float64(unit.GB)
			zs.DataNodeStat.Used += float64(node.Used) / float64(unit.GB)
			if node.UsageRatio >= defaultHighUsedRatioDataNodesThreshold {
				highUsedRatioDataNodes++
			}
			return true
		})
		zs.DataNodeStat.Total = fixedPoint(zs.DataNodeStat.Total, 2)
		zs.DataNodeStat.Used = fixedPoint(zs.DataNodeStat.Used, 2)
		zs.DataNodeStat.Avail = fixedPoint(zs.DataNodeStat.Total-zs.DataNodeStat.Used, 2)
		if zs.DataNodeStat.Total == 0 {
			zs.DataNodeStat.Total = 1
		}
		zs.DataNodeStat.UsedRatio = fixedPoint(float64(zs.DataNodeStat.Used)/float64(zs.DataNodeStat.Total), 2)
		zs.DataNodeStat.HighUsedRatioNodes = highUsedRatioDataNodes
		zone.metaNodes.Range(func(key, value interface{}) bool {
			zs.MetaNodeStat.TotalNodes++
			node := value.(*MetaNode)
			if node.IsActive && node.isWritable(proto.StoreModeMem) {
				zs.MetaNodeStat.WritableNodes++
			}
			zs.MetaNodeStat.Total += float64(node.Total) / float64(unit.GB)
			zs.MetaNodeStat.Used += float64(node.Used) / float64(unit.GB)
			if node.Ratio > defaultHighUsedRatioMetaNodesThreshold {
				highUsedRatioMetaNodes++
			}
			return true
		})
		zs.MetaNodeStat.Total = fixedPoint(zs.MetaNodeStat.Total, 2)
		zs.MetaNodeStat.Used = fixedPoint(zs.MetaNodeStat.Used, 2)
		zs.MetaNodeStat.Avail = fixedPoint(zs.MetaNodeStat.Total-zs.MetaNodeStat.Used, 2)
		if zs.MetaNodeStat.Total == 0 {
			zs.MetaNodeStat.Total = 1
		}
		zs.MetaNodeStat.UsedRatio = fixedPoint(float64(zs.MetaNodeStat.Used)/float64(zs.MetaNodeStat.Total), 2)
		zs.MetaNodeStat.HighUsedRatioNodes = highUsedRatioMetaNodes
	}
}

func fixedPoint(x float64, scale int) float64 {
	decimal := math.Pow10(scale)
	return float64(int(math.Round(x*decimal))) / decimal
}

func (c *Cluster) updateDataNodeStatInfo() {
	var (
		total              uint64
		used               uint64
		totalNodes         int
		writableNodes      int
		highUsedRatioNodes int
	)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		total = total + dataNode.Total
		used = used + dataNode.Used
		totalNodes++
		if dataNode.isActive && dataNode.isWriteAble() {
			writableNodes++
		}
		if dataNode.UsageRatio >= defaultHighUsedRatioDataNodesThreshold {
			highUsedRatioNodes++
		}
		return true
	})
	if total <= 0 {
		return
	}
	usedRate := float64(used) / float64(total)
	if usedRate > spaceAvailableRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add dataNode",
			c.Name, usedRate, used, total))
	}
	c.dataNodeStatInfo.TotalGB = total / unit.GB
	usedGB := used / unit.GB
	c.dataNodeStatInfo.IncreasedGB = int64(usedGB) - int64(c.dataNodeStatInfo.UsedGB)
	c.dataNodeStatInfo.UsedGB = usedGB
	c.dataNodeStatInfo.UsedRatio = strconv.FormatFloat(usedRate, 'f', 3, 32)
	c.dataNodeStatInfo.TotalNodes = totalNodes
	c.dataNodeStatInfo.WritableNodes = writableNodes
	c.dataNodeStatInfo.HighUsedRatioNodes = highUsedRatioNodes
}

func (c *Cluster) updateMetaNodeStatInfo() {
	var (
		total              uint64
		used               uint64
		totalNodes         int
		writableNodes      int
		highUsedRatioNodes int
	)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		total = total + metaNode.Total
		used = used + metaNode.Used
		totalNodes++
		if metaNode.IsActive && metaNode.isWritable(proto.StoreModeMem) {
			writableNodes++
		}
		if metaNode.Ratio > defaultHighUsedRatioMetaNodesThreshold {
			highUsedRatioNodes++
		}
		return true
	})
	if total <= 0 {
		return
	}
	useRate := float64(used) / float64(total)
	if useRate > spaceAvailableRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add metaNode",
			c.Name, useRate, used, total))
	}
	c.metaNodeStatInfo.TotalGB = total / unit.GB
	newUsed := used / unit.GB
	c.metaNodeStatInfo.IncreasedGB = int64(newUsed) - int64(c.metaNodeStatInfo.UsedGB)
	c.metaNodeStatInfo.UsedGB = newUsed
	c.metaNodeStatInfo.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
	c.metaNodeStatInfo.TotalNodes = totalNodes
	c.metaNodeStatInfo.WritableNodes = writableNodes
	c.metaNodeStatInfo.HighUsedRatioNodes = highUsedRatioNodes
}

func (c *Cluster) updateVolStatInfo() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.totalUsedSpace(), vol.Capacity*unit.GB
		if total <= 0 {
			continue
		}
		useRate := float64(used) / float64(total)
		c.volStatInfo.Store(vol.Name, newVolStatInfo(vol.Name, total, used, strconv.FormatFloat(useRate, 'f', 3, 32), vol.enableToken, vol.enableWriteCache))
	}
}

func setSSDAndHDDStatByZoneTag(cs *proto.ClusterStatInfo, zoneTag string) {
	filterZoneStat := &proto.ZoneStat{
		DataNodeStat: &proto.ZoneNodesStat{},
		MetaNodeStat: &proto.ZoneNodesStat{},
	}
	otherZoneStat := &proto.ZoneStat{
		DataNodeStat: &proto.ZoneNodesStat{},
		MetaNodeStat: &proto.ZoneNodesStat{},
	}
	for zoneName, zoneStat := range cs.ZoneStatInfo {
		if strings.Contains(zoneName, zoneTag) {
			filterZoneStat.Add(zoneStat)
		} else {
			otherZoneStat.Add(zoneStat)
		}
	}
	updateZoneStatUsedRatio(filterZoneStat)
	updateZoneStatUsedRatio(otherZoneStat)
	if zoneTag == mediumSSD {
		cs.SSDZoneStatInfo = filterZoneStat
		cs.HDDZoneStatInfo = otherZoneStat
	} else {
		cs.HDDZoneStatInfo = filterZoneStat
		cs.SSDZoneStatInfo = otherZoneStat
	}
}

func updateZoneStatUsedRatio(zoneStat *proto.ZoneStat) {
	if zoneStat == nil {
		return
	}
	if zoneStat.DataNodeStat != nil && zoneStat.DataNodeStat.Total > 0 {
		zoneStat.DataNodeStat.UsedRatio = fixedPoint(zoneStat.DataNodeStat.Used/zoneStat.DataNodeStat.Total, 2)
		zoneStat.DataNodeStat.Total = fixedPoint(zoneStat.DataNodeStat.Total, 2)
		zoneStat.DataNodeStat.Used = fixedPoint(zoneStat.DataNodeStat.Used, 2)
		zoneStat.DataNodeStat.Avail = fixedPoint(zoneStat.DataNodeStat.Avail, 2)
	}
	if zoneStat.MetaNodeStat != nil && zoneStat.MetaNodeStat.Total > 0 {
		zoneStat.MetaNodeStat.UsedRatio = fixedPoint(zoneStat.MetaNodeStat.Used/zoneStat.MetaNodeStat.Total, 2)
		zoneStat.MetaNodeStat.Total = fixedPoint(zoneStat.MetaNodeStat.Total, 2)
		zoneStat.MetaNodeStat.Used = fixedPoint(zoneStat.MetaNodeStat.Used, 2)
		zoneStat.MetaNodeStat.Avail = fixedPoint(zoneStat.MetaNodeStat.Avail, 2)
	}
}

func (c *Cluster) handleDataNodeHeartbeatPbResponse(nodeAddr string, taskPb *proto.HeartbeatAdminTaskPb) {
	if taskPb == nil {
		log.LogInfof("action[handleDataNodeHeartbeatPbResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	var (
		err      error
		dataNode *DataNode
	)
	task := taskPb.ConvertToView()
	log.LogDebugf("action[handleDataNodeHeartbeatPbResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())

	response := taskPb.DataNodeResponse.ConvertToView()
	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	dataNode.TaskManager.DelTask(task)
	err = c.handleDataNodeHeartbeatResp(task.OperatorAddr, response)
	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogErrorf("process task[%v] failed,status:%v, err:%v", task.ID, task.Status, err)
	return
}

func (c *Cluster) handleMetaNodeHeartbeatPbResponse(nodeAddr string, taskPb *proto.HeartbeatAdminTaskPb) {
	if taskPb == nil {
		return
	}
	task := taskPb.ConvertToView()
	log.LogDebugf(fmt.Sprintf("action[handleMetaNodeHeartbeatPbResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		metaNode *MetaNode
		err      error
	)
	response := taskPb.MetaNodeResponse.ConvertToView()
	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	metaNode.Sender.DelTask(task)
	err = c.dealMetaNodeHeartbeatResp(task.OperatorAddr, response)
	if err != nil {
		log.LogErrorf("process task:%v failed,status:%v,err:%v ", task.ID, task.Status, err)
	} else {
		log.LogInfof("process task:%v status:%v success", task.ID, task.Status)
	}
	return
errHandler:
	log.LogError(fmt.Sprintf("action[handleMetaNodeHeartbeatPbResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}
