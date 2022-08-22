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

	"math"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type nodeStatInfo = proto.NodeStatInfo

type volStatInfo = proto.VolStatInfo

func newVolStatInfo(name string, total, used, cacheTotal, cacheUsed uint64) *volStatInfo {
	usedRatio := strconv.FormatFloat(float64(used)/float64(total), 'f', 3, 32)
	cacheUsedRatio := "0.00"
	if cacheTotal > 0 {
		strconv.FormatFloat(float64(cacheUsed)/float64(cacheTotal), 'f', 3, 32)
	}

	return &volStatInfo{
		Name:           name,
		TotalSize:      total,
		UsedSize:       used,
		UsedRatio:      usedRatio,
		CacheTotalSize: cacheTotal,
		CacheUsedSize:  cacheUsed,
		CacheUsedRatio: cacheUsedRatio,
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
	c.updateVolStatInfo()
	c.updateZoneStatInfo()
}

func (c *Cluster) updateZoneStatInfo() {
	for _, zone := range c.t.zones {
		zs := newZoneStatInfo()
		c.zoneStatInfos[zone.name] = zs
		zone.dataNodes.Range(func(key, value interface{}) bool {
			zs.DataNodeStat.TotalNodes++
			node := value.(*DataNode)
			if node.isActive && node.isWriteAble() {
				zs.DataNodeStat.WritableNodes++
			}
			zs.DataNodeStat.Total += float64(node.Total) / float64(util.GB)
			zs.DataNodeStat.Used += float64(node.Used) / float64(util.GB)
			return true
		})
		zs.DataNodeStat.Total = fixedPoint(zs.DataNodeStat.Total, 2)
		zs.DataNodeStat.Used = fixedPoint(zs.DataNodeStat.Used, 2)
		zs.DataNodeStat.Avail = fixedPoint(zs.DataNodeStat.Total-zs.DataNodeStat.Used, 2)
		if zs.DataNodeStat.Total == 0 {
			zs.DataNodeStat.Total = 1
		}
		zs.DataNodeStat.UsedRatio = fixedPoint(float64(zs.DataNodeStat.Used)/float64(zs.DataNodeStat.Total), 2)
		zone.metaNodes.Range(func(key, value interface{}) bool {
			zs.MetaNodeStat.TotalNodes++
			node := value.(*MetaNode)
			if node.IsActive && node.isWritable() {
				zs.MetaNodeStat.WritableNodes++
			}
			zs.MetaNodeStat.Total += float64(node.Total) / float64(util.GB)
			zs.MetaNodeStat.Used += float64(node.Used) / float64(util.GB)
			return true
		})
		zs.MetaNodeStat.Total = fixedPoint(zs.MetaNodeStat.Total, 2)
		zs.MetaNodeStat.Used = fixedPoint(zs.MetaNodeStat.Used, 2)
		zs.MetaNodeStat.Avail = fixedPoint(zs.MetaNodeStat.Total-zs.MetaNodeStat.Used, 2)
		if zs.MetaNodeStat.Total == 0 {
			zs.MetaNodeStat.Total = 1
		}
		zs.MetaNodeStat.UsedRatio = fixedPoint(float64(zs.MetaNodeStat.Used)/float64(zs.MetaNodeStat.Total), 2)
	}
}

func fixedPoint(x float64, scale int) float64 {
	decimal := math.Pow10(scale)
	return float64(int(math.Round(x*decimal))) / decimal
}

func (c *Cluster) updateDataNodeStatInfo() {
	var (
		total uint64
		used  uint64
	)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		total = total + dataNode.Total
		used = used + dataNode.Used
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
	c.dataNodeStatInfo.TotalGB = total / util.GB
	usedGB := used / util.GB
	c.dataNodeStatInfo.IncreasedGB = int64(usedGB) - int64(c.dataNodeStatInfo.UsedGB)
	c.dataNodeStatInfo.UsedGB = usedGB
	c.dataNodeStatInfo.UsedRatio = strconv.FormatFloat(usedRate, 'f', 3, 32)
}

func (c *Cluster) updateMetaNodeStatInfo() {
	var (
		total uint64
		used  uint64
	)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		total = total + metaNode.Total
		used = used + metaNode.Used
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
	c.metaNodeStatInfo.TotalGB = total / util.GB
	newUsed := used / util.GB
	c.metaNodeStatInfo.IncreasedGB = int64(newUsed) - int64(c.metaNodeStatInfo.UsedGB)
	c.metaNodeStatInfo.UsedGB = newUsed
	c.metaNodeStatInfo.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
}

func (c *Cluster) updateVolStatInfo() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.totalUsedSpace(), vol.Capacity*util.GB
		if total <= 0 {
			continue
		}

		cacheUsed, cacheTotal := vol.cfsUsedSpace(), vol.CacheCapacity*util.GB
		if proto.IsHot(vol.VolType) {
			cacheUsed, cacheTotal = 0, 0
		}

		c.volStatInfo.Store(vol.Name, newVolStatInfo(vol.Name, total, used, cacheTotal, cacheUsed))
	}
}
