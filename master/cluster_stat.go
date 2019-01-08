// Copyright 2018 The Container File System Authors.
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
	"github.com/tiglabs/containerfs/util"
	"strconv"
)

type nodeStatInfo struct {
	TotalGB     uint64
	UsedGB      uint64
	IncreasedGB int64
	UsedRatio   string
}

type volStatInfo struct {
	Name      string
	TotalGB   uint64
	UsedGB    uint64
	UsedRatio string
}

func newVolStatInfo(name string, total, used uint64, ratio string) *volStatInfo {
	return &volStatInfo{
		Name:      name,
		TotalGB:   total,
		UsedGB:    used,
		UsedRatio: ratio,
	}
}

// Check the total space, available space, and daily-used space in data nodes,  meta nodes, and volumes
func (c *Cluster) updateStatInfo() {
	c.updateDataNodeStatInfo()
	c.updateMetaNodeStatInfo()
	c.updateVolStatInfo()
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
		useRate := float64(used) / float64(total)
		c.volStatInfo.Store(vol.Name, newVolStatInfo(vol.Name, total/util.GB, used/util.GB, strconv.FormatFloat(useRate, 'f', 3, 32)))
	}
}
