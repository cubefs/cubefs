package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/util"
	"strconv"
)

type DataNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type MetaNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type VolSpaceStat struct {
	Name      string
	TotalGB   uint64
	UsedGB    uint64
	UsedRatio string
}

func newVolSpaceStat(name string, total, used uint64, ratio string) *VolSpaceStat {
	return &VolSpaceStat{
		Name:      name,
		TotalGB:   total,
		UsedGB:    used,
		UsedRatio: ratio,
	}
}

func (c *Cluster) checkAvailSpace() {
	c.checkDataNodeAvailSpace()
	c.checkMetaNodeAvailSpace()
	c.checkVolAvailSpace()
}

func (c *Cluster) checkDataNodeAvailSpace() {
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
	useRate := float64(used) / float64(total)
	if useRate > SpaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add dataNode",
			c.Name, useRate, used, total))
	}
	c.dataNodeSpace.TotalGB = total / util.GB
	newUsed := used / util.GB
	c.dataNodeSpace.IncreaseGB = int64(newUsed) - int64(c.dataNodeSpace.UsedGB)
	c.dataNodeSpace.UsedGB = newUsed
	c.dataNodeSpace.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
}

func (c *Cluster) checkMetaNodeAvailSpace() {
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
	if useRate > SpaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add metaNode",
			c.Name, useRate, used, total))
	}
	c.metaNodeSpace.TotalGB = total / util.GB
	newUsed := used / util.GB
	c.metaNodeSpace.IncreaseGB = int64(newUsed) - int64(c.metaNodeSpace.UsedGB)
	c.metaNodeSpace.UsedGB = newUsed
	c.metaNodeSpace.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
}

func (c *Cluster) checkVolAvailSpace() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.getTotalUsedSpace(), vol.Capacity*util.GB
		if total <= 0 {
			continue
		}
		useRate := float64(used) / float64(total)
		c.volSpaceStat.Store(vol.Name, newVolSpaceStat(vol.Name, total/util.GB, used/util.GB, strconv.FormatFloat(useRate, 'f', 3, 32)))
	}
}
