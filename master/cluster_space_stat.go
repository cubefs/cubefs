package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/util"
	"strconv"
)

// TODO can we merge dataNodeStatInfo and metaNodeStatInfo as "nodeStatInfo"?
//type dataNodeStatInfo struct {
//	TotalGB    uint64
//	UsedGB     uint64
//	IncreaseGB int64
//	UsedRatio  string
//}
//
//type metaNodeStatInfo struct {
//	TotalGB    uint64
//	UsedGB     uint64
//	IncreaseGB int64
//	UsedRatio  string
//}

type nodeStatInfo struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
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
	if useRate > spaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add dataNode",
			c.Name, useRate, used, total))
	}
	c.dataNodeStatInfo.TotalGB = total / util.GB
	newUsed := used / util.GB
	c.dataNodeStatInfo.IncreaseGB = int64(newUsed) - int64(c.dataNodeStatInfo.UsedGB)
	c.dataNodeStatInfo.UsedGB = newUsed
	c.dataNodeStatInfo.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
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
	if useRate > spaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add metaNode",
			c.Name, useRate, used, total))
	}
	c.metaNodeStatInfo.TotalGB = total / util.GB
	newUsed := used / util.GB
	c.metaNodeStatInfo.IncreaseGB = int64(newUsed) - int64(c.metaNodeStatInfo.UsedGB)
	c.metaNodeStatInfo.UsedGB = newUsed
	c.metaNodeStatInfo.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
}

func (c *Cluster) checkVolAvailSpace() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.getTotalUsedSpace(), vol.Capacity*util.GB
		if total <= 0 {
			continue
		}
		useRate := float64(used) / float64(total)
		c.volStatInfo.Store(vol.Name, newVolStatInfo(vol.Name, total/util.GB, used/util.GB, strconv.FormatFloat(useRate, 'f', 3, 32)))
	}
}
