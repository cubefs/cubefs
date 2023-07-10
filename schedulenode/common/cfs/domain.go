package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/unit"
	"math"
)

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type NodeView struct {
	ID                        uint64
	Addr                      string
	Status                    bool
	PersistenceDataPartitions []uint64
	PersistenceMetaPartitions []uint64
}

type DataPartition struct {
	PartitionID uint64
	ReplicaNum  int
	Used        int
	Hosts       []string
	Replicas    []*DataReplica
}

type DataReplica struct {
	Addr      string
	FileCount int
	TotalSize uint64
	UsedSize  uint64
}

func (partition *DataPartition) getMaxUsed() (maxUsed uint64) {
	for _, r := range partition.Replicas {
		if r.UsedSize > maxUsed {
			maxUsed = r.UsedSize
		}
	}
	return
}

func (partition *DataPartition) IsDataHasRecover() bool {
	if len(partition.Replicas) == 0 || len(partition.Replicas) < partition.ReplicaNum {
		return false
	}
	minus := partition.getMinus()
	maxUsed := partition.getMaxUsed()
	if maxUsed > 10*unit.GB {
		if minus < unit.GB {
			return true
		}
	} else if maxUsed > unit.GB {
		if minus < 500*unit.MB {
			return true
		}
	} else {
		if maxUsed == 0 {
			return true
		}
		percent := minus / float64(maxUsed)
		if maxUsed > unit.MB {
			if percent < 0.5 {
				return true
			}
		} else {
			if percent < 0.7 {
				return true
			}
		}
	}
	return false
}

func (partition *DataPartition) getMinus() (minus float64) {
	if len(partition.Replicas) == 0 {
		return 2 * unit.GB
	}
	used := partition.Replicas[0].UsedSize
	for _, replica := range partition.Replicas {
		diff := math.Abs(float64(replica.UsedSize) - float64(used))
		if diff > minus {
			minus = diff
		}
	}
	return minus
}

func (partition *DataPartition) getMinusOfFileCount() (minus float64) {

	if len(partition.Replicas) == 0 {
		return 1
	}
	sentry := partition.Replicas[0].FileCount
	for _, replica := range partition.Replicas {
		diff := math.Abs(float64(replica.FileCount) - float64(sentry))
		if diff > minus {
			minus = diff
		}
	}
	return
}

type MetaPartition struct {
	PartitionID uint64
	ReplicaNum  int
	Hosts       []string
	Replicas    []*MetaReplica
	InodeCount  uint64
	DentryCount uint64
}

type MetaReplica struct {
	Addr        string
	InodeCount  uint64
	DentryCount uint64
}

func (mp *MetaPartition) IsDataHasRecover() bool {
	if len(mp.Replicas) == 0 || len(mp.Replicas) < mp.ReplicaNum {
		return false
	}
	dentryCountDiff := mp.getMinusOfDentryCount()
	//inodeCountDiff := mp.getMinusOfInodeCount()
	if dentryCountDiff == 0 {
		return true
	}
	return false
}

func (mp *MetaPartition) getMinusOfDentryCount() (minus float64) {
	if len(mp.Replicas) == 0 {
		return 1
	}
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.DentryCount)
			continue
		}
		diff := math.Abs(float64(replica.DentryCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) getMinusOfInodeCount() (minus float64) {
	if len(mp.Replicas) == 0 {
		return 1
	}
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.InodeCount)
			continue
		}
		diff := math.Abs(float64(replica.InodeCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

type ClusterView struct {
	Name                   string
	LeaderAddr             string
	CompactStatus          bool
	DisableAutoAlloc       bool
	Applied                uint64
	MaxDataPartitionID     uint64
	MaxMetaNodeID          uint64
	MaxMetaPartitionID     uint64
	DataNodeStat           *DataNodeSpaceStat
	MetaNodeStat           *MetaNodeSpaceStat
	VolStat                []*VolSpaceStat
	DataNodeStatInfo       *DataNodeSpaceStat
	MetaNodeStatInfo       *MetaNodeSpaceStat
	VolStatInfo            []*VolSpaceStat
	MetaNodes              []MetaNodeView
	DataNodes              []DataNodeView
	BadPartitionIDs        []BadPartitionView
	MigratedDataPartitions []BadPartitionView
	MigratedMetaPartitions []BadPartitionView
}

func (cv *ClusterView) String() string {
	return fmt.Sprintf("name[%v],leaderAddr[%v]", cv.Name, cv.LeaderAddr)
}

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
	TotalSize uint64
	UsedSize  uint64
	UsedRatio string
}

type DataNodeView struct {
	Addr   string
	Status bool
}

func (dnv DataNodeView) String() string {
	return fmt.Sprintf("Addr:%v,Status:%v", dnv.Addr, dnv.Status)
}

type MetaNodeView struct {
	ID     uint64
	Addr   string
	Status bool
}

func (mnv MetaNodeView) String() string {
	return fmt.Sprintf("ID:%v,Addr:%v,Status:%v", mnv.ID, mnv.Addr, mnv.Status)
}

type BadPartitionView struct {
	DiskPath     string
	PartitionIDs []uint64
}

type MpInfoOnMn struct {
	Cursor     int
	LeaderAddr string
	NodeId     int
}

type ClusterStatInfoView struct {
	DataNodeStatInfo *NodeStatInfo
	MetaNodeStatInfo *NodeStatInfo
	ZoneStatInfo     map[string]*ZoneStat
}

type ZoneStat struct {
	DataNodeStat *ZoneNodesStat
	MetaNodeStat *ZoneNodesStat
}

type ZoneNodesStat struct {
	Total              float64 `json:"TotalGB"`
	Used               float64 `json:"UsedGB"`
	Avail              float64 `json:"AvailGB"`
	UsedRatio          float64
	TotalNodes         int
	WritableNodes      int
	HighUsedRatioNodes int
}

type NodeStatInfo struct {
	TotalGB            uint64
	UsedGB             uint64
	IncreasedGB        int64
	UsedRatio          string
	TotalNodes         int
	WritableNodes      int
	HighUsedRatioNodes int
}
