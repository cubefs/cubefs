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

package proto

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultZoneName = "default"
)

// MetaNode defines the structure of a meta node
type MetaNodeInfo struct {
	ID                        uint64
	Addr                      string
	DomainAddr                string
	IsActive                  bool
	IsWriteAble               bool
	ZoneName                  string `json:"Zone"`
	MaxMemAvailWeight         uint64 `json:"MaxMemAvailWeight"`
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Ratio                     float64
	SelectCount               uint64
	Threshold                 float32
	ReportTime                time.Time
	MetaPartitionCount        int
	NodeSetID                 uint64
	PersistenceMetaPartitions []uint64
	RdOnly                    bool
	CpuUtil                   float64 `json:"cpuUtil"`
}

// DataNode stores all the information about a data node
type DataNodeInfo struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	ID                        uint64
	ZoneName                  string `json:"Zone"`
	Addr                      string
	DomainAddr                string
	ReportTime                time.Time
	IsActive                  bool
	IsWriteAble               bool
	UsageRatio                float64 // used / total space
	SelectedTimes             uint64  // number times that this datanode has been selected as the location for a data partition.
	DataPartitionReports      []*DataPartitionReport
	DataPartitionCount        uint32
	NodeSetID                 uint64
	PersistenceDataPartitions []uint64
	BadDisks                  []string
	RdOnly                    bool
	MaxDpCntLimit             uint32             `json:"maxDpCntLimit"`
	CpuUtil                   float64            `json:"cpuUtil"`
	IoUtils                   map[string]float64 `json:"ioUtil"`
}

// MetaPartition defines the structure of a meta partition
type MetaPartitionInfo struct {
	PartitionID   uint64
	Start         uint64
	End           uint64
	MaxInodeID    uint64
	InodeCount    uint64
	DentryCount   uint64
	VolName       string
	Replicas      []*MetaReplicaInfo
	ReplicaNum    uint8
	Status        int8
	IsRecover     bool
	Hosts         []string
	Peers         []Peer
	Zones         []string
	NodeSets      []uint64
	OfflinePeerID uint64
	MissNodes     map[string]int64
	LoadResponse  []*MetaPartitionLoadResponse
	Forbidden     bool
}

// MetaReplica defines the replica of a meta partition
type MetaReplicaInfo struct {
	Addr        string
	DomainAddr  string
	MaxInodeID  uint64
	ReportTime  int64
	Status      int8 // unavailable, readOnly, readWrite
	IsLeader    bool
	InodeCount  uint64
	MaxInode    uint64
	DentryCount uint64
}

// ClusterView provides the view of a cluster.
type ClusterView struct {
	Name                 string
	CreateTime           string
	LeaderAddr           string
	DisableAutoAlloc     bool
	ForbidMpDecommission bool
	MetaNodeThreshold    float32
	Applied              uint64
	MaxDataPartitionID   uint64
	MaxMetaNodeID        uint64
	MaxMetaPartitionID   uint64
	DataNodeStatInfo     *NodeStatInfo
	MetaNodeStatInfo     *NodeStatInfo
	VolStatInfo          []*VolStatInfo
	BadPartitionIDs      []BadPartitionView
	BadMetaPartitionIDs  []BadPartitionView
	MasterNodes          []NodeView
	MetaNodes            []NodeView
	DataNodes            []NodeView
	FlashNodes           []NodeView
}

// ClusterNode defines the structure of a cluster node
type ClusterNodeInfo struct {
	// BatchCount          int
	LoadFactor string
	// MarkDeleteRate      int
	// AutoRepairRate      int
	// DeleteWorkerSleepMs int
}

type ClusterIP struct {
	Cluster string
	// MetaNodeDeleteBatchCount 	int
	// MetaNodeDeleteWorkerSleepMs int
	// DataNodeDeleteLimitRate     int
	// DataNodeAutoRepairLimitRate int
	// Ip 							string
	EbsAddr string
	// ServicePath 				string
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	IsActive   bool
	DomainAddr string
	ID         uint64
	IsWritable bool
}

type DpRepairInfo struct {
	PartitionID                uint64
	DecommissionRepairProgress float64
}

type BadPartitionRepairView struct {
	Path           string
	PartitionInfos []DpRepairInfo
}

type BadPartitionView struct {
	Path         string
	PartitionIDs []uint64
}

type ClusterStatInfo struct {
	DataNodeStatInfo *NodeStatInfo
	MetaNodeStatInfo *NodeStatInfo
	ZoneStatInfo     map[string]*ZoneStat
}

type ZoneStat struct {
	DataNodeStat *ZoneNodesStat
	MetaNodeStat *ZoneNodesStat
}

type ZoneNodesStat struct {
	Total         float64 `json:"TotalGB"`
	Used          float64 `json:"UsedGB"`
	Avail         float64 `json:"AvailGB"`
	UsedRatio     float64
	TotalNodes    int
	WritableNodes int
}

type NodeSetStat struct {
	ID          uint64
	Capacity    int
	Zone        string
	MetaNodeNum int
	DataNodeNum int
}

type NodeSetStatInfo struct {
	ID               uint64
	Capacity         int
	Zone             string
	MetaNodes        []*NodeStatView
	DataNodes        []*NodeStatView
	DataNodeSelector string
	MetaNodeSelector string
}

type NodeStatView struct {
	Addr       string
	Status     bool
	DomainAddr string
	ID         uint64
	IsWritable bool
	Total      uint64
	Used       uint64
	Avail      uint64
}

type NodeStatInfo struct {
	TotalGB     uint64
	UsedGB      uint64
	IncreasedGB int64
	UsedRatio   string
	AvailGB     uint64
}

type VolStatInfo struct {
	Name                  string
	TotalSize             uint64
	UsedSize              uint64
	UsedRatio             string
	CacheTotalSize        uint64
	CacheUsedSize         uint64
	CacheUsedRatio        string
	EnableToken           bool
	InodeCount            uint64
	TxCnt                 uint64
	TxRbInoCnt            uint64
	TxRbDenCnt            uint64
	DpReadOnlyWhenVolFull bool
}

// DataPartition represents the structure of storing the file contents.
type DataPartitionInfo struct {
	PartitionID              uint64
	PartitionTTL             int64
	PartitionType            int
	LastLoadedTime           int64
	ReplicaNum               uint8
	Status                   int8
	Recover                  bool
	Replicas                 []*DataReplica
	Hosts                    []string // host addresses
	Peers                    []Peer
	Zones                    []string
	NodeSets                 []uint64
	MissingNodes             map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                  string
	VolID                    uint64
	OfflinePeerID            uint64
	FileInCoreMap            map[string]*FileInCore
	IsRecover                bool
	FilesWithMissingReplica  map[string]int64 // key: file name, value: last time when a missing replica is found
	SingleDecommissionStatus uint32
	SingleDecommissionAddr   string
	RdOnly                   bool
	IsDiscard                bool
	Forbidden                bool
}

// FileInCore define file in data partition
type FileInCore struct {
	Name          string
	LastModify    int64
	MetadataArray []*FileMetadata
}

// FileMetadata defines the file metadata on a dataNode
type FileMetadata struct {
	Crc     uint32
	LocAddr string
	Size    uint32
	ApplyID uint64
}

// DataReplica represents the replica of a data partition
type DataReplica struct {
	Addr                       string
	DomainAddr                 string
	ReportTime                 int64
	FileCount                  uint32
	Status                     int8
	HasLoadResponse            bool   // if there is any response when loading
	Total                      uint64 `json:"TotalSize"`
	Used                       uint64 `json:"UsedSize"`
	IsLeader                   bool
	NeedsToCompare             bool
	DiskPath                   string
	DecommissionRepairProgress float64
}

// data partition diagnosis represents the inactive data nodes, corrupt data partitions, and data partitions lack of replicas
type DataPartitionDiagnosis struct {
	InactiveDataNodes           []string
	CorruptDataPartitionIDs     []uint64
	LackReplicaDataPartitionIDs []uint64
	RepFileCountDifferDpIDs     []uint64
	RepUsedSizeDifferDpIDs      []uint64
	ExcessReplicaDpIDs          []uint64
	// BadDataPartitionIDs         []BadPartitionView
	BadDataPartitionInfos      []BadPartitionRepairView
	BadReplicaDataPartitionIDs []uint64
}

// meta partition diagnosis represents the inactive meta nodes, corrupt meta partitions, and meta partitions lack of replicas
type MetaPartitionDiagnosis struct {
	InactiveMetaNodes                          []string
	CorruptMetaPartitionIDs                    []uint64
	LackReplicaMetaPartitionIDs                []uint64
	BadMetaPartitionIDs                        []BadPartitionView
	BadReplicaMetaPartitionIDs                 []uint64
	ExcessReplicaMetaPartitionIDs              []uint64
	InodeCountNotEqualReplicaMetaPartitionIDs  []uint64
	MaxInodeNotEqualReplicaMetaPartitionIDs    []uint64
	DentryCountNotEqualReplicaMetaPartitionIDs []uint64
}

type DecommissionProgress struct {
	Status        uint32
	Progress      string
	FailedDps     []uint64
	StatusMessage string
}

type BadDiskInfo struct {
	Address              string
	Path                 string
	TotalPartitionCnt    int
	DiskErrPartitionList []uint64
}

type BadDiskInfos struct {
	BadDisks []BadDiskInfo
}

type DiscardDataPartitionInfos struct {
	DiscardDps []DataPartitionInfo
}

type VolVersionInfo struct {
	Ver     uint64 // unixMicro of createTime used as version
	DelTime int64
	Status  uint8 // building,normal,deleted,abnormal
}

func (vv *VolVersionInfo) String() string {
	return fmt.Sprintf("Ver:%v|DelTimt:%v|status:%v", vv.Ver, vv.DelTime, vv.Status)
}

type VolVersionInfoList struct {
	VerList         []*VolVersionInfo
	Strategy        VolumeVerStrategy
	TemporaryVerMap map[uint64]*VolVersionInfo
	RWLock          sync.RWMutex
}

func (v *VolVersionInfoList) GetNextOlderVer(ver uint64) (verSeq uint64, err error) {
	v.RWLock.RLock()
	defer v.RWLock.RUnlock()
	log.LogDebugf("getNextOlderVer ver %v", ver)
	for idx, info := range v.VerList {
		log.LogDebugf("getNextOlderVer id %v ver %v info %v", idx, info.Ver, info)
		if info.Ver >= ver {
			if idx == 0 {
				return 0, fmt.Errorf("not found")
			}
			return v.VerList[idx-1].Ver, nil
		}
	}
	log.LogErrorf("getNextOlderVer ver %v not found", ver)
	return 0, fmt.Errorf("version not exist")
}

func (v *VolVersionInfoList) GetNextNewerVer(ver uint64) (verSeq uint64, err error) {
	log.LogDebugf("getNextOlderVer ver %v", ver)
	for idx, info := range v.VerList {
		log.LogDebugf("getNextOlderVer id %v ver %v info %v", idx, info.Ver, info)
		if info.Ver > ver {
			return info.Ver, nil
		}
	}
	log.LogErrorf("getNextOlderVer ver %v not found", ver)
	return 0, fmt.Errorf("version not exist")
}

func (v *VolVersionInfoList) GetLastVolVerInfo() *VolVersionInfo {
	if len(v.VerList) == 0 {
		return nil
	}
	return v.VerList[len(v.VerList)-1]
}

func (v *VolVersionInfoList) GetLastVer() uint64 {
	if len(v.VerList) == 0 {
		return 0
	}
	return v.VerList[len(v.VerList)-1].Ver
}

type DecommissionDiskLimitDetail struct {
	NodeSetId uint64
	Limit     int
}

type DecommissionDiskLimit struct {
	Details []DecommissionDiskLimitDetail
}

type DecommissionDiskInfo struct {
	SrcAddr                  string
	DiskPath                 string
	DecommissionStatus       uint32
	DecommissionRaftForce    bool
	DecommissionRetry        uint8
	DecommissionDpTotal      int
	DecommissionTerm         uint64
	DecommissionLimit        int
	Type                     uint32
	DecommissionCompleteTime int64
	Progress                 float64
}

type DecommissionDisksResponse struct {
	Infos []DecommissionDiskInfo
}
