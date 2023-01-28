// Copyright 2018 The Chubao Authors.
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
	"time"
)

const (
	DefaultZoneName = "default"
)

const (
	VolStNormal       uint8 = 0
	VolStMarkDelete   uint8 = 1
	ZoneStNormal            = 0
	ZoneStUnavailable       = 1
)

const (
	MinMetaRaftLogSize = 4  //MB
	MaxMetaRaftLogSize = 32 //MB
	MinMetaRaftLogCap  = 2
)

// MetaNode defines the structure of a meta node
type MetaNodeInfo struct {
	ID                        uint64
	Addr                      string
	IsActive                  bool
	ZoneName                  string `json:"Zone"`
	MaxMemAvailWeight         uint64 `json:"MaxMemAvailWeight"`
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Ratio                     float64
	SelectCount               uint64
	Carry                     float64
	Threshold                 float32
	ReportTime                time.Time
	PhyMetaPartitionCount     int
	MetaPartitionCount        int
	NodeSetID                 uint64
	PersistenceMetaPartitions []uint64
	ToBeOffline               bool
	ToBeMigrated              bool
	ProfPort                  string
	Version                   string
}

// DataNode stores all the information about a data node
type DataNodeInfo struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	ID                        uint64
	Version                   string
	ZoneName                  string `json:"Zone"`
	Addr                      string
	HttpPort                  string
	ReportTime                time.Time
	IsActive                  bool
	UsageRatio                float64 // used / total space
	SelectedTimes             uint64  // number times that this datanode has been selected as the location for a data partition.
	Carry                     float64 // carry is a factor used in cacluate the node's weight
	DataPartitionReports      []*PartitionReport
	DataPartitionCount        uint32
	NodeSetID                 uint64
	PersistenceDataPartitions []uint64
	BadDisks                  []string
	ToBeOffline               bool
	ToBeMigrated              bool
}

type CodecNodeInfo struct {
	ID         uint64
	Addr       string
	ReportTime time.Time
	IsActive   bool
	Version    string
}

type EcNodeInfo struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	MaxDiskAvailSpace         uint64
	ID                        uint64
	ZoneName                  string `json:"Zone"`
	Addr                      string
	HttpPort                  string
	Version                   string
	ReportTime                time.Time
	IsActive                  bool
	UsageRatio                float64 // used / total space
	SelectedTimes             uint64
	Carry                     float64 // carry is a factor used in cacluate the node's weight
	DataPartitionReports      []*PartitionReport
	DataPartitionCount        uint32
	PersistenceDataPartitions []uint64
	BadDisks                  []string
	ToBeOffline               bool
	ToBeMigrated              bool
}

// MetaPartition defines the structure of a meta partition
type MetaPartitionInfo struct {
	PartitionID   uint64
	PhyPID        uint64
	Start         uint64
	End           uint64
	MaxInodeID    uint64
	InodeCount    uint64
	DentryCount   uint64
	MaxExistIno   uint64
	VolName       string
	Replicas      []*MetaReplicaInfo
	ReplicaNum    uint8
	LearnerNum    uint8
	Status        int8
	IsRecover     bool
	DisableReuse  bool
	Hosts         []string
	Peers         []Peer
	Learners      []Learner
	Zones         []string
	OfflinePeerID uint64
	MissNodes     map[string]int64
	LoadResponse  []*MetaPartitionLoadResponse
	MemStoreCnt   uint8
	RcokStoreCnt  uint8
	VirtualMPs    []VirtualMetaPartition
	PhyMPStatus   int8
}

// InodeInfo define the information of inode
type InodeInfoView struct {
	Ino         uint64
	PartitionID uint64
	At          string
	Ct          string
	Mt          string
	Nlink       uint64
	Size        uint64
	Gen         uint64
	Gid         uint64
	Uid         uint64
	Mode        uint64
}

// inodeExtentInfoView  define information of inodeExtentInfo
type InodeExtentInfoView struct {
	FileOffset   uint64
	PartitionId  uint64
	ExtentId     uint64
	ExtentOffset uint64
	Size         uint64
	CRC          uint64
}

// MetaReplica defines the replica of a meta partition
type MetaReplicaInfo struct {
	Addr        string
	ReportTime  int64
	Status      int8 // unavailable, readOnly, readWrite
	IsLeader    bool
	InodeCount  uint64
	DentryCount uint64
	IsLearner   bool
	StoreMode   StoreMode
	ApplyId     uint64
	IsRecover   bool
	VirtualMPs  []VirtualMetaPartition
}

// ClusterView provides the view of a cluster.
type ClusterView struct {
	Name                                string
	LeaderAddr                          string
	DisableAutoAlloc                    bool
	AutoMergeNodeSet                    bool
	NodeSetCapacity                     int
	MetaNodeThreshold                   float32
	DpRecoverPool                       int32
	MpRecoverPool                       int32
	Applied                             uint64
	MaxDataPartitionID                  uint64
	MaxMetaNodeID                       uint64
	MaxMetaPartitionID                  uint64
	EcScrubEnable                       bool
	EcMaxScrubExtents                   uint8
	EcScrubPeriod                       uint32
	EcScrubStartTime                    int64
	MaxCodecConcurrent                  int
	VolCount                            int
	DataNodeStatInfo                    *NodeStatInfo
	MetaNodeStatInfo                    *NodeStatInfo
	EcNodeStatInfo                      *NodeStatInfo
	BadPartitionIDs                     []BadPartitionView
	BadMetaPartitionIDs                 []BadPartitionView
	BadEcPartitionIDs                   []BadPartitionView
	MigratedDataPartitions              []BadPartitionView
	MigratedMetaPartitions              []BadPartitionView
	MetaNodes                           []NodeView
	DataNodes                           []NodeView
	CodEcnodes                          []NodeView
	EcNodes                             []NodeView
	DataNodeBadDisks                    []DataNodeBadDisksView
	SchedulerDomain                     string // todo
	ClientPkgAddr                       string
	UmpJmtpAddr                         string
	UmpJmtpBatch                        uint64
	MetaNodeRocksdbDiskThreshold        float32
	MetaNodeMemModeRocksdbDiskThreshold float32
	RocksDBDiskReservedSpace            uint64
	LogMaxMB                            uint64
	MetaRockDBWalFileSize               uint64 //MB
	MetaRocksWalMemSize                 uint64 //MB
	MetaRocksLogSize                    uint64 //MB
	MetaRocksLogReservedTime            uint64 //day
	MetaRocksLogReservedCnt             uint64
	MetaRocksFlushWalInterval           uint64 //min
	MetaRocksDisableFlushFlag           uint64 //0 flush, !=0 disable flush
	MetaRocksWalTTL                     uint64
	MetaDelEKRecordFileMaxMB            uint64 //MB
	MetaTrashCleanInterval              uint64 //second
	MetaRaftLogSize                     int64  //MB
	MetaRaftLogCap                      int64
	ReuseMPInodeCountThreshold          float64
	ReuseMPDentryCountThreshold         float64
	MetaPartitionMaxInodeCount          uint64
	MetaPartitionMaxDentryCount         uint64
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
	Version    string
}

type BadPartitionView struct {
	Path         string
	PartitionIDs []uint64
}

type DataNodeBadDisksView struct {
	Addr        string
	BadDiskPath []string
}

type ClusterStatInfo struct {
	DataNodeStatInfo *NodeStatInfo
	MetaNodeStatInfo *NodeStatInfo
	EcNodeStatInfo   *NodeStatInfo
	ZoneStatInfo     map[string]*ZoneStat
	SSDZoneStatInfo  *ZoneStat `json:"SSDZoneStatInfo,omitempty"`
	HDDZoneStatInfo  *ZoneStat `json:"HDDZoneStatInfo,omitempty"`
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

func (zoneStat *ZoneStat) Add(otherZoneStat *ZoneStat) {
	if otherZoneStat == nil {
		return
	}
	zoneStat.DataNodeStat.Add(otherZoneStat.DataNodeStat)
	zoneStat.MetaNodeStat.Add(otherZoneStat.MetaNodeStat)
}

func (zoneNodesStat *ZoneNodesStat) Add(otherZoneNodesStat *ZoneNodesStat) {
	if otherZoneNodesStat == nil {
		return
	}
	zoneNodesStat.Total += otherZoneNodesStat.Total
	zoneNodesStat.Used += otherZoneNodesStat.Used
	zoneNodesStat.Avail += otherZoneNodesStat.Avail
	zoneNodesStat.TotalNodes += otherZoneNodesStat.TotalNodes
	zoneNodesStat.WritableNodes += otherZoneNodesStat.WritableNodes
	zoneNodesStat.HighUsedRatioNodes += otherZoneNodesStat.HighUsedRatioNodes
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

type VolStatInfo struct {
	Name             string
	TotalSize        uint64
	UsedSize         uint64
	RealUsedSize     uint64
	UsedRatio        string
	EnableToken      bool
	EnableWriteCache bool
}

// DataPartition represents the structure of storing the file contents.
type DataPartitionInfo struct {
	PartitionID             uint64
	LastLoadedTime          int64
	CreateTime              int64
	ReplicaNum              uint8
	EcMigrateStatus         uint8
	Status                  int8
	IsRecover               bool
	IsFrozen                bool
	IsManual                bool
	Replicas                []*DataReplica
	Hosts                   []string // host addresses
	Peers                   []Peer
	Learners                []Learner
	Zones                   []string
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	OfflinePeerID           uint64
	FileInCoreMap           map[string]*FileInCore
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
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
}

// DataReplica represents the replica of a data partition
type DataReplica struct {
	Addr            string
	ReportTime      int64
	FileCount       uint32
	Status          int8
	HasLoadResponse bool   // if there is any response when loading
	Total           uint64 `json:"TotalSize"`
	Used            uint64 `json:"UsedSize"`
	IsLeader        bool
	NeedsToCompare  bool
	IsLearner       bool
	IsRecover       bool
	DiskPath        string
	MType           string
}

// data partition diagnosis represents the inactive data nodes, corrupt data partitions, and data partitions lack of replicas
type DataPartitionDiagnosis struct {
	InactiveDataNodes           []string
	CorruptDataPartitionIDs     []uint64
	LackReplicaDataPartitionIDs []uint64
}

// meta partition diagnosis represents the inactive meta nodes, corrupt meta partitions, and meta partitions lack of replicas
type MetaPartitionDiagnosis struct {
	InactiveMetaNodes           []string
	CorruptMetaPartitionIDs     []uint64
	LackReplicaMetaPartitionIDs []uint64
}
type ExtentInfo struct {
	FileID     uint64 `json:"fileId"`
	Size       uint64 `json:"size"`
	Crc        uint32 `json:"Crc"`
	IsDeleted  bool   `json:"deleted"`
	ModifyTime int64  `json:"modTime"`
	Source     string `json:"src"`
}

// Status raft status
type Status struct {
	ID                uint64
	NodeID            uint64
	Leader            uint64
	Term              uint64
	Index             uint64
	Commit            uint64
	Applied           uint64
	Vote              uint64
	PendQueue         int
	RecvQueue         int
	AppQueue          int
	Stopped           bool
	RestoringSnapshot bool
	State             string // leader、follower、candidate
	Replicas          map[uint64]*ReplicaStatus
	Log               *LogStatus
}
type LogStatus struct {
	FirstIndex uint64
	LastIndex  uint64
}

// ReplicaStatus  replica status
type ReplicaStatus struct {
	Match       uint64 // copy progress
	Commit      uint64 // commmit position
	Next        uint64
	State       string
	Snapshoting bool
	Paused      bool
	Active      bool
	LastActive  time.Time
	Inflight    int
}

type DataPartitionExtentCrcInfo struct {
	PartitionID               uint64
	ExtentCrcInfos            []*ExtentCrcInfo
	IsBuildValidateCRCTaskErr bool
	ErrMsg                    string
}

type ExtentCrcInfo struct {
	FileID        uint64
	ExtentNum     int
	CrcLocAddrMap map[uint64][]string
}

type RegionView struct {
	Name       string
	RegionType RegionType
	Zones      []string
}

type IDCView struct {
	Name  string
	Zones map[string]string
}

func NewRegionView(name string) (regionView *RegionView) {
	regionView = &RegionView{
		Name:  name,
		Zones: make([]string, 0),
	}
	return
}

type ZoneView struct {
	Name       string
	Status     string
	Region     string
	IDC        string
	MediumType string
	NodeSet    map[uint64]*nodeSetView
}

type nodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []NodeView
	DataNodes   []NodeView
}

// EcPartition represents the structure of storing the file contents by erasure code.
type EcPartitionInfo struct {
	*DataPartitionInfo
	EcReplicas     []*EcReplica
	DataUnitsNum   uint8
	ParityUnitsNum uint8
}

// EcReplica represents the replica of a ec partition
type EcReplica struct {
	Addr            string
	ReportTime      int64
	FileCount       uint32
	Status          int8
	HasLoadResponse bool   // if there is any response when loading
	Total           uint64 `json:"TotalSize"`
	Used            uint64 `json:"UsedSize"`
	IsLeader        bool
	NeedsToCompare  bool
	DiskPath        string
	NodeIndex       uint32
	HttpPort        string
}

// ec partition diagnosis represents the inactive data nodes, corrupt data partitions, and data partitions lack of replicas
type EcPartitionDiagnosis struct {
	InactiveEcNodes           []string
	CorruptEcPartitionIDs     []uint64
	LackReplicaEcPartitionIDs []uint64
}
