package cfs

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type AlarmMetric struct {
	name          string
	lastAlarmTime time.Time
}

type ClusterHost struct {
	host                          string
	deadDataNodes                 map[string]*DeadNode
	deadMetaNodes                 map[string]*DeadNode
	deadFlashNodes                map[string]*DeadNode
	lastTimeAlarmDP               time.Time
	lastTimeAlarmClusterUsedRatio time.Time
	lastTimeOfflineMetaNode       time.Time
	lastTimeOfflineDataNode       time.Time
	offlineDataNodesIn24Hour      map[string]time.Time
	inOfflineDiskDataNodes        map[string]time.Time
	offlineDisksIn24Hour          map[string]time.Time
	dataNodeBadDisk               map[string]time.Time
	isReleaseCluster              bool
	LeaderAddr                    string
	LeaderAddrUpdateTime          time.Time
	lastTimeWarn                  time.Time
	DPMaxPendQueueCount           int
	DPMaxAppliedIDDiffCount       int
	MPMaxPendQueueCount           int
	MPMaxAppliedIDDiffCount       int
	DPPendQueueAlarmThreshold     int // 连续告警次数
	MPPendQueueAlarmThreshold     int
	badPartitionAppliedMap        map[string]*PartitionApplied
	badPartitionAppliedMapMutex   sync.Mutex
	badPartitionPendingMap        map[string]map[string]*PartitionPending // key pType:ID, value replicaAddr
	badPartitionPendingMapMutex   sync.Mutex
	inactiveNodesForCheckVol      map[string]bool
	inactiveNodesForCheckVolLock  sync.RWMutex
	metaNodeDiskRatioWarnTime     time.Time
	metaNodeDiskUsedWarnTime      time.Time
	nodeMemInfo                   map[string]float64
}

func newClusterHost(host string) *ClusterHost {
	ch := &ClusterHost{
		host:                     host,
		deadMetaNodes:            make(map[string]*DeadNode, 0),
		deadDataNodes:            make(map[string]*DeadNode, 0),
		deadFlashNodes:           make(map[string]*DeadNode, 0),
		dataNodeBadDisk:          make(map[string]time.Time, 0),
		offlineDisksIn24Hour:     make(map[string]time.Time, 0),
		offlineDataNodesIn24Hour: make(map[string]time.Time, 0),
		inOfflineDiskDataNodes:   make(map[string]time.Time, 0),
		badPartitionAppliedMap:   make(map[string]*PartitionApplied, 0),
		badPartitionPendingMap:   make(map[string]map[string]*PartitionPending, 0),
		inactiveNodesForCheckVol: make(map[string]bool),
		nodeMemInfo:              make(map[string]float64),
	}
	ch.isReleaseCluster = isReleaseCluster(host)
	return ch
}

func (ch *ClusterHost) String() string {
	return ch.host
}

type DeadNode struct {
	ID                        uint64
	Addr                      string
	LastReportTime            time.Time
	ProcessStatusCount        int // 进程状态检测正常的次数
	IsNeedWarnBySpecialUMPKey bool
}

func (dn *DeadNode) String() string {
	lastReportTime := ""
	if dn != nil {
		lastReportTime = dn.LastReportTime.Format("20060102 15:04:05")
	}
	return fmt.Sprintf("ID:%v,Addr:%v,lastReportTime:%v", dn.ID, dn.Addr, lastReportTime)
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
	VolStatInfo            []*VolSpaceStat // 兼容性暂时保留，应该使用VolStat字段
	MetaNodes              []MetaNodeView
	DataNodes              []DataNodeView
	FlashNodes             []FlashNodeView
	BadPartitionIDs        []BadPartitionView
	MigratedDataPartitions []BadPartitionView
	DataNodeBadDisks       []DataNodeBadDisksView
}

func (cv *ClusterView) String() string {
	return fmt.Sprintf("name[%v],leaderAddr[%v]", cv.Name, cv.LeaderAddr)
}

type DataNodeBadDisksView struct {
	Addr        string
	BadDiskPath []string
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

type VolInfo struct {
	Name               string   `json:"Name"`
	Owner              string   `json:"Owner"`
	CreateTime         int      `json:"CreateTime"`
	Status             int      `json:"Status"`
	TotalSize          int64    `json:"TotalSize"`
	UsedSize           int64    `json:"UsedSize"`
	TrashRemainingDays int      `json:"TrashRemainingDays"`
	IsSmart            bool     `json:"IsSmart"`
	SmartRules         []string `json:"SmartRules"`
	ForceROW           bool     `json:"ForceROW"`
	CompactTag         int      `json:"CompactTag"`
}

type DataNodeView struct {
	Addr                      string
	Status                    bool
	ReportTime                time.Time
	PersistenceDataPartitions []uint64
}

type FlashNodeView struct {
	Addr       string
	Status     bool
	ReportTime time.Time
	//	PersistenceDataPartitions []uint64
}

func (dnv DataNodeView) String() string {
	return fmt.Sprintf("Addr:%v,Status:%v", dnv.Addr, dnv.Status)
}

type MetaNodeView struct {
	ID         uint64
	Addr       string
	Status     bool
	ReportTime time.Time
}

func (mnv MetaNodeView) String() string {
	return fmt.Sprintf("ID:%v,Addr:%v,Status:%v", mnv.ID, mnv.Addr, mnv.Status)
}

type BadPartitionView struct {
	DiskPath     string
	PartitionID  uint64
	PartitionIDs []uint64
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                  uint64
	Name                string
	Owner               string
	DpReplicaNum        uint8
	MpReplicaNum        uint8
	Status              uint8
	Capacity            uint64 // GB
	RwDpCnt             int
	MpCnt               int
	DpCnt               int
	AvailSpaceAllocated uint64 //GB
	MaxMetaPartitionID  uint64
}

type MetaPartitionView struct {
	PartitionID uint64
	PhyPid      uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

type MetaPartition struct {
	PhyPID           uint64
	PartitionID      uint64
	Start            uint64
	End              uint64
	MaxInodeID       uint64
	InodeCount       uint64
	DentryCount      uint64
	IsManual         bool
	IsRecover        bool
	Replicas         []*MetaReplica
	ReplicaNum       uint8
	LearnerNum       uint8
	Status           int8
	VolName          string
	Hosts            []string // spark版本
	PersistenceHosts []string // isReleaseCluster版本
}

func (mp MetaPartition) getMissReplicas() (missHosts []string) {
	for _, host := range mp.Hosts {
		flag := false
		for _, replica := range mp.Replicas {
			if replica.Addr == host {
				flag = true
				break
			}
		}
		if !flag {
			missHosts = append(missHosts, host)
		}
	}
	return
}

type MetaReplica struct {
	Addr        string
	start       uint64
	end         uint64
	nodeId      uint64
	ReportTime  int64
	Status      int8
	IsLeader    bool
	InodeCount  uint64 `json:"InodeCount"`
	DentryCount uint64 `json:"DentryCount"`
}

type NoLeaderMetaPartition struct {
	ID              uint64
	VolName         string
	Count           int
	LastCheckedTime int64
}

func newNoLeaderMetaPartition(mp *MetaPartition) *NoLeaderMetaPartition {
	return &NoLeaderMetaPartition{
		ID:              mp.PartitionID,
		VolName:         mp.VolName,
		Count:           1,
		LastCheckedTime: time.Now().Unix(),
	}
}

type DataNode struct {
	Addr                      string
	PersistenceDataPartitions []uint64
}

type FaultMasterNodesInfo struct {
	FaultCount int
	FaultMsg   string
}

type XBPTicketInfo struct {
	tickerID         int
	url              string
	nodeAddr         string
	ticketType       string
	isReleaseCluster bool
	status           int
	lastUpdateTime   time.Time
}

type MasterLBWarnInfo struct {
	ContinuedTimes int
	LastWarnTime   time.Time
}

type MinRWDPAndMPVolInfo struct {
	Host         string `json:"host"`
	VolName      string `json:"volName"`
	MinRWMPCount int    `json:"minRWMPCount"`
	MinRWDPCount int    `json:"minRWDPCount,omitempty"`
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones []*ZoneView
}

// ZoneView define the view of zone
type ZoneView struct {
	Name       string
	Status     string
	Region     string
	MediumType string
	NodeSet    map[uint64]*nodeSetView
}
type nodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []NodeView
	DataNodes   []NodeView
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

type WarnFaultToTargetUsers struct {
	ClusterDomain    string //	指定检查的集群域名，以及对应的zone列表
	Users            []*TargetUserInfo
	host             *ClusterHost
	dataNodeBadDisks []DataNodeBadDisksView
	//zone 级别故障信息
	zoneBadDataNodeMap map[string][]*DeadNode
	zoneBadMetaNodeMap map[string][]*DeadNode
	zoneBadNodeDisks   map[string][]DataNodeBadDisksView //key zoneName
}

type TargetUserInfo struct {
	UserName  string
	Zones     []string
	TargetGid int    // 用户接收告警的群组，需要 存储机器人 在该群组中
	AppName   string //自定义的应用名称，对用户展示的告警应用名称
}

type PartitionApplied struct {
	Sub             int
	lastDiffReplica []string
	lastCheckTime   time.Time
}

type PartitionPending struct {
	Applied       uint64
	PendQueue     int
	ContinueCount int
	lastCheckTime time.Time
}

// MdcValues cpu 内存 使用率 磁盘 mount 使用率 网络出入口带宽 ，5分钟一个点 ，当前的1分钟
type MdcValues struct {
	DiskPath         string  `json:"path" gorm:"column:disk_path;comment:磁盘地址"`
	NetDevice        string  `json:"netDevice" gorm:"column:net_device;comment:网卡名称"`
	DiskUsagePercent float64 `gorm:"column:fs_usage_percent;comment:秒级 磁盘使用率"`
	CpuUsagePercent  float64 `gorm:"column:cpu_usage_percent;comment:秒级 CPU使用率 "`
	MemUsagePercent  float64 `gorm:"column:mem_usage_percent;comment:秒级 内存使用率"`
	NetRXBytes       float64 `gorm:"column:net_dev_rx_bytes;comment:秒级 网络流入速度 (网络带宽)"`
	NetTXBytes       float64 `gorm:"column:net_dev_tx_bytes;comment:秒级 网络流出速度 (网络带宽)"`
}

type DashboardMdc struct {
	ID              uint64    `gorm:"primary_key;AUTO_INCREMENT"`
	IPAddr          string    `gorm:"column:ip;comment:"`
	Origin          string    `gorm:"column:origin;comment:"`
	MonitorDataType int       `gorm:"column:monitor_data_type"`
	TimeStamp       time.Time `gorm:"column:time_stamp;comment:获取的时间"`
	MdcValues
}

func (DashboardMdc) TableName() string {
	return "tb_dashboard_mdc"
}
