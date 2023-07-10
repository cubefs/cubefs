package img

import (
	"fmt"
	"strings"
	"sync"
)

type Vol struct {
	VolID     uint32
	Goal      uint8
	Size      uint64
	VolStatus int8
	VolHosts  []string
	Pods      map[string]int
}

func (v *Vol) hostsToIPs() (ips string) {
	hosts := make([]string, 0)
	for _, host := range v.VolHosts {
		ipPort := strings.Split(host, ":")
		hosts = append(hosts, ipPort[0])
	}
	return strings.Join(hosts, ",")
}

type ImgClusterView struct {
	ClusterID string
	Vols      []*Vol
}

type ClusterTopologyView struct {
	Cluster        *Cluster
	MaxVolID       uint32
	Applied        uint64
	Topology       *TopologyView
	BadVolsIDs     map[string][]uint32
	MigrateVolsIDs map[string][]uint32
}

type Cluster struct {
}

type TopologyView struct {
	RegionMap map[string]*Region
	ZonesMap  map[string]*ZoneView
}

type ZoneView struct {
	ClusterID string      `json:"cluster"`
	Name      string      `json:"name"`
	Nodes     []*NodeView `json:"nodes"`
	sync.RWMutex
}

type NodeView struct {
	TcpAddr  string
	HttpAddr string
	IsActive bool
}
type Region struct {
	clusterName string
	RegionName  string
	Zones       []string
	index       int
}

type ClusterView struct {
	Name               string
	LeaderAddr         string
	CompactStatus      bool
	DisableAutoAlloc   bool
	Applied            uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	DataNodeStat       *DataNodeSpaceStat
	MetaNodeStat       *MetaNodeSpaceStat
	VolStat            []*VolSpaceStat
	DataNodeStatInfo   *DataNodeSpaceStat
	MetaNodeStatInfo   *MetaNodeSpaceStat
	VolStatInfo        []*VolSpaceStat
	MetaNodes          []MetaNodeView
	DataNodes          []DataNodeView
	BadPartitionIDs    []BadPartitionView
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

type ClusterSpace struct {
	Cluster ClusterSpaceInfo
}

type ClusterSpaceInfo struct {
	ClusterID string `json:"ClusterID"`
	VolSize   int64  `json:"VolSize"`
	Space     *Space
}

type Space struct {
	TTLDeleteLastDay int    `json:"TTLDeleteLastDay"`
	Total            string `json:"Total"`
	Used             string `json:"Used"`
	RealUsed         string `json:"RealUsed"`
	Avail            string `json:"Avail"`
	Increased        string `json:"Increased"`
	UsedOfAllVols    string `json:"UsedOfAllVols"`
	AvailOfAllVols   string `json:"AvailOfAllVols"`
	TotalOfAllVols   string `json:"TotalOfAllVols"`
}
