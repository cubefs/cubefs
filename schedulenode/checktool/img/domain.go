package img

import (
	"fmt"
	"sync"
	"time"
)

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

type ExceptionRequest struct {
	lastTime int64
	count    int
}

type LbDownload struct {
	ScheduleInterval int             `json:"imgInterval"`
	SystemAppInfo    []SystemAppInfo `json:"imgSystemApp"`
	ComIpPath        string          `json:"imgComIpPath"`
	LocalIpPath      string          `json:"imgLocalIpPath"`
	LbPwd            string          `json:"imgLbPwd"`
	ComConfPath      string          `json:"imgComConfPath"`
	LocalConfPath    string          `json:"imgLocalConfPath"`
	JDOS             JDOSApi         `json:"jDOS"`
	IgnoreFileName   string          `json:"imgIgnoreFileName"`
	LbNotMatchCnt    int             `json:"imgLbNotMatchCnt"`
}

func (lb *LbDownload) LbInfo() string {
	return fmt.Sprintf("ScheduleInterval:%v;SystemAppInfo:%v;com:ipPath:%v,confPath:%v;local:ipPath:%v,confPath:%v;JDOS:%v;IgnoreFileName:%v",
		lb.ScheduleInterval, lb.SystemAppInfo, lb.ComIpPath, lb.ComConfPath, lb.LocalIpPath, lb.LocalConfPath, lb.JDOS, lb.IgnoreFileName)
}

type SystemAppInfo struct {
	System string `json:"system"`
	App    string `json:"app"`
}

type AppGroups struct {
	systemApp  SystemAppInfo
	groupNames []string
}

type JDOSApi struct {
	Erp   string `json:"erp"`
	Token string `json:"token"`
	Site  string `json:"site"`
}

type Node struct {
	ClusterID          string
	Addr               string `json:"TcpAddr"`
	MaxDiskAvailWeight uint64 `json:"MaxDiskAvailWeight"`
	Total              uint64 `json:"TotalWeight"`
	Used               uint64 `json:"UsedWeight"`
	Ratio              float64
	SelectCount        uint64
	Carry              float64
	BalanceDiskCarry   float64 // disk used ratio
	ReportTime         time.Time
	IsActive           bool
	IsMigrated         bool // is processed by migrateDataNodeAPI，true for yes
	Ver                string
	ZoneName           string `json:"Zone"`
	TaskLen            int
	HttpAddr           string
	VolInfoCount       int
	DiskCount          int
	VolInfo            []*VolReport
	DiskInfo           []*DiskInfo
}

type VolReport struct {
	VolID         uint32
	VolStatus     int
	Files         uint32
	Total         uint64 `json:"TotalSize"`
	Used          uint64 `json:"UsedSize"`
	DiskPath      string
	DeletePercent float64
}
type DiskInfo struct {
	status      int32
	RealAvail   int64
	VolsAvail   int64
	MinRestSize uint64
	MaxErrs     int32
	Total       int64
	Path        string
	VolIDs      []uint32
}
