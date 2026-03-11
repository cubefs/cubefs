package proto

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

type MrBalanceInfo struct {
	Source       string    `json:"source" bson:"source"`
	SrcMemSize   uint64    `json:"srcMemSize" bson:"srcmemsize"`
	SrcNodeSetId uint64    `json:"srcNodeSetId" bson:"srcnodesetid"`
	SrcZoneName  string    `json:"srcZoneName" bson:"srczonename"`
	SrcRack      string    `json:"srcRack" bson:"srcrack"`
	Destination  string    `json:"destination" bson:"destination"`
	DstId        uint64    `json:"dstID" bson:"dstid"`
	DstNodeSetId uint64    `json:"dstNodeSetId" bson:"dstnodesetid"`
	DstZoneName  string    `json:"dstZoneName" bson:"dstzonename"`
	DstRack      string    `json:"dstRack" bson:"dstrack"`
	Status       string    `json:"status" bson:"status"`
	Msg          string    `json:"msg" bson:"msg"`
	StoreMode    StoreMode `json:"storeMode"`
}

type MetaBalancePlan struct {
	ID         uint64           `json:"id" bson:"id"`
	CrossZone  bool             `json:"crossZone" bson:"crosszone"`
	Original   []*MrBalanceInfo `json:"original" bson:"original"`
	OverLoad   []*MrBalanceInfo `json:"overLoad" bson:"overload"`
	Plan       []*MrBalanceInfo `json:"plan" bson:"plan"`
	PlanNum    int              `json:"planNum" bson:"plannum"`
	InodeCount uint64           `json:"inodeCount" bson:"inodecount"`
	StartTime  time.Time        `json:"startTime"`
	Msg        string           `json:"msg" bson:"msg"`
}

type MetaNodeBalanceInfo struct {
	ID             uint64   `json:"id"`
	Addr           string   `json:"address"`
	DomainAddr     string   `json:"domainAddress"`
	ZoneName       string   `json:"zone"`
	Rack           string   `json:"rack"`
	NodeSetID      uint64   `json:"nodeSetId"`
	Total          uint64   `json:"totalMem"`
	Used           uint64   `json:"usedMem"`
	Free           uint64   `json:"freeMem"`
	Ratio          float64  `json:"ratio"`
	NodeMemTotal   uint64   `json:"nodeMemTotal"`
	NodeMemUsed    uint64   `json:"nodeMemUsed"`
	NodeMemFree    uint64   `json:"nodeMemFree"`
	NodeMemRatio   float64  `json:"nodeMemRatio"`
	MpCount        int      `json:"mpCount"`
	MetaPartitions []uint64 `json:"-"`
	InodeCount     uint64   `json:"inodeCount"`
	Estimate       int      `json:"estimate"`
	PlanCnt        int      `json:"planCount"`
	Selected       int      `json:"selected"`
}

type NodeSetPressureView struct {
	NodeSetID uint64                          `json:"nodeSetId"`
	Number    int                             `json:"number"`
	MetaNodes map[uint64]*MetaNodeBalanceInfo `json:"metaNodes"`
}

type ZonePressureView struct {
	ZoneName string                          `json:"zone"`
	Status   string                          `json:"status"`
	NodeSet  map[uint64]*NodeSetPressureView `json:"nodeSet"`
}

type ClusterPlan struct {
	sync.RWMutex                                 // 保护 Low 和 RocksdbLow map 的并发访问
	Low             map[string]*ZonePressureView `json:"-" bson:"-"`
	RocksdbLow      map[string]*ZonePressureView `json:"-" bson:"-"`
	Plan            []*MetaBalancePlan           `json:"plan" bson:"plan"`
	Name            string                       `json:"name"`
	DoneNum         int32                        `json:"doneMpCount" bson:"donenum"`
	RunningNum      int32                        `json:"runningMpCount"`
	ErrorNum        int32                        `json:"errorMpCount"`
	UndoNum         int32                        `json:"undoMpCount"`
	Total           int                          `json:"total" bson:"total"`
	Status          string                       `json:"status" bson:"status"`
	Expire          time.Time                    `json:"expire" bson:"expire"`
	Type            string                       `json:"type" bson:"type"`
	Msg             string                       `json:"msg" bson:"msg"`
	Mode            StoreMode                    `json:"storeMode"`
	ModeCnt         int                          `json:"storeModeCount"`
	StartId         uint64                       `json:"startId"`
	EndId           uint64                       `json:"endId"`
	RackLevel       RackAwareLevel               `json:"rackLevel"`
	StartTime       time.Time                    `json:"startTime"`
	EndTime         time.Time                    `json:"endTime"`
	FailedList      []uint64                     `json:"FailedMetaPartitions"`
	DoneReplicaNum  int32                        `json:"doneReplicaNum"`
	RunReplicaNum   int32                        `json:"runningReplicaNum"`
	ErrorReplicaNum int32                        `json:"errorReplicaNum"`
	UndoReplicaNum  int32                        `json:"undoReplicaNum"`
	TotalReplicaNum int                          `json:"totalReplicaNum"`
	ProcessPercent  float64                      `json:"processPercent"`
	AutoPromote     bool                         `json:"autoPromoteLearner"`
	SelectType      int                          `json:"selectType"` // 0: not set. 1: zone name. 2: node set id. 3: node address list.
	ZoneName        string                       `json:"zoneName"`
	NodeSetID       uint64                       `json:"nodesetId"`
	Tag             string                       `json:"tag"`
}

type MetaReplicaChecksumInfo struct {
	Addr    string `json:"addr"`
	ApplyID uint64 `json:"applyID"`
	Md5Sum  string `json:"md5Sum"`
}

type MetaPartitionChecksumInfo struct {
	PartitionID uint64                     `json:"partitionID"`
	Status      string                     `json:"status"`
	Replicas    []*MetaReplicaChecksumInfo `json:"replicas"`
	StartTime   time.Time                  `json:"startTime"`
	Msg         string                     `json:"msg"`
	LastApplyID uint64                     `json:"lastApplyID"`
}

type MetaPartitionsChecksumPlan struct {
	Status       string                       `json:"status"`
	CheckSumList []*MetaPartitionChecksumInfo `json:"checksumList"`
	Total        int32                        `json:"total"`
	Undo         int32                        `json:"undo"`
	Running      int32                        `json:"running"`
	Done         int32                        `json:"done"`
	FailedList   []uint64                     `json:"failedList"`
	Progress     float64                      `json:"progress"`
	StartTime    time.Time                    `json:"startTime"`
	EndTime      time.Time                    `json:"endTime"`
	Expire       time.Time                    `json:"expire"`
	Msg          string                       `json:"msg"`
}

type MetaPartitionLearnerInfo struct {
	ID         uint64   `json:"id"`
	Learners   []string `json:"learners"`
	DeleteAddr []string `json:"deleteAddr"`
	Msg        string   `json:"msg"`
}

type PromoteLearnerPlan struct {
	Name       string                      `json:"name"`
	StartID    uint64                      `json:"startId"`
	EndID      uint64                      `json:"endId"`
	Mode       StoreMode                   `json:"mode"`
	SelectType int                         `json:"selectType"` // 0: not set. 1: zone name. 2: node set id. 3: node address list.
	ZoneName   string                      `json:"zoneName"`
	NodeSetID  uint64                      `json:"nodesetId"`
	Tag        string                      `json:"tag"`
	Learners   []*MetaPartitionLearnerInfo `json:"learners"`
	TotalNum   int32                       `json:"totalNum"`
	UndoNum    int32                       `json:"undoNum"`
	RunningNum int32                       `json:"runningNum"`
	DoneNum    int32                       `json:"doneNum"`
	FailedNum  int32                       `json:"failedNum"`
	FailedList []uint64                    `json:"failedList"`
	Progress   float64                     `json:"progress"`
	StartTime  time.Time                   `json:"startTime"`
	EndTime    time.Time                   `json:"endTime"`
	Expire     time.Time                   `json:"expire"`
	Status     string                      `json:"status"`
	Msg        string                      `json:"msg"`
}

type DataNodeSpace struct {
	Used        uint64  `json:"used"`
	Free        uint64  `json:"free"`
	Total       uint64  `json:"total"`
	Ratio       float64 `json:"ratio"`
	WritableNum int     `json:"writableNum"`
	Tag         string  `json:"tag"`
}

type MetaNodeSpace struct {
	MemUsed            uint64  `json:"memUsed"`
	MemFree            uint64  `json:"memFree"`
	MemTotal           uint64  `json:"memTotal"`
	MemRatio           float64 `json:"memRatio"`
	MemWritableNum     int     `json:"memWritableNum"`
	RocksdbUsed        uint64  `json:"rocksdbUsed"`
	RocksdbFree        uint64  `json:"rocksdbFree"`
	RocksdbTotal       uint64  `json:"rocksdbTotal"`
	RocksdbRatio       float64 `json:"rocksdbRatio"`
	RocksdbWritableNum int     `json:"rocksdbWritableNum"`
	SystemMemoryUsed   uint64  `json:"systemMemoryUsed"`
	SystemMemoryFree   uint64  `json:"systemMemoryFree"`
	SystemMemoryTotal  uint64  `json:"systemMemoryTotal"`
	SystemMemoryRatio  float64 `json:"systemMemoryRatio"`
	Tag                string  `json:"tag"`
}

type TagMismatchSample struct {
	Vol         string `json:"volume,omitempty"`
	PartitionID uint64 `json:"partitionId"`
	NodeAddr    string `json:"nodeAddr"`
	PeerTag     string `json:"peerTag"`
	NodeTag     string `json:"nodeTag"`
}

type TagSummary struct {
	AutoFixTag          bool                      `json:"autoFixTag"`
	ClusterDpTag        string                    `json:"clusterDpTag"`
	ClusterMpTag        string                    `json:"clusterMpTag"`
	VolumeNum           int                       `json:"volumeNum"`
	VolWithTagNum       int                       `json:"volumeWithTagNum"`
	VolWithoutTagNum    int                       `json:"volumeWithoutTagNum"`
	TotalDpNum          int                       `json:"totalDpNum"`
	TotalMpNum          int                       `json:"totalMpNum"`
	UnmatchDpNum        int                       `json:"unmatchDpNum"`
	DecommissionDpNum   int                       `json:"decommissionDpNum"`
	UnmatchMpNum        int                       `json:"unmatchMpNum"`
	MpPlanStatus        string                    `json:"mpPlanStatus"`
	MpDecommissionNum   uint32                    `json:"mpDecommissionNum"`
	DpCheckThreadStatus string                    `json:"dpCheckThreadStatus"`
	MpCheckThreadStatus string                    `json:"mpCheckThreadStatus"`
	UnmatchDpSamples    []TagMismatchSample       `json:"unmatchDpSamples,omitempty"`
	UnmatchMpSamples    []TagMismatchSample       `json:"unmatchMpSamples,omitempty"`
	DataNodeTagCount    map[string]int            `json:"dataNodeTagCount"`
	MetaNodeTagCount    map[string]int            `json:"metaNodeTagCount"`
	DataNodeSpace       map[string]*DataNodeSpace `json:"dataNodeSpace,omitempty"`
	MetaNodeSpace       map[string]*MetaNodeSpace `json:"metaNodeSpace,omitempty"`
	FailedMpKeys        []string                  `json:"failedMpKeys,omitempty"`
	LastDpQuitReason    string                    `json:"DpThreadLastQuitReason,omitempty"`
	LastMpQuitReason    string                    `json:"MpThreadLastQuitReason,omitempty"`
	LastDpThreadTime    *time.Time                `json:"DpThreadLastQuitTime,omitempty"`
	LastMpThreadTime    *time.Time                `json:"MpThreadLastQuitTime,omitempty"`
}

var TagPattern = regexp.MustCompile("^[0-9A-Za-z]{1,49}$")

func ValidateTag(tag string) bool {
	if tag == "null" {
		return true
	}
	rules := strings.Split(tag, ";")
	srcTagSet := make(map[string]struct{}, 0)
	for _, rule := range rules {
		groups := strings.Split(rule, "->")
		if len(groups) != 2 {
			return false
		}
		srcItems := strings.Split(groups[0], ",")
		dstItems := strings.Split(groups[1], ",")
		if len(srcItems) != len(dstItems) || len(srcItems) == 0 {
			return false
		}
		for _, item := range srcItems {
			if !TagPattern.MatchString(item) {
				return false
			}
			srcTagSet[item] = struct{}{}
		}
		for _, item := range dstItems {
			if !TagPattern.MatchString(item) {
				return false
			}
			if _, exist := srcTagSet[item]; exist {
				return false
			}
		}
	}
	return true
}

const (
	ValidateTagFormat = "Example: 'tag1->tag2', 'tag1,tag1->tag2,tag2;tag3->tag4' or 'null' to clear tag."
	TagFormatErr      = "tag format error. " + ValidateTagFormat
)

type VolTagSummary struct {
	MpTag            string              `json:"mpTag"`
	DpTag            string              `json:"dpTag"`
	EffectiveMpTags  []string            `json:"effectiveMpTags"`
	EffectiveDpTags  []string            `json:"effectiveDpTags"`
	Vol              string              `json:"volume"`
	VolStatus        uint8               `json:"volStatus"`
	TotalDpNum       int                 `json:"totalDpNum"`
	TotalMpNum       int                 `json:"totalMpNum"`
	UnmatchDpNum     int                 `json:"unmatchDpNum"`
	UnmatchDps       string              `json:"unmatchDps"`
	UnmatchMpNum     int                 `json:"unmatchMpNum"`
	UnmatchMps       string              `json:"unmatchMps"`
	UnmatchDpSamples []TagMismatchSample `json:"unmatchDpSamples"`
	UnmatchMpSamples []TagMismatchSample `json:"unmatchMpSamples"`
	FailedMpKeys     []string            `json:"failedMpKeys,omitempty"`
}
