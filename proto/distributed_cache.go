package proto

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/fastcrc32"
)

const (
	PageSize                = 4 * 1024
	CACHE_BLOCK_SIZE        = 1 << 20
	ReadCacheTimeout        = 1 // second
	DefaultCacheTTLSec      = 5 * 24 * 3600
	FlashGroupDefaultWeight = 1
	FlashGroupMaxWeight     = 30
)

const (
	FlashGroupStatus_Inactive FlashGroupStatus = 0x0
	FlashGroupStatus_Active   FlashGroupStatus = 0x1
)

const (
	SlotStatus_Completed SlotStatus = 0x0
	SlotStatus_Creating  SlotStatus = 0x1
	SlotStatus_Deleting  SlotStatus = 0x2

	FlashNodeTaskWorker     = "task"
	FlashNodeCacheWorker    = "cache"
	FlashManualWarmupAction = "warmup"
	FlashManualClearAction  = "clear"
	FlashManualCheckAction  = "check"
)

type FlashGroupStatus int

type SlotStatus int

func (status FlashGroupStatus) String() string {
	switch status {
	case FlashGroupStatus_Inactive:
		return "Inactive"
	case FlashGroupStatus_Active:
		return "Active"
	default:
		return "Unknown"
	}
}

func (status FlashGroupStatus) IsActive() bool {
	return status == FlashGroupStatus_Active
}

type ManualTaskStatus int

// 0 init,1 running,2 pause,3 success,4 failed
const (
	Flash_Task_Init ManualTaskStatus = iota
	Flash_Task_Running
	Flash_Task_Pause
	Flash_Task_Success
	Flash_Task_Failed
	Flash_Task_Stop
)

func ManualTaskDone(status int) bool {
	success, failed, stop := int(Flash_Task_Success), int(Flash_Task_Failed), int(Flash_Task_Stop)
	return status == success || status == failed || status == stop
}

func ManualTaskIsRunning(status int) bool {
	running := int(Flash_Task_Running)
	return status == running
}

type FlashGroupInfo struct {
	ID    uint64   `json:"i"`
	Slot  []uint32 `json:"s"` // FlashGroup's position in hasher ring
	Hosts []string `json:"h"`
}

type FlashGroupView struct {
	Enable      bool
	FlashGroups []*FlashGroupInfo
}

func (f *FlashGroupInfo) String() string {
	return fmt.Sprintf("{ID: %d, Hosts: %v}", f.ID, f.Hosts)
}

func (ds *DataSource) EncodeBinaryTo(b []byte) {
	binary.BigEndian.PutUint64(b[:8], ds.FileOffset)
	binary.BigEndian.PutUint64(b[8:16], ds.PartitionID)
	binary.BigEndian.PutUint64(b[16:24], ds.ExtentID)
	binary.BigEndian.PutUint64(b[24:32], ds.ExtentOffset)
	binary.BigEndian.PutUint64(b[32:40], ds.Size_)
	binary.BigEndian.PutUint16(b[40:42], uint16(len(ds.Hosts)))
	off := 42
	for i := 0; i < len(ds.Hosts); i++ {
		b[off] = uint8(len(ds.Hosts[i]))
		off += 1
		copy(b[off:off+len(ds.Hosts[i])], ds.Hosts[i])
		off += len(ds.Hosts[i])
	}
}

func (ds *DataSource) DecodeBinaryFrom(b []byte) uint32 {
	ds.FileOffset = binary.BigEndian.Uint64(b[:8])
	ds.PartitionID = binary.BigEndian.Uint64(b[8:16])
	ds.ExtentID = binary.BigEndian.Uint64(b[16:24])
	ds.ExtentOffset = binary.BigEndian.Uint64(b[24:32])
	ds.Size_ = binary.BigEndian.Uint64(b[32:40])
	hostLen := binary.BigEndian.Uint16(b[40:42])
	off := 42
	ds.Hosts = make([]string, 0)
	for i := uint16(0); i < hostLen; i++ {
		l := b[off]
		off += 1
		ds.Hosts = append(ds.Hosts, string(b[off:uint8(off)+l]))
		off += int(l)
	}
	return uint32(off)
}

func (ds *DataSource) EncodeBinaryLen() int {
	hostsLen := 0
	for i := 0; i < len(ds.Hosts); i++ {
		hostsLen += 1 + len(ds.Hosts[i])
	}
	return 40 + 2 + hostsLen
}

func (cr *CacheRequest) DecodeBinaryFrom(b []byte) {
	volLen := binary.BigEndian.Uint16(b[:2])
	cr.Volume = string(b[2 : 2+volLen])
	off := 2 + uint32(volLen)
	cr.FixedFileOffset = binary.BigEndian.Uint64(b[off : off+8]) // FixedFileOffset (uint64, 8 bytes)
	off += 8
	cr.Inode = binary.BigEndian.Uint64(b[off : off+8]) // Inode (uint64, 8 bytes)
	off += 8
	cr.Version = binary.BigEndian.Uint32(b[off : off+4]) // Version (uint32, 4 bytes)
	off += 4
	cr.TTL = int64(binary.BigEndian.Uint64(b[off : off+8])) // TTL (uint64, 8 bytes)
	off += 8
	sourceNum := binary.BigEndian.Uint32(b[off : off+4]) // Number of sources (uint32, 4 bytes)
	off += 4
	cr.Sources = make([]*DataSource, 0)
	for i := uint32(0); i < sourceNum; i++ {
		source := new(DataSource)
		sourceLen := binary.BigEndian.Uint16(b[off : off+2])
		off += 2
		source.DecodeBinaryFrom(b[off : off+uint32(sourceLen)])
		off += uint32(sourceLen)
		cr.Sources = append(cr.Sources, source)
	}
}

func (cr *CacheRequest) EncodeBinaryTo(b []byte) {
	binary.BigEndian.PutUint16(b[:2], uint16(len(cr.Volume))) // Length of Volume (uint16, 2bytes)
	copy(b[2:2+len(cr.Volume)], cr.Volume)                    // Volume
	off := 2 + len(cr.Volume)
	binary.BigEndian.PutUint64(b[off:off+8], cr.FixedFileOffset) // FixedFileOffset (uint64, 8 bytes)
	off += 8
	binary.BigEndian.PutUint64(b[off:off+8], cr.Inode) // Inode (uint64, 8 bytes)
	off += 8
	binary.BigEndian.PutUint32(b[off:off+4], cr.Version) // Version (uint32, 4 bytes)
	off += 4
	binary.BigEndian.PutUint64(b[off:off+8], uint64(cr.TTL)) // TTL (uint64, 8 bytes)
	off += 8
	binary.BigEndian.PutUint32(b[off:off+4], uint32(len(cr.Sources))) // Number of sources (uint32, 4 bytes)
	off += 4
	for i := 0; i < len(cr.Sources); i++ {
		sourceLen := cr.Sources[i].EncodeBinaryLen()
		binary.BigEndian.PutUint16(b[off:off+2], uint16(sourceLen))
		off += 2
		cr.Sources[i].EncodeBinaryTo(b[off : off+sourceLen])
		off += sourceLen
	}
}

func (cr *CacheRequest) EncodeBinaryLen() int {
	var sourcesLen int
	for i := 0; i < len(cr.Sources); i++ {
		sourcesLen += 2 + cr.Sources[i].EncodeBinaryLen()
	}
	return 2 + len(cr.Volume) + 8 + 8 + 4 + 4 + sourcesLen + 8
}

func (cr *CacheReadRequest) EncodeBinaryTo(b []byte) {
	var off int
	binary.BigEndian.PutUint64(b[off:off+8], cr.Offset)
	off += 8
	binary.BigEndian.PutUint64(b[off:off+8], cr.Size_)
	off += 8
	crLen := cr.CacheRequest.EncodeBinaryLen()
	binary.BigEndian.PutUint32(b[off:off+4], uint32(crLen))
	off += 4
	cr.CacheRequest.EncodeBinaryTo(b[off : off+crLen])
}

func (cr *CacheReadRequest) DecodeBinaryFrom(b []byte) {
	var off uint32
	cr.Offset = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	cr.Size_ = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	crLen := binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	cr.CacheRequest = new(CacheRequest)
	cr.CacheRequest.DecodeBinaryFrom(b[off:crLen])
}

func (cr *CacheReadRequest) EncodeBinaryLen() int {
	return 4 + cr.CacheRequest.EncodeBinaryLen() + 8 + 8
}

func (ds *DataSource) String() string {
	if ds == nil {
		return ""
	}
	return fmt.Sprintf("FileOffset(%v) PartitionID(%v) ExtentID(%v) ExtentOffset(%v) Size(%v) Hosts(%v)", ds.FileOffset, ds.PartitionID, ds.ExtentID, ds.ExtentOffset, ds.Size_, ds.Hosts)
}

func (cacheReq *CacheRequest) String() string {
	if cacheReq == nil {
		return ""
	}
	return fmt.Sprintf("CacheRequest[Volume(%v) Inode(%v) FixedFileOffset(%v) Sources(%v) TTL(%v) ]", cacheReq.Volume, cacheReq.Inode, cacheReq.FixedFileOffset, len(cacheReq.Sources), cacheReq.TTL)
}

func (cr *CacheReadRequest) String() string {
	if cr == nil {
		return ""
	}
	return fmt.Sprintf("cacheReadRequest[Volume(%v) Inode(%v) FixedFileOffset(%v) Sources(%v) TTL(%v) Offset(%v) Size(%v)]",
		cr.CacheRequest.Volume, cr.CacheRequest.Inode, cr.CacheRequest.FixedFileOffset, len(cr.CacheRequest.Sources), cr.CacheRequest.TTL, cr.Offset, cr.Size_)
}

func (pr *CachePrepareRequest) String() string {
	if pr == nil {
		return ""
	}
	return fmt.Sprintf("cachePrepareRequest[Volume(%v) Inode(%v) FixedFileOffset(%v) Sources(%v) TTL(%v)]", pr.CacheRequest.Volume, pr.CacheRequest.Inode, pr.CacheRequest.FixedFileOffset, len(pr.CacheRequest.Sources), pr.CacheRequest.TTL)
}

type FlashGroupsAdminView struct {
	FlashGroups []FlashGroupAdminView
}

type FlashGroupAdminView struct {
	ID             uint64
	Slots          []uint32
	Weight         uint32
	Status         FlashGroupStatus
	SlotStatus     SlotStatus
	PendingSlots   []uint32
	Step           uint32
	FlashNodeCount int
	ZoneFlashNodes map[string][]*FlashNodeViewInfo
}

type FlashNodeViewInfo struct {
	ID            uint64
	Addr          string
	ReportTime    time.Time
	IsActive      bool
	Version       string
	ZoneName      string
	FlashGroupID  uint64
	IsEnable      bool
	DiskStat      []*FlashNodeDiskCacheStat
	LimiterStatus *FlashNodeLimiterStatusInfo
}

type FlashNodeStat struct {
	WaitForCacheBlock bool
	NodeLimit         uint64
	VolLimit          map[string]uint64
	CacheStatus       []*CacheStatus
}

type CacheStatus struct {
	DataPath string   `json:"data_path"`
	Medium   string   `json:"medium"`
	MaxAlloc int64    `json:"max_alloc"`
	HasAlloc int64    `json:"has_alloc"`
	Used     int64    `json:"used"`
	Total    int64    `json:"total"`
	HitRate  float64  `json:"hit_rate"`
	Evicts   int      `json:"evicts"`
	Num      int      `json:"num"`
	Capacity int      `json:"capacity"`
	Keys     []string `json:"keys"`
	Status   int      `json:"status"`
}

type SlotStat struct {
	SlotId      uint32    `json:"slot_id"`
	OwnerSlotId uint32    `json:"owner_slot_id"`
	HitCount    uint32    `json:"hit_count"`
	HitRate     float64   `json:"hit_rate"`
	RecentTime  time.Time `json:"recent_time"`
}

type FlashNodeSlotStat struct {
	NodeId   uint64
	Addr     string
	SlotStat []*SlotStat
}

func ComputeSourcesVersion(sources []*DataSource, gen uint64) (version uint32) {
	if len(sources) == 0 {
		return 0
	}
	crcData := make([]byte, len(sources)*32+8)
	for i, s := range sources {
		binary.BigEndian.PutUint64(crcData[i*32:i*32+8], s.PartitionID)
		binary.BigEndian.PutUint64(crcData[i*32+8:i*32+16], s.ExtentID)
		binary.BigEndian.PutUint64(crcData[i*32+16:i*32+24], s.ExtentOffset)
		binary.BigEndian.PutUint64(crcData[i*32+24:i*32+32], s.Size_)
	}
	binary.BigEndian.PutUint64(crcData[len(sources)*32:len(sources)*32+8], gen)
	return fastcrc32.Checksum(crcData)
}

func ComputeCacheBlockSlot(volume string, inode, fixedFileOffset uint64) uint32 {
	volLen := len(volume)
	buf := make([]byte, volLen+16)
	copy(buf[:volLen], volume)
	binary.BigEndian.PutUint64(buf[volLen:volLen+8], inode)
	binary.BigEndian.PutUint64(buf[volLen+8:volLen+16], fixedFileOffset)
	return fastcrc32.Checksum(buf)
}

type FlashManualTask struct {
	Id                   string
	Action               string
	VolName              string
	Status               int
	ManualTaskConfig     ManualTaskConfig
	ManualTaskStatistics *ManualTaskStatistics
	StartTime            *time.Time
	UpdateTime           *time.Time
	EndTime              *time.Time
	RcvStop              bool
	Done                 bool
	sync.Mutex
}

type ManualTaskConfig struct {
	Prefix                  string
	TraverseFileConcurrency int
	HandlerFileConcurrency  int
	TotalFileSizeLimit      int64
}

type ManualTaskStatistics struct {
	TotalFileScannedNum int64
	TotalFileCachedNum  int64
	TotalDirScannedNum  int64
	TotalExtentKeyNum   int64
	ErrorCacheNum       int64
	ErrorReadDirNum     int64
	TotalCacheSize      int64
	LastCacheSize       int64
	FlashNode           string
}

type FlashNodeManualTaskRequest struct {
	MasterAddr string
	FnNodeAddr string
	Task       *FlashManualTask
}

type FlashNodeManualTaskResponse struct {
	ID         string
	FlashNode  string
	StartTime  *time.Time
	EndTime    *time.Time
	UpdateTime *time.Time
	Done       bool
	Status     uint8
	StartErr   string
	Volume     string
	RcvStop    bool
	ManualTaskStatistics
}

type FlashNodeManualTaskCommand struct {
	ID      string
	Command string
}

type ScanItem struct {
	ParentId     uint64 `json:"pid"`   // FileID value of the parent inode.
	Inode        uint64 `json:"inode"` // FileID value of the current inode.
	Name         string `json:"name"`  // Name of the current dentry.
	Path         string `json:"path"`  // Path of the current dentry.
	Type         uint32 `json:"type"`  // Type of the current dentry.
	Op           string `json:"op"`    // to warmup or check
	Size         uint64 `json:"size"`  // for warmup: size of the current dentry
	StorageClass uint32 `json:"sc"`    // for warmup: storage class of the current dentry
	WriteGen     uint64 `json:"gen"`   // for warmup: used to determine whether a file is modified
}

func (flt *FlashManualTask) GetPathPrefix() string {
	return strings.TrimPrefix(strings.TrimRight(flt.ManualTaskConfig.Prefix, "/"), "/")
}

func (flt *FlashManualTask) SetResponse(taskRsp *FlashNodeManualTaskResponse) {
	t := time.Now()
	flt.UpdateTime = &t
	flt.ManualTaskStatistics.FlashNode = taskRsp.FlashNode
	flt.ManualTaskStatistics.TotalFileScannedNum = taskRsp.TotalFileScannedNum
	flt.ManualTaskStatistics.TotalFileCachedNum = taskRsp.TotalFileCachedNum
	flt.ManualTaskStatistics.TotalDirScannedNum = taskRsp.TotalDirScannedNum
	flt.ManualTaskStatistics.ErrorCacheNum = taskRsp.ErrorCacheNum
	flt.ManualTaskStatistics.ErrorReadDirNum = taskRsp.ErrorReadDirNum
	flt.ManualTaskStatistics.TotalCacheSize = taskRsp.TotalCacheSize
	flt.ManualTaskStatistics.LastCacheSize = taskRsp.LastCacheSize
	flt.Done = taskRsp.Done
}
