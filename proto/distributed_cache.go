package proto

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/util/fastcrc32"
)

const (
	PageSize           = 4 * 1024
	CACHE_BLOCK_SIZE   = 1 << 20
	ReadCacheTimeout   = 1 // second
	DefaultCacheTTLSec = 5 * 60
)

const (
	FlashGroupStatus_Inactive FlashGroupStatus = 0x0
	FlashGroupStatus_Active   FlashGroupStatus = 0x1
)

type FlashGroupStatus int

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

type FlashGroupInfo struct {
	ID    uint64   `json:"i"`
	Slot  []uint32 `json:"s"` // FlashGroup's position in hasher ring
	Hosts []string `json:"h"`
}

type FlashGroupView struct {
	Enable      bool
	FlashGroups []*FlashGroupInfo
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
	Status         FlashGroupStatus
	FlashNodeCount int
	ZoneFlashNodes map[string][]*FlashNodeViewInfo
}

type FlashNodeViewInfo struct {
	ID           uint64
	Addr         string
	ReportTime   time.Time
	IsActive     bool
	Version      string
	ZoneName     string
	FlashGroupID uint64
	IsEnable     bool
}

type FlashNodeStat struct {
	NodeLimit   uint64
	VolLimit    map[string]uint64
	CacheStatus *CacheStatus
}

type CacheStatus struct {
	MaxAlloc int64    `json:"max_alloc"`
	HasAlloc int64    `json:"has_alloc"`
	Used     int64    `json:"used"`
	Total    int64    `json:"total"`
	HitRate  float64  `json:"hit_rate"`
	Evicts   int      `json:"evicts"`
	Num      int      `json:"num"`
	Capacity int      `json:"capacity"`
	Keys     []string `json:"keys"`
}

func ComputeSourcesVersion(sources []*DataSource) (version uint32) {
	if len(sources) == 0 {
		return 0
	}
	crcData := make([]byte, len(sources)*32)
	for i, s := range sources {
		binary.BigEndian.PutUint64(crcData[i*32:i*32+8], s.PartitionID)
		binary.BigEndian.PutUint64(crcData[i*32+8:i*32+16], s.ExtentID)
		binary.BigEndian.PutUint64(crcData[i*32+16:i*32+24], s.ExtentOffset)
		binary.BigEndian.PutUint64(crcData[i*32+24:i*32+32], s.Size_)
	}
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
