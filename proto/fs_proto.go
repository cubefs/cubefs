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
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	RootIno = uint64(1)
)

// Mode returns the fileMode.
func Mode(osMode os.FileMode) uint32 {
	return uint32(osMode)
}

// OsMode returns os.FileMode.
func OsMode(mode uint32) os.FileMode {
	return os.FileMode(mode)
}

// Returns os.FileMode masked by os.ModeType
func OsModeType(mode uint32) os.FileMode {
	return os.FileMode(mode) & os.ModeType
}

// IsRegular checks if the mode is regular.
func IsRegular(mode uint32) bool {
	return OsMode(mode).IsRegular()
}

// IsDir checks if the mode is dir.
func IsDir(mode uint32) bool {
	return OsMode(mode).IsDir()
}

// IsSymlink checks if the mode is symlink.
func IsSymlink(mode uint32) bool {
	return OsMode(mode)&os.ModeSymlink != 0
}

// InodeInfo defines the inode struct.
type InodeInfo struct {
	Inode      uint64    `json:"ino"`
	Mode       uint32    `json:"mode"`
	Nlink      uint32    `json:"nlink"`
	Size       uint64    `json:"sz"`
	Uid        uint32    `json:"uid"`
	Gid        uint32    `json:"gid"`
	Generation uint64    `json:"gen"`
	ModifyTime time.Time `json:"mt"`
	CreateTime time.Time `json:"ct"`
	AccessTime time.Time `json:"at"`
	Target     []byte    `json:"tgt"`

	expiration int64
}

func (info *InodeInfo) Expiration() int64 {
	return info.expiration
}

func (info *InodeInfo) SetExpiration(e int64) {
	info.expiration = e
}

// String returns the string format of the inode.
func (info *InodeInfo) String() string {
	if info == nil {
		return ""
	}
	return fmt.Sprintf("Inode(%v) Mode(%v) OsMode(%v) Nlink(%v) Size(%v) Uid(%v) Gid(%v) Gen(%v)", info.Inode, info.Mode, OsMode(info.Mode), info.Nlink, info.Size, info.Uid, info.Gid, info.Generation)
}

type XAttrInfo struct {
	Inode  uint64
	XAttrs map[string]string
}

func (info XAttrInfo) Get(key string) []byte {
	return []byte(info.XAttrs[key])
}

func (info XAttrInfo) VisitAll(visitor func(key string, value []byte) bool) {
	for k, v := range info.XAttrs {
		if visitor == nil || !visitor(k, []byte(v)) {
			return
		}
	}
}

func (info XAttrInfo) String() string {
	builder := strings.Builder{}
	for k, v := range info.XAttrs {
		if builder.Len() != 0 {
			builder.WriteString(",")
		}
		builder.WriteString(fmt.Sprintf("%s:%s", k, v))
	}
	return fmt.Sprintf("XAttrInfo{Inode(%v), XAttrs(%v)}", info.Inode, builder.String())
}

// Dentry defines the dentry struct.
type Dentry struct {
	Name  string `json:"name"`
	Inode uint64 `json:"ino"`
	Type  uint32 `json:"type"`
}

// String returns the string format of the dentry.
func (d Dentry) String() string {
	return fmt.Sprintf("Dentry{Name(%v),Inode(%v),Type(%v)}", d.Name, d.Inode, d.Type)
}

// CreateInodeRequest defines the request to create an inode.
type CreateInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Mode        uint32 `json:"mode"`
	Uid         uint32 `json:"uid"`
	Gid         uint32 `json:"gid"`
	Target      []byte `json:"tgt"`
}

// CreateInodeResponse defines the response to the request of creating an inode.
type CreateInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

// LinkInodeRequest defines the request to link an inode.
type LinkInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

// LinkInodeResponse defines the response to the request of linking an inode.
type LinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

// UnlinkInodeRequest defines the request to unlink an inode.
type UnlinkInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	TrashEnable bool   `json:"trash"`
}

// UnlinkInodeRequest defines the request to unlink an inode.
type BatchUnlinkInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	TrashEnable bool     `json:"trash"`
}

// UnlinkInodeResponse defines the response to the request of unlinking an inode.
type UnlinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

// batch UnlinkInodeResponse defines the response to the request of unlinking an inode.
type BatchUnlinkInodeResponse struct {
	Items []*struct {
		Info   *InodeInfo `json:"info"`
		Status uint8      `json:"status"`
	} `json:"items"`
}

// EvictInodeRequest defines the request to evict an inode.
type EvictInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	TrashEnable bool   `json:"trash"`
}

// EvictInodeRequest defines the request to evict some inode.
type BatchEvictInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	TrashEnable bool     `json:"trash"`
}

// CreateDentryRequest defines the request to create a dentry.
type CreateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Inode       uint64 `json:"ino"`
	Name        string `json:"name"`
	Mode        uint32 `json:"mode"`
}

// UpdateDentryRequest defines the request to update a dentry.
type UpdateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	Inode       uint64 `json:"ino"` // new inode number
}

// UpdateDentryResponse defines the response to the request of updating a dentry.
type UpdateDentryResponse struct {
	Inode uint64 `json:"ino"` // old inode number
}

// DeleteDentryRequest define the request tp delete a dentry.
type DeleteDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	TrashEnable bool   `json:"trash"`
}

type BatchDeleteDentryRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	ParentID    uint64   `json:"pino"`
	TrashEnable bool     `json:"trash"`
	Dens        []Dentry `json:"dens"`
}

// DeleteDentryResponse defines the response to the request of deleting a dentry.
type DeleteDentryResponse struct {
	Inode uint64 `json:"ino"`
}

// BatchDeleteDentryResponse defines the response to the request of deleting a dentry.
type BatchDeleteDentryResponse struct {
	Items []*struct {
		Inode  uint64 `json:"ino"`
		Status uint8  `json:"status"`
	} `json:"items"`
}

// LookupRequest defines the request for lookup.
type LookupRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
}

type LookupDeletedDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	StartTime   int64  `json:"start_time"`
	EndTime     int64  `json:"end_time"`
}

type LookupDeletedDentryResponse struct {
	Dentrys []*DeletedDentry `json:"dentrys"`
}

// LookupResponse defines the response for the loopup request.
type LookupResponse struct {
	Inode uint64 `json:"ino"`
	Mode  uint32 `json:"mode"`
}

// InodeGetRequest defines the request to get the inode.
type InodeGetRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

// InodeGetResponse defines the response to the InodeGetRequest.
type InodeGetResponse struct {
	Info *InodeInfo `json:"info"`
}

// BatchInodeGetRequest defines the request to get the inode in batch.
type BatchInodeGetRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}

// BatchInodeGetResponse defines the response to the request of getting the inode in batch.
type BatchInodeGetResponse struct {
	Infos []*InodeInfo `json:"infos"`
}

// ReadDirRequest defines the request to read dir.
type ReadDirRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Marker      string `json:"marker"` // the name of child from which the operation begins
	IsBatch     bool   `json:"is_batch"`
}

// ReadDirResponse defines the response to the request of reading dir.
type ReadDirResponse struct {
	Children   []Dentry `json:"children"`
	NextMarker string   `json:"next_marker"` // the name of child from which the next operation begins
}

// BatchAppendExtentKeyRequest defines the request to append an extent key.
type AppendExtentKeyRequest struct {
	VolName     string    `json:"vol"`
	PartitionID uint64    `json:"pid"`
	Inode       uint64    `json:"ino"`
	Extent      ExtentKey `json:"ek"`
	IsPreExtent bool      `json:"pre"`
}

type InsertExtentKeyRequest struct {
	VolName     string    `json:"vol"`
	PartitionID uint64    `json:"pid"`
	Inode       uint64    `json:"ino"`
	Extent      ExtentKey `json:"ek"`
	IsPreExtent bool      `json:"pre"`
}

// GetExtentsRequest defines the reques to get extents.
type GetExtentsRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

// GetExtentsResponse defines the response to the request of getting extents.
type GetExtentsResponse struct {
	Generation uint64      `json:"gen"`
	Size       uint64      `json:"sz"`
	Extents    []ExtentKey `json:"eks"`
}

const TruncateRequestVersion_1 = 1

// TruncateRequest defines the request to truncate.
type TruncateRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Size        uint64 `json:"sz"`
	OldSize     uint64 `json:"oldSize"`
	Version     uint64 `json:"version"`
}

// SetAttrRequest defines the request to set attribute.
type SetAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Mode        uint32 `json:"mode"`
	Uid         uint32 `json:"uid"`
	Gid         uint32 `json:"gid"`
	ModifyTime  int64  `json:"mt"`
	AccessTime  int64  `json:"at"`
	Valid       uint32 `json:"valid"`
}

const (
	AttrMode uint32 = 1 << iota
	AttrUid
	AttrGid
	AttrModifyTime
	AttrAccessTime
)

// DeleteInodeRequest defines the request to delete an inode.
type DeleteInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

// DeleteInodeRequest defines the request to delete an inode.
type CursorResetRequest struct {
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Cursor      uint64 `json:"cursor"`
	Force       bool   `json:"force"`
}

type CursorResetResponse struct {
	PartitionId uint64 `json:"pid"`
	Start       uint64 `json:"start"`
	End         uint64 `json:"end"`
	Cursor      uint64 `json:"cursor"`
}

type MpAllInodesId struct {
	Count  uint64   `json:"count"`
	Inodes []uint64 `json:"inodes"`
}

// DeleteInodeRequest defines the request to delete an inode.
type DeleteInodeBatchRequest struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"ino"`
}

// AppendExtentKeysRequest defines the request to append an extent key.
type AppendExtentKeysRequest struct {
	VolName     string      `json:"vol"`
	PartitionId uint64      `json:"pid"`
	Inode       uint64      `json:"ino"`
	Extents     []ExtentKey `json:"eks"`
	IsPreExtent bool        `json:"pre"`
}

type SetXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	Value       string `json:"val"`
}

type GetXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
}

type GetXAttrResponse struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	Value       string `json:"val"`
}

type RemoveXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
}

type ListXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type ListXAttrResponse struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"`
	Inode       uint64   `json:"ino"`
	XAttrs      []string `json:"xattrs"`
}

type BatchGetXAttrRequest struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	Keys        []string `json:"keys"`
}

type BatchGetXAttrResponse struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	XAttrs      []*XAttrInfo
}

type XAttrRaftResponse struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Status      uint8  `json:"status"`
}

type MultipartInfo struct {
	ID       string               `json:"id"`
	Path     string               `json:"path"`
	InitTime time.Time            `json:"itime"`
	Parts    []*MultipartPartInfo `json:"parts"`
	Extend   map[string]string    `json:"extend"`
}

type MultipartPartInfo struct {
	ID         uint16    `json:"id"`
	Inode      uint64    `json:"ino"`
	MD5        string    `json:"md5"`
	Size       uint64    `json:"sz"`
	UploadTime time.Time `json:"ut"`
}

type CreateMultipartRequest struct {
	VolName     string            `json:"vol"`
	PartitionId uint64            `json:"pid"`
	Path        string            `json:"path"`
	Extend      map[string]string `json:"extend"`
}

type CreateMultipartResponse struct {
	Info *MultipartInfo `json:"info"`
}

type GetMultipartRequest struct {
	VolName     string `json:"vol"`
	Path        string `json:"path"`
	PartitionId uint64 `json:"pid"`
	MultipartId string `json:"mid"`
}

type GetMultipartResponse struct {
	Info *MultipartInfo `json:"info"`
}

type AddMultipartPartRequest struct {
	VolName     string             `json:"vol"`
	PartitionId uint64             `json:"pid"`
	Path        string             `json:"path"`
	MultipartId string             `json:"mid"`
	Part        *MultipartPartInfo `json:"part"`
}

type RemoveMultipartRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Path        string `json:"path"`
	MultipartId string `json:"mid"`
}

type ListMultipartRequest struct {
	VolName           string `json:"vol"`
	PartitionId       uint64 `json:"pid"`
	Marker            string `json:"mk"`
	MultipartIdMarker string `json:"mmk"`
	Max               uint64 `json:"max"`
	Delimiter         string `json:"dm"`
	Prefix            string `json:"pf"`
}

type ListMultipartResponse struct {
	Multiparts []*MultipartInfo `json:"mps"`
}

type GetAppliedIDRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
}

type GetSnapshotCrcRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
}

type SnapshotCrdResponse struct {
	LastSnapshotStr string `json:"last_snapshot_str"`
	LocalAddr       string `json:"local_addr"`
}

type GetCmpInodesRequest struct {
	VolName      string   `json:"vol"`
	PartitionId  uint64   `json:"pid"`
	ParallelCnt  uint32   `json:"parallel_cnt"`
	Inodes       []uint64 `json:"inodes"`
	MinEkLen     int      `json:"min_ek_len"`
	MinInodeSize uint64   `json:"min_inode_size"`
	MaxEkAvgSize uint64   `json:"max_ek_avg_size"`
}

type CmpInodeInfo struct {
	Inode   *InodeInfo
	Extents []ExtentKey
}

type GetCmpInodesResponse struct {
	Inodes []*CmpInodeInfo
}

type InodeMergeExtentsRequest struct {
	VolName     string      `json:"vol"`
	PartitionId uint64      `json:"pid"`
	Inode       uint64      `json:"inode_id"`
	OldExtents  []ExtentKey `json:"old_extents"`
	NewExtents  []ExtentKey   `json:"new_extents"`
}

// Dentry defines the dentry struct.
type MetaDentry struct {
	ParentId uint64 // FileID value of the parent inode.
	Name     string // Name of the current dentry.
	Inode    uint64 // FileID value of the current inode.
	Type     uint32
}

// String returns the string format of the dentry.
func (d MetaDentry) String() string {
	return fmt.Sprintf("Dentry{Pid(%d),Name(%v),Inode(%v),Type(%v)}", d.ParentId, d.Name, d.Inode, d.Type)
}

type MetaInode struct {
	Inode      uint64 // Inode ID
	Type       uint32
	Uid        uint32
	Gid        uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte // SymLink target name
	NLink      uint32 // NodeLink counts
	Flag       int32
	Reserved   uint64 // reserved space
}

func (ino MetaInode) String() string {
	return fmt.Sprintf("Inode{Ino(%d),Type(%v),Size(%v),NLikn(%v)}", ino.Inode, ino.Type, ino.Size, ino.NLink)
}

type RecoverDeletedDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	TimeStamp   int64  `json:"time"`
	Inode       uint64 `json:"ino"`
}

type RecoverDeletedDentryResponse struct {
	Inode uint64 `json:"ino"`
	Name  string `json:"name"`
}

type BatchRecoverDeletedDentryRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Dens        []*DeletedDentry `json:"dens"`
}

type BatchRecoverDeletedDentryResponse struct {
	Items map[uint64]uint8 `json:"items"` // key is inode, value is status
}

type RecoverDeletedInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type BatchRecoverDeletedInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}

type BatchRecoverDeletedInodeResponse struct {
	Res map[uint64]uint8 `json:"res"`
}

type CleanDeletedDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	Timestamp   int64  `json:"time"`
	Inode       uint64 `json:"ino"`
}

type BatchCleanDeletedDentryRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Dens        []*DeletedDentry `json:"dens"`
}

type CleanDeletedInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type BatchCleanDeletedInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}

type BatchCleanDeletedInodeResponse struct {
	Res map[uint64]uint8 `json:"res"`
}

const (
	ReadDeletedDirBatchNum = 1000
)

type ReadDeletedDirRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	Timestamp   int64  `json:"ts"`
}

type DeletedDentry struct {
	ParentID  uint64 `json:"pid"`
	Name      string `json:"name"`
	Inode     uint64 `json:"inode"`
	Type      uint32 `json:"type"`
	Timestamp int64  `json:"ts"`
	From      string `json:"from"`
}

// String returns the string format of the dentry.
func (d *DeletedDentry) String() string {
	return fmt.Sprintf("Dentry{PID(%v) Name(%v),Inode(%v),Type(%v), DeleteTime(%v)}",
		d.ParentID, d.Name, d.Inode, d.Type, d.Timestamp)
}

func (d *DeletedDentry) AppendTimestampToName() string {
	timeStr := time.Unix(d.Timestamp/1000000, d.Timestamp%1000000*1000).Format("20060102150405.000000")
	return fmt.Sprintf("%v_%v", d.Name, timeStr)
}

type ReadDeletedDirResponse struct {
	Children []*DeletedDentry `json:"children"`
}

type CleanExpiredInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Expires     uint64 `json:"expires"`
}

type CleanExpiredDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Expires     uint64 `json:"expires"`
}

type StatDeletedFileInfoRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
}

type StatDeletedFileInfoResponse struct {
	StatInfo map[string]*DeletedFileInfo
}

type DeletedFileInfo struct {
	InodeSum  uint64 `json:"inosum"`
	DentrySum uint64 `json:"densum"`
	Size      uint64 `json:"size"`
}

type DeletedInodeInfo struct {
	Inode      uint64    `json:"ino"`
	Mode       uint32    `json:"mode"`
	Nlink      uint32    `json:"nlink"`
	Size       uint64    `json:"sz"`
	Uid        uint32    `json:"uid"`
	Gid        uint32    `json:"gid"`
	Generation uint64    `json:"gen"`
	ModifyTime time.Time `json:"mt"`
	CreateTime time.Time `json:"ct"`
	AccessTime time.Time `json:"at"`
	Target     []byte    `json:"tgt"`
	expiration int64
	DeleteTime int64 `json:"dt"`
	IsDeleted  bool  `json:"isDel"`
}

func (info *DeletedInodeInfo) String() string {
	return fmt.Sprintf("Inode(%v) Mode(%v) OsMode(%v) Nlink(%v) Size(%v) Uid(%v) Gid(%v) Gen(%v), Time(%v), IsDeleted(%v)",
		info.Inode, info.Mode, OsMode(info.Mode), info.Nlink, info.Size, info.Uid, info.Gid, info.Generation, info.DeleteTime, info.IsDeleted)
}

type GetDeletedInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type GetDeletedInodeResponse struct {
	Info *DeletedInodeInfo `json:"info"`
}

type BatchGetDeletedInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}
type BatchGetDeletedInodeResponse struct {
	Infos []*DeletedInodeInfo `json:"infos"`
}

type OpDeletedDentryRsp struct {
	Status uint8          `json:"st"`
	Den    *DeletedDentry `json:"den"`
}

type BatchOpDeletedDentryRsp struct {
	Dens []*OpDeletedDentryRsp
}

type OpDeletedINodeRsp struct {
	Status uint8             `json:"st"`
	Inode  *DeletedInodeInfo `json:"ino"`
}

type BatchOpDeletedINodeRsp struct {
	Inos []*OpDeletedINodeRsp `json:"inos"`
}
