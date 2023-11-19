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
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	RootIno    = uint64(1)
	SummaryKey = "cbfs.dir.summary"
	QuotaKey   = "qa"
)

const (
	FlagsSyncWrite int = 1 << iota
	FlagsAppend
	FlagsCache
)

const (
	FlagsSnapshotDel int = 1 << iota
	FlagsSnapshotDelDir
	FlagsVerAll
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

func IsAncestor(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return !strings.HasPrefix(rel, "..")
}

// InodeInfo defines the inode struct.
type InodeInfo struct {
	Inode        uint64                    `json:"ino"`
	Mode         uint32                    `json:"mode"`
	Nlink        uint32                    `json:"nlink"`
	Size         uint64                    `json:"sz"`
	Uid          uint32                    `json:"uid"`
	Gid          uint32                    `json:"gid"`
	Generation   uint64                    `json:"gen"`
	ModifyTime   time.Time                 `json:"mt"`
	CreateTime   time.Time                 `json:"ct"`
	AccessTime   time.Time                 `json:"at"`
	Target       []byte                    `json:"tgt"`
	QuotaInfos   map[uint32]*MetaQuotaInfo `json:"qifs"`
	VerSeq       uint64                    `json:"seq"`
	expiration   int64
	StorageClass uint32 `json:"storageClass"`
}

type SimpleExtInfo struct {
	ID          uint64
	PartitionID uint32
	ExtentID    uint32
}

// InodeInfo defines the inode struct.
type InodeSplitInfo struct {
	Inode    uint64          `json:"ino"`
	SplitArr []SimpleExtInfo `json:"splitInfo"`
	VerSeq   uint64          `json:"seq"`
}

type SummaryInfo struct {
	Files   int64 `json:"files"`
	Subdirs int64 `json:"subdirs"`
	Fbytes  int64 `json:"fbytes"`
}

type DentryInfo struct {
	Name       string `json:"name"`
	Inode      uint64 `json:"inode"`
	expiration int64
}

func (info *DentryInfo) SetExpiration(e int64) {
	info.expiration = e
}

func (info *DentryInfo) Expiration() int64 {
	return info.expiration
}

func (info *InodeInfo) Expiration() int64 {
	return info.expiration
}

func (info *InodeInfo) SetExpiration(e int64) {
	info.expiration = e
}

// String returns the string format of the inode.
func (info *InodeInfo) String() string {
	return fmt.Sprintf("Inode(%v) Mode(%v) OsMode(%v) Nlink(%v) Size(%v) Uid(%v) Gid(%v) Gen(%v) QuotaIds(%v) StorageClass(%v)",
		info.Inode, info.Mode, OsMode(info.Mode), info.Nlink, info.Size, info.Uid, info.Gid, info.Generation, info.QuotaInfos, info.StorageClass)
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

type RequestExtend struct {
	FullPaths []string `json:"fullPaths"`
}

// NOTE: batch request may have multi full path
// values, but other request only have one
func (r *RequestExtend) GetFullPath() string {
	if len(r.FullPaths) < 1 {
		return ""
	}
	return r.FullPaths[0]
}

// CreateInodeRequest defines the request to create an inode.
type QuotaCreateInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Mode        uint32   `json:"mode"`
	Uid         uint32   `json:"uid"`
	Gid         uint32   `json:"gid"`
	Target      []byte   `json:"tgt"`
	QuotaIds    []uint32 `json:"qids"`
	RequestExtend
	StorageType uint32 `json:"storageType"`
}

type CreateInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Mode        uint32 `json:"mode"`
	Uid         uint32 `json:"uid"`
	Gid         uint32 `json:"gid"`
	Target      []byte `json:"tgt"`
	RequestExtend
	StorageType uint32 `json:"storageType"`
}

// CreateInodeResponse defines the response to the request of creating an inode.
type CreateInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type TxCreateRequest struct {
	VolName          string `json:"vol"`
	PartitionID      uint64 `json:"pid"`
	*TransactionInfo `json:"tx"`
}

type TxCreateResponse struct {
	TxInfo *TransactionInfo `json:"tx"`
}

type TxApplyRMRequest struct {
	VolName          string `json:"vol"`
	PartitionID      uint64 `json:"pid"`
	*TransactionInfo `json:"tx"`
}

// TxCreateInodeRequest defines the request to create an inode with transaction info.
type TxCreateInodeRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Mode        uint32           `json:"mode"`
	Uid         uint32           `json:"uid"`
	Gid         uint32           `json:"gid"`
	Target      []byte           `json:"tgt"`
	QuotaIds    []uint32         `json:"qids"`
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
	StorageType uint32 `json:"storageType"`
}

// TxCreateInodeResponse defines the response with transaction info to the request of creating an inode.
type TxCreateInodeResponse struct {
	Info   *InodeInfo       `json:"info"`
	TxInfo *TransactionInfo `json:"tx"`
}

const (
	TxCommit int = 1 << iota
	TxRollback
)

type TxApplyRequest struct {
	TxID        string `json:"tx"`
	TmID        uint64 `json:"tmid"`
	TxApplyType int    `json:"type"`
}

type TxSetStateRequest struct {
	TxID  string `json:"tx"`
	State int32  `json:"state"`
}

type TxInodeApplyRequest struct {
	TxID        string `json:"txid"`
	Inode       uint64 `json:"ino"`
	TxApplyType int    `json:"type"`
	ApplyFrom   uint32 `json:"from"`
}

type TxDentryApplyRequest struct {
	TxID string `json:"txid"`
	//DenKey      string `json:"denkey"`
	Pid         uint64 `json:"pid"`
	Name        string `json:"name"`
	TxApplyType int    `json:"type"`
	ApplyFrom   uint32 `json:"from"`
}

type TxGetInfoRequest struct {
	VolName string `json:"vol"`
	TxID    string `json:"txid"`
	Pid     uint64 `json:"pid"`
}

type TxGetInfoResponse struct {
	TxInfo *TransactionInfo `json:"tx"`
}

// LinkInodeRequest defines the request to link an inode.
type LinkInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	UniqID      uint64 `json:"uiq"`
	IsRename    bool   `json:"rename"`
	RequestExtend
}

// LinkInodeResponse defines the response to the request of linking an inode.
type LinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type TxLinkInodeRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Inode       uint64           `json:"ino"`
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
}

func (tx *TxLinkInodeRequest) GetInfo() string {
	return tx.TxInfo.String()
}

type TxLinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type ClearInodeCacheRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type ClearInodeCacheResponse struct {
	Info *InodeInfo `json:"info"`
}

type TxUnlinkInodeRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Inode       uint64           `json:"ino"`
	Evict       bool             `json:"evict"`
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
}

func (tx *TxUnlinkInodeRequest) GetInfo() string {
	return tx.TxInfo.String()
}

type TxUnlinkInodeResponse struct {
	Info   *InodeInfo       `json:"info"`
	TxInfo *TransactionInfo `json:"tx"`
}

// UnlinkInodeRequest defines the request to unlink an inode.
type UnlinkInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	UniqID      uint64 `json:"uid"` //for request dedup
	VerSeq      uint64 `json:"ver"`
	DenVerSeq   uint64 `json:"denVer"`
	RequestExtend
}

// UnlinkInodeRequest defines the request to unlink an inode.
type BatchUnlinkInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	FullPaths   []string `json:"fullPaths"`
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
	RequestExtend
}

// EvictInodeRequest defines the request to evict some inode.
type BatchEvictInodeRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	FullPaths   []string `json:"fullPaths"`
}

// CreateDentryRequest defines the request to create a dentry.
type QuotaCreateDentryRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	ParentID    uint64   `json:"pino"`
	Inode       uint64   `json:"ino"`
	Name        string   `json:"name"`
	Mode        uint32   `json:"mode"`
	QuotaIds    []uint32 `json:"qids"`
	VerSeq      uint64   `json:"seq"`
	RequestExtend
}

type CreateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Inode       uint64 `json:"ino"`
	Name        string `json:"name"`
	Mode        uint32 `json:"mode"`
	RequestExtend
}

type TxPack interface {
	GetInfo() string
}

// TxCreateDentryRequest defines the request to create a dentry.
type TxCreateDentryRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	ParentID    uint64           `json:"pino"`
	Inode       uint64           `json:"ino"`
	Name        string           `json:"name"`
	Mode        uint32           `json:"mode"`
	QuotaIds    []uint32         `json:"qids"`
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
}

func (tx *TxCreateDentryRequest) GetInfo() string {
	return tx.TxInfo.String()
}

type TxCreateDentryResponse struct {
	TxInfo *TransactionInfo `json:"tx"`
}

// UpdateDentryRequest defines the request to update a dentry.
type UpdateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	Inode       uint64 `json:"ino"` // new inode number
	RequestExtend
}

// UpdateDentryResponse defines the response to the request of updating a dentry.
type UpdateDentryResponse struct {
	Inode uint64 `json:"ino"` // old inode number
}

type TxUpdateDentryRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	ParentID    uint64           `json:"pino"`
	Name        string           `json:"name"`
	Inode       uint64           `json:"ino"`    // new inode number
	OldIno      uint64           `json:"oldIno"` // new inode number
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
}

func (tx *TxUpdateDentryRequest) GetInfo() string {
	return tx.TxInfo.String()
}

type TxUpdateDentryResponse struct {
	Inode uint64 `json:"ino"` // old inode number
}

type TxDeleteDentryRequest struct {
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	ParentID    uint64           `json:"pino"`
	Name        string           `json:"name"`
	Ino         uint64           `json:"ino"`
	TxInfo      *TransactionInfo `json:"tx"`
	RequestExtend
}

func (tx *TxDeleteDentryRequest) GetInfo() string {
	return tx.TxInfo.String()
}

type TxDeleteDentryResponse struct {
	Inode uint64 `json:"ino"`
}

// DeleteDentryRequest define the request tp delete a dentry.
type DeleteDentryRequest struct {
	VolName         string `json:"vol"`
	PartitionID     uint64 `json:"pid"`
	ParentID        uint64 `json:"pino"`
	Name            string `json:"name"`
	InodeCreateTime int64  `json:"inodeCreateTime"`
	Verseq          uint64 `json:"ver"`
	RequestExtend
}

type BatchDeleteDentryRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	ParentID    uint64   `json:"pino"`
	Dens        []Dentry `json:"dens"`
	FullPaths   []string `json:"fullPaths"`
}

// DeleteDentryResponse defines the response to the request of deleting a dentry.
type DeleteDentryResponse struct {
	Inode uint64 `json:"ino"`
}

// BatchDeleteDentryResponse defines the response to the request of deleting a dentry.
type BatchDeleteDentryResponse struct {
	ParentID uint64 `json:"pino"`
	Items    []*struct {
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
	VerSeq      uint64 `json:"seq"`
	VerAll      bool   `json:"verAll"`
}
type DetryInfo struct {
	Inode  uint64 `json:"ino"`
	Mode   uint32 `json:"mode"`
	VerSeq uint64 `json:"seq"`
	IsDel  bool   `json:"isDel"`
}

// LookupResponse defines the response for the loopup request.
type LookupResponse struct {
	Inode  uint64      `json:"ino"`
	Mode   uint32      `json:"mode"`
	VerSeq uint64      `json:"seq"`
	LayAll []DetryInfo `json:"layerInfo"`
}

// InodeGetRequest defines the request to get the inode.
type InodeGetRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	VerSeq      uint64 `json:"seq"`
	VerAll      bool   `json:"verAll"`
}

type LayerInfo struct {
	LayerIdx uint32      `json:"layerIdx"`
	Info     *InodeInfo  `json:"info"`
	Eks      []ExtentKey `json:"eks"`
}

// InodeGetResponse defines the response to the InodeGetRequest.
type InodeGetResponse struct {
	Info   *InodeInfo  `json:"info"`
	LayAll []InodeInfo `json:"layerInfo"`
}

// BatchInodeGetRequest defines the request to get the inode in batch.
type BatchInodeGetRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
	VerSeq      uint64   `json:"seq"`
}

// BatchInodeGetResponse defines the response to the request of getting the inode in batch.
type BatchInodeGetResponse struct {
	Infos []*InodeInfo `json:"infos"`
}

// InodeGetRequest defines the request to get the inode.
type InodeGetSplitRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	VerSeq      uint64 `json:"seq"`
	VerAll      bool   `json:"verAll"`
}

// InodeGetResponse defines the response to the InodeGetRequest.
type InodeGetSplitResponse struct {
	Info   *InodeSplitInfo  `json:"info"`
	LayAll []InodeSplitInfo `json:"layerInfo"`
}

// ReadDirRequest defines the request to read dir.
type ReadDirRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	VerSeq      uint64 `json:"seq"`
}

type ReadDirOnlyRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	VerSeq      uint64 `json:"seq"`
}

// ReadDirResponse defines the response to the request of reading dir.
type ReadDirResponse struct {
	Children []Dentry `json:"children"`
}
type ReadDirOnlyResponse struct {
	Children []Dentry `json:"children"`
}

// ReadDirLimitRequest defines the request to read dir with limited dentries.
type ReadDirLimitRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Marker      string `json:"marker"`
	Limit       uint64 `json:"limit"`
	VerSeq      uint64 `json:"seq"`
	VerOpt      uint8  `json:"VerOpt"`
}

type ReadDirLimitResponse struct {
	Children []Dentry `json:"children"`
}

// AppendExtentKeyRequest defines the request to append an extent key.
type AppendExtentKeyRequest struct {
	VolName     string    `json:"vol"`
	PartitionID uint64    `json:"pid"`
	Inode       uint64    `json:"ino"`
	Extent      ExtentKey `json:"ek"`
}

type AppendExtentKeyWithCheckRequest struct {
	VolName        string      `json:"vol"`
	PartitionID    uint64      `json:"pid"`
	Inode          uint64      `json:"ino"`
	Extent         ExtentKey   `json:"ek"`
	DiscardExtents []ExtentKey `json:"dek"`
	VerSeq         uint64      `json:"seq"`
	IsSplit        bool
	IsCache        bool
	StorageClass   uint32 `json:"storageClass"`
}

// AppendObjExtentKeyRequest defines the request to append an obj extent key.
type AppendObjExtentKeysRequest struct {
	VolName     string         `json:"vol"`
	PartitionID uint64         `json:"pid"`
	Inode       uint64         `json:"ino"`
	Extents     []ObjExtentKey `json:"ek"`
}

// GetExtentsRequest defines the reques to get extents.
type GetExtentsRequest struct {
	VolName      string `json:"vol"`
	PartitionID  uint64 `json:"pid"`
	Inode        uint64 `json:"ino"`
	VerSeq       uint64 `json:"seq"`
	VerAll       bool
	IsCache      bool
	OpenForWrite bool
}

// GetObjExtentsResponse defines the response to the request of getting obj extents.
type GetObjExtentsResponse struct {
	Generation uint64         `json:"gen"`
	Size       uint64         `json:"sz"`
	Extents    []ExtentKey    `json:"eks"`
	ObjExtents []ObjExtentKey `json:"objeks"`
}

// GetExtentsResponse defines the response to the request of getting extents.
type GetExtentsResponse struct {
	Generation      uint64      `json:"gen"`
	Size            uint64      `json:"sz"`
	Extents         []ExtentKey `json:"eks"`
	LayerInfo       []LayerInfo `json:"layer"`
	Status          int
	WriteGeneration uint64 `json:"writeGeneration"`
}

// TruncateRequest defines the request to truncate.
type TruncateRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Size        uint64 `json:"sz"`
	RequestExtend
}

type EmptyExtentKeyRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type DelVerRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	VerSeq      uint64 `json:"ver"`
}

type DelExtentKeyRequest struct {
	VolName     string      `json:"vol"`
	PartitionID uint64      `json:"pid"`
	Inode       uint64      `json:"ino"`
	Extents     []ExtentKey `json:"ek"`
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
	VerSeq      uint64 `json:"seq"`
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
	RequestExtend
}

// DeleteInodeRequest defines the request to delete an inode.
type DeleteInodeBatchRequest struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"ino"`
	FullPaths   []string `json:"fullPaths"`
}

// AppendExtentKeysRequest defines the request to append an extent key.
type AppendExtentKeysRequest struct {
	VolName      string      `json:"vol"`
	PartitionId  uint64      `json:"pid"`
	Inode        uint64      `json:"ino"`
	Extents      []ExtentKey `json:"eks"`
	StorageClass uint32      `json:"storageClass"`
}

type SetXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	Value       string `json:"val"`
}

type BatchSetXAttrRequest struct {
	VolName     string            `json:"vol"`
	PartitionId uint64            `json:"pid"`
	Inode       uint64            `json:"ino"`
	Attrs       map[string]string `json:"attrs"`
}

type GetAllXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	VerSeq      uint64 `json:"seq"`
}

type GetAllXAttrResponse struct {
	VolName     string            `json:"vol"`
	PartitionId uint64            `json:"pid"`
	Inode       uint64            `json:"ino"`
	Attrs       map[string]string `json:"attrs"`
}

type GetXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	VerSeq      uint64 `json:"seq"`
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
	VerSeq      uint64 `json:"seq"`
}

type ListXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	VerSeq      uint64 `json:"seq"`
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
	VerSeq      uint64   `json:"seq"`
}

type BatchGetXAttrResponse struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	XAttrs      []*XAttrInfo
}

type UpdateXAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	Value       string `json:"val"`
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

type GetExpiredMultipartRequest struct {
	VolName     string `json:"vol"`
	Prefix      string `json:"path"`
	Days        int    `json:"days"`
	PartitionId uint64 `json:"pid"`
}

type ExpiredMultipartInfo struct {
	Path        string   `json:"path"`
	MultipartId string   `json:"mid"`
	Inodes      []uint64 `json:"inodes"`
}

type GetExpiredMultipartResponse struct {
	Infos []*ExpiredMultipartInfo `json:"infos"`
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

type UpdateSummaryInfoRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Key         string `json:"key"`
	FileInc     int64  `json:"fileinc"`
	DirInc      int64  `json:"dirinc"`
	ByteInc     int64  `json:"byteinc"`
}

type SetMasterQuotaReuqest struct {
	VolName   string          `json:"vol"`
	PathInfos []QuotaPathInfo `json:"pinfos"`
	MaxFiles  uint64          `json:"mf"`
	MaxBytes  uint64          `json:"mbyte"`
}

type UpdateMasterQuotaReuqest struct {
	VolName  string `json:"vol"`
	QuotaId  uint32 `json:"qid"`
	MaxFiles uint64 `json:"mf"`
	MaxBytes uint64 `json:"mbyte"`
}

type ListMasterQuotaResponse struct {
	Quotas []*QuotaInfo
}

type BatchSetMetaserverQuotaReuqest struct {
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"ino"`
	QuotaId     uint32   `json:"qid"`
	IsRoot      bool     `json:"root"`
}

type BatchSetMetaserverQuotaResponse struct {
	InodeRes map[uint64]uint8 `json:"inores"`
}

type BatchDeleteMetaserverQuotaReuqest struct {
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"ino"`
	QuotaId     uint32   `json:"qid"`
}

type BatchDeleteMetaserverQuotaResponse struct {
	InodeRes map[uint64]uint8 `json:"inores"`
}

type GetInodeQuotaRequest struct {
	PartitionId uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type GetInodeQuotaResponse struct {
	MetaQuotaInfoMap map[uint32]*MetaQuotaInfo
}

type AppendMultipartResponse struct {
	Status   uint8  `json:"status"`
	Update   bool   `json:"update"`
	OldInode uint64 `json:"oldinode"`
}

type GetUniqIDRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Num         uint32 `json:"num"`
}

type GetUniqIDResponse struct {
	Start uint64 `json:"start"`
}

type RenewalForbiddenMigrationRequest struct {
	VolName      string `json:"vol"`
	PartitionID  uint64 `json:"pid"`
	Inode        uint64 `json:"ino"`
	StorageClass uint32 `json:"storageClass"`
}
