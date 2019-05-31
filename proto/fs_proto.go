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
}

// String returns the string format of the inode.
func (info *InodeInfo) String() string {
	return fmt.Sprintf("Inode(%v) Mode(%v) OsMode(%v) Nlink(%v) Size(%v) Uid(%v) Gid(%v) Gen(%v)", info.Inode, info.Mode, OsMode(info.Mode), info.Nlink, info.Size, info.Uid, info.Gid, info.Generation)
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
}

// UnlinkInodeResponse defines the response to the request of unlinking an inode.
type UnlinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

// EvictInodeRequest defines the request to evict an inode.
type EvictInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
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
}

// DeleteDentryResponse defines the response to the request of deleting a dentry.
type DeleteDentryResponse struct {
	Inode uint64 `json:"ino"`
}

// LookupRequest defines the request for lookup.
type LookupRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
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
}

// ReadDirResponse defines the response to the request of reading dir.
type ReadDirResponse struct {
	Children []Dentry `json:"children"`
}

// AppendExtentKeyRequest defines the request to append an extent key.
type AppendExtentKeyRequest struct {
	VolName     string    `json:"vol"`
	PartitionID uint64    `json:"pid"`
	Inode       uint64    `json:"ino"`
	Extent      ExtentKey `json:"ek"`
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

// TruncateRequest defines the request to truncate.
type TruncateRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Size        uint64 `json:"sz"`
}

// SetAttrRequest defines the request to set attribute.
type SetAttrRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Mode        uint32 `json:"mode"`
	Uid         uint32 `json:"uid"`
	Gid         uint32 `json:"gid"`
	Valid       uint32 `json:"valid"`
}

const (
	AttrMode uint32 = 1 << iota
	AttrUid
	AttrGid
)
