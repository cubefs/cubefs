// Copyright 2018 The Containerfs Authors.
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

func Mode(osMode os.FileMode) uint32 {
	return uint32(osMode)
}

func OsMode(mode uint32) os.FileMode {
	return os.FileMode(mode)
}

func IsRegular(mode uint32) bool {
	return OsMode(mode).IsRegular()
}

func IsDir(mode uint32) bool {
	return OsMode(mode).IsDir()
}

func IsSymlink(mode uint32) bool {
	return OsMode(mode)&os.ModeSymlink != 0
}

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

func (info *InodeInfo) String() string {
	return fmt.Sprintf("Inode(%v) Mode(%v) OsMode(%v) Nlink(%v) Size(%v) Uid(%v) Gid(%v) Gen(%v)", info.Inode, info.Mode, OsMode(info.Mode), info.Nlink, info.Size, info.Uid, info.Gid, info.Generation)
}

type Dentry struct {
	Name  string `json:"name"`
	Inode uint64 `json:"ino"`
	Type  uint32 `json:"type"`
}

func (d Dentry) String() string {
	return fmt.Sprintf("Dentry{Name(%v),Inode(%v),Type(%v)}", d.Name, d.Inode, d.Type)
}

type CreateInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Mode        uint32 `json:"mode"`
	Target      []byte `json:"tgt"`
}

type CreateInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type LinkInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type LinkInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

//FIXME: unlink inode
type DeleteInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type DeleteInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type EvictInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type CreateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Inode       uint64 `json:"ino"`
	Name        string `json:"name"`
	Mode        uint32 `json:"mode"`
}

type UpdateDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
	Inode       uint64 `json:"ino"` // new inode number
}

type UpdateDentryResponse struct {
	Inode uint64 `json:"ino"` // old inode number
}

type DeleteDentryRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
}

type DeleteDentryResponse struct {
	Inode uint64 `json:"ino"`
}

type OpenRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type LookupRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
	Name        string `json:"name"`
}

type LookupResponse struct {
	Inode uint64 `json:"ino"`
	Mode  uint32 `json:"mode"`
}

type InodeGetRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type InodeGetResponse struct {
	Info *InodeInfo `json:"info"`
}

type BatchInodeGetRequest struct {
	VolName     string   `json:"vol"`
	PartitionID uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}

type BatchInodeGetResponse struct {
	Infos []*InodeInfo `json:"infos"`
}

type ReadDirRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	ParentID    uint64 `json:"pino"`
}

type ReadDirResponse struct {
	Children []Dentry `json:"children"`
}

type AppendExtentKeyRequest struct {
	VolName     string    `json:"vol"`
	PartitionID uint64    `json:"pid"`
	Inode       uint64    `json:"ino"`
	Extent      ExtentKey `json:"ek"`
}

type GetExtentsRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type GetExtentsResponse struct {
	Generation uint64      `json:"gen"`
	Size       uint64      `json:"sz"`
	Extents    []ExtentKey `json:"eks"`
}

type TruncateRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	Size        uint64 `json:"sz"`
}

type SetattrRequest struct {
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
