package proto

import (
	"fmt"
	"time"
)

const (
	RootIno = uint64(1)
)

const (
	ModeRegular uint32 = iota
	ModeDir
)

type InodeInfo struct {
	Inode      uint64    `json:"ino"`
	Mode       uint32    `json:"mode"`
	Size       uint64    `json:"sz"`
	Generation uint64    `json:"gen"`
	ModifyTime time.Time `json:"mt"`
	CreateTime time.Time `json:"ct"`
	AccessTime time.Time `json:"at"`
}

func (info *InodeInfo) String() string {
	return fmt.Sprintf("Inode(%v) Mode(%v) Size(%v) Gen(%v)", info.Inode, info.Mode, info.Size, info.Generation)
}

type Dentry struct {
	Name  string `json:"name"`
	Inode uint64 `json:"ino"`
	Type  uint32 `json:"type"`
}

type CreateInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Mode        uint32 `json:"mode"`
}

type CreateInodeResponse struct {
	Info *InodeInfo `json:"info"`
}

type DeleteInodeRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
}

type DeleteInodeResponse struct {
	Extents []ExtentKey `json:"ek"`
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
	Extents []ExtentKey `json:"eks"`
}

type TruncateRequest struct {
	VolName     string `json:"vol"`
	PartitionID uint64 `json:"pid"`
	Inode       uint64 `json:"ino"`
	FileOffset  uint64 `json:"fof"` // always 0 for now
	Size        uint64 `json:"sz"`  // always 0 for now
}

type TruncateResponse struct {
	Extents []ExtentKey `json:"ek"`
}
