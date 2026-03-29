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
	"sync"

	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
)

const (
	DeleteMarkFlag               = 1 << 0
	InodeDelTop                  = 1 << 1
	DeleteMigrationExtentKeyFlag = 1 << 2 // only delete migration ek by delay
)

// CreateNameSpaceRequest defines the request to create a name space.
type CreateNameSpaceRequest struct {
	Name string
}

// CreateNameSpaceResponse defines the response to the request of creating a name space.
type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type RecoverPair struct {
	RecoverSrc       string
	RecoverDst       string
	RecoverStart     int64
	RecoverRetryCnt  int
	RecoverRetryTime int64
	RecoverState     RecoverState // Learner recovery state: 0=Init, 1=Recovering, 2=Failed
	DecommissionType uint32
	IsRecover        atomicutil.Bool
}

func (rp *RecoverPair) IsEmpty() bool {
	return rp.RecoverDst == ""
}

func (rp *RecoverPair) String() string {
	return fmt.Sprintf("RecoverSrc[%v], RecoverDst[%v], RecoverStart[%v], RecoverRetryCnt[%v], RecoverRetryTime[%v], RecoverState[%v], DecommissionType[%v], IsRecover[%v]",
		rp.RecoverSrc, rp.RecoverDst, rp.RecoverStart, rp.RecoverRetryCnt, rp.RecoverRetryTime, rp.RecoverState, rp.DecommissionType, rp.IsRecover.Load())
}

// Peer defines the peer of the node id and address.
type Peer struct {
	Type          raftproto.PeerType `json:"type"`
	ID            uint64             `json:"id"`
	Addr          string             `json:"addr"`
	HeartbeatPort string             `json:"raftHeartbeat"`
	ReplicaPort   string             `json:"raftReplica"`
	ManualPromote bool               `json:"manualPromote"` // if true, can't be promoted or deleted automatically
	Tag           string             `json:"tag"`           // tag of the peer
}

func (p *Peer) String() string {
	return fmt.Sprintf("type[%v],id[%v],addr[%v],heartbeatPort[%v],replicaPort[%v],manualPromote[%v]",
		p.Type, p.ID, p.Addr, p.HeartbeatPort, p.ReplicaPort, p.ManualPromote)
}

// CreateMetaPartitionRequest defines the request to create a meta partition.
type CreateMetaPartitionRequest struct {
	MetaId      string
	VolName     string
	Start       uint64
	End         uint64
	PartitionID uint64
	Members     []Peer
	VerSeq      uint64
	StoreMode   StoreMode
}

// CreateMetaPartitionResponse defines the response to the request of creating a meta partition.
type CreateMetaPartitionResponse struct {
	VolName     string
	PartitionID uint64
	Status      uint8
	Result      string
}

type UidSpaceInfo struct {
	VolName   string
	Uid       uint32
	CTime     int64
	Enabled   bool
	Limited   bool
	UsedSize  uint64
	LimitSize uint64
	Rsv       string
}

type UidReportSpaceInfo struct {
	Uid   uint32
	Size  uint64
	Rsv   string
	MTime int64
}

type QuotaUsedInfo struct {
	UsedFiles int64
	UsedBytes int64
}

type QuotaLimitedInfo struct {
	LimitedFiles bool
	LimitedBytes bool
}

type QuotaReportInfo struct {
	QuotaId  uint32
	UsedInfo QuotaUsedInfo
}

type QuotaInfo struct {
	VolName     string
	QuotaId     uint32
	CTime       int64
	PathInfos   []QuotaPathInfo
	LimitedInfo QuotaLimitedInfo
	UsedInfo    QuotaUsedInfo
	MaxFiles    uint64
	MaxBytes    uint64
	Rsv         string
}

type QuotaHeartBeatInfo struct {
	VolName     string
	QuotaId     uint32
	LimitedInfo QuotaLimitedInfo
	Enable      bool
}

type MetaQuotaInfos struct {
	QuotaInfoMap map[uint32]*MetaQuotaInfo
	sync.RWMutex
}

type MetaQuotaInfo struct {
	RootInode bool `json:"rid"`
}

type QuotaPathInfo struct {
	FullPath    string
	RootInode   uint64
	PartitionId uint64
}

func (usedInfo *QuotaUsedInfo) Add(info *QuotaUsedInfo) {
	usedInfo.UsedFiles += info.UsedFiles
	usedInfo.UsedBytes += info.UsedBytes
}

func (quotaInfo *QuotaInfo) IsOverQuotaFiles() (isOver bool) {
	if uint64(quotaInfo.UsedInfo.UsedFiles) > quotaInfo.MaxFiles {
		isOver = true
	} else {
		isOver = false
	}
	return
}

func (quotaInfo *QuotaInfo) IsOverQuotaBytes() (isOver bool) {
	if uint64(quotaInfo.UsedInfo.UsedBytes) > quotaInfo.MaxBytes {
		isOver = true
	} else {
		isOver = false
	}
	return
}

type StatOfStorageClass struct {
	StorageClass  uint32
	InodeCount    uint64
	UsedSizeBytes uint64
	QuotaGB       uint64
	PoolId        uint8
}

func NewStatOfStorageClass(storageClass uint32) *StatOfStorageClass {
	return &StatOfStorageClass{
		StorageClass:  storageClass,
		InodeCount:    0,
		UsedSizeBytes: 0,
	}
}

func NewStatOfStorageClassByPool(poolId uint8) *StatOfStorageClass {
	return &StatOfStorageClass{
		PoolId:        poolId,
		InodeCount:    0,
		UsedSizeBytes: 0,
	}
}

func NewStatOfStorageClassByPoolWithQuota(poolId uint8, quotaGB uint64) *StatOfStorageClass {
	return &StatOfStorageClass{
		PoolId:        poolId,
		InodeCount:    0,
		UsedSizeBytes: 0,
		QuotaGB:       quotaGB,
	}
}

func NewStatOfStorageClassEx(storageClass uint32, cap uint64) *StatOfStorageClass {
	return &StatOfStorageClass{
		StorageClass:  storageClass,
		InodeCount:    0,
		UsedSizeBytes: 0,
		QuotaGB:       cap,
	}
}

func (st *StatOfStorageClass) Full() bool {
	if st == nil {
		return false
	}
	return st.QuotaGB != 0 && st.QuotaGB*util.GB <= st.UsedSizeBytes
}

func (st *StatOfStorageClass) String() string {
	return fmt.Sprintf("class(%s)_inoCnt(%d)_used(%d)_quota(%d)GB",
		StorageClassString(st.StorageClass), st.InodeCount, st.UsedSizeBytes, st.QuotaGB)
}

type StoreMode uint8

const StoreModeDef StoreMode = 0
const (
	StoreModeMem StoreMode = 1 << iota
	StoreModeRocksDb
	StoreModeMax
)

func (mode *StoreMode) Str() string {
	switch *mode {
	case StoreModeMem:
		return "Memory"
	case StoreModeRocksDb:
		return "Rocksdb"
	case StoreModeMem | StoreModeRocksDb:
		return "Memory&Rocksdb"
	default:
	}
	return "Unknown"
}

func (mode *StoreMode) Valid() (ok bool) {
	ok = *mode == StoreModeMem || *mode == StoreModeRocksDb
	return
}

type SelectMetaNodeInfo struct {
	PartitionID uint64 `json:"partition_id"`
	OldNodeAddr string `json:"old_node_addr"`
	NewNodeAddr string `json:"new_node_addr"`
	StoreMode   uint8  `json:"store_mode"`
}

// ScanInodeByPoolRequest defines the request to scan inodes by pool ID
type ScanInodeByPoolRequest struct {
	PartitionID uint64 `json:"partition_id"`
	PoolId      uint8  `json:"pool_id"`
	PageSize    uint32 `json:"page_size"`   // Maximum number of inodes to return (max 10000)
	StartInode  uint64 `json:"start_inode"` // Start inode ID for pagination (0 for first page)
	MinSize     uint64 `json:"min_size"`    // Minimum size of the inode to return
	CheckLease  bool   `json:"check_lease"` // Check lease of the inode to return
}

func (req *ScanInodeByPoolRequest) String() string {
	if req == nil {
		return "nil"
	}

	return fmt.Sprintf("partitionID[%d] poolId[%d] pageSize[%d] startInode[%d] minSize[%d] checkLease[%v]",
		req.PartitionID, req.PoolId, req.PageSize, req.StartInode, req.MinSize, req.CheckLease)
}

// ScanInodeByPoolResponse defines the response to ScanInodeByPoolRequest
type ScanInodeByPoolResponse struct {
	Inodes       []*InodeInfo `json:"inodes"`        // List of inode info
	NextInode    uint64       `json:"next_inode"`    // Next inode ID for pagination (0 if no more)
	HasMore      bool         `json:"has_more"`      // Whether there are more inodes to scan
	TotalScanned uint64       `json:"total_scanned"` // Total number of inodes scanned in this request
}

func (resp *ScanInodeByPoolResponse) String() string {
	return fmt.Sprintf("inodes[%v] nextInode[%d] hasMore[%v] totalScanned[%d]",
		len(resp.Inodes), resp.NextInode, resp.HasMore, resp.TotalScanned)
}
