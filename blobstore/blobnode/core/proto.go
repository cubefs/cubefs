// Copyright 2022 The CubeFS Authors.
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

package core

import (
	"context"
	"io"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	CrcBlockUnitSize = 64 * 1024 // 64k
)

// chunk meta data for kv db
type VuidMeta struct {
	Version     uint8             `json:"version"`
	Vuid        proto.Vuid        `json:"vuid"`
	DiskID      proto.DiskID      `json:"diskid"`
	ChunkId     bnapi.ChunkId     `json:"chunkname"`
	ParentChunk bnapi.ChunkId     `json:"parentchunk"`
	ChunkSize   int64             `json:"chunksize"`
	Ctime       int64             `json:"ctime"` // nsec
	Mtime       int64             `json:"mtime"` // nsec
	Compacting  bool              `json:"compacting"`
	Status      bnapi.ChunkStatus `json:"status"` // normal、release
	Reason      string            `json:"reason"`
}

// disk meta data for rocksdb
type DiskMeta struct {
	FormatInfo
	Host       string           `json:"host"`
	Path       string           `json:"path"`
	Status     proto.DiskStatus `json:"status"`
	Registered bool             `json:"registered"`
	Mtime      int64            `json:"mtime"`
}

type DiskStats struct {
	Used          int64 `json:"used"`            // actual physical space usage
	Free          int64 `json:"free"`            // actual remaining physical space on the disk
	Reserved      int64 `json:"reserved"`        // reserve space on the disk
	TotalDiskSize int64 `json:"total_disk_size"` // total actual disk size
}

type StorageStat struct {
	FileSize   int64         `json:"file_size"`
	PhySize    int64         `json:"phy_size"`
	ParentID   bnapi.ChunkId `json:"parent_id"`
	CreateTime int64         `json:"create_time"`
}

type MetaHandler interface {
	ID() bnapi.ChunkId
	InnerDB() db.MetaHandler
	SupportInline() bool
	Write(ctx context.Context, bid proto.BlobID, value ShardMeta) (err error)
	Read(ctx context.Context, bid proto.BlobID) (value ShardMeta, err error)
	Delete(ctx context.Context, bid proto.BlobID) (err error)
	Scan(ctx context.Context, startBid proto.BlobID, limit int,
		fn func(bid proto.BlobID, sm *ShardMeta) error) (err error)
	Destroy(ctx context.Context) (err error)
	Close()
}

type DataHandler interface {
	Write(ctx context.Context, shard *Shard) error
	Read(ctx context.Context, shard *Shard, from, to uint32) (r io.Reader, err error)
	Stat() (stat *StorageStat, err error)
	Flush() (err error)
	Delete(ctx context.Context, shard *Shard) (err error)
	Destroy(ctx context.Context) (err error)
	Close()
}

type Storage interface {
	ID() bnapi.ChunkId
	MetaHandler() MetaHandler
	DataHandler() DataHandler
	RawStorage() Storage
	Write(ctx context.Context, b *Shard) (err error)
	ReadShardMeta(ctx context.Context, bid proto.BlobID) (sm *ShardMeta, err error)
	NewRangeReader(ctx context.Context, b *Shard, from, to int64) (rc io.Reader, err error)
	MarkDelete(ctx context.Context, bid proto.BlobID) (err error)
	Delete(ctx context.Context, bid proto.BlobID) (n int64, err error)
	ScanMeta(ctx context.Context, startBid proto.BlobID, limit int,
		fn func(bid proto.BlobID, sm *ShardMeta) error) (err error)
	SyncData(ctx context.Context) (err error)
	Sync(ctx context.Context) (err error)
	Stat(ctx context.Context) (stat *StorageStat, err error)
	PendingError() error
	PendingRequest() int64
	IncrPendingCnt()
	DecrPendingCnt()
	Close(ctx context.Context)
	Destroy(ctx context.Context)
}

// chunk storage api
type ChunkAPI interface {
	// infos
	ID() bnapi.ChunkId
	Vuid() proto.Vuid
	Disk() (disk DiskAPI)
	Status() bnapi.ChunkStatus
	VuidMeta() (vm *VuidMeta)
	ChunkInfo(ctx context.Context) (info bnapi.ChunkInfo)

	// method
	Write(ctx context.Context, b *Shard) (err error)
	Read(ctx context.Context, b *Shard) (n int64, err error)
	RangeRead(ctx context.Context, b *Shard) (n int64, err error)
	MarkDelete(ctx context.Context, bid proto.BlobID) (err error)
	Delete(ctx context.Context, bid proto.BlobID) (err error)
	ReadShardMeta(ctx context.Context, bid proto.BlobID) (sm *ShardMeta, err error)
	ListShards(ctx context.Context, startBid proto.BlobID, cnt int, status bnapi.ShardStatus) (infos []*bnapi.ShardInfo, next proto.BlobID, err error)
	Sync(ctx context.Context) (err error)
	SyncData(ctx context.Context) (err error)
	Close(ctx context.Context)

	// compact
	StartCompact(ctx context.Context) (ncs ChunkAPI, err error)
	CommitCompact(ctx context.Context, ncs ChunkAPI) (err error)
	StopCompact(ctx context.Context, ncs ChunkAPI) (err error)
	NeedCompact(ctx context.Context) bool
	IsDirty() bool
	IsClosed() bool
	AllowModify() (err error)
	HasEnoughSpace(needSize int64) bool
	HasPendingRequest() bool
	SetStatus(status bnapi.ChunkStatus) (err error)
	SetDirty(dirty bool)
}

type DiskAPI interface {
	ID() proto.DiskID
	Status() (status proto.DiskStatus)
	DiskInfo() (info bnapi.DiskInfo)
	Stats() (stat DiskStats)
	GetChunkStorage(vuid proto.Vuid) (cs ChunkAPI, found bool)
	GetConfig() (config *Config)
	GetIoQos() (ioQos qos.Qos)
	GetDataPath() (path string)
	GetMetaPath() (path string)
	SetStatus(status proto.DiskStatus)
	LoadDiskInfo(ctx context.Context) (dm DiskMeta, err error)
	UpdateDiskStatus(ctx context.Context, status proto.DiskStatus) (err error)
	CreateChunk(ctx context.Context, vuid proto.Vuid, chunksize int64) (cs ChunkAPI, err error)
	ReleaseChunk(ctx context.Context, vuid proto.Vuid, force bool) (err error)
	UpdateChunkStatus(ctx context.Context, vuid proto.Vuid, status bnapi.ChunkStatus) (err error)
	UpdateChunkCompactState(ctx context.Context, vuid proto.Vuid, compacting bool) (err error)
	ListChunks(ctx context.Context) (chunks []VuidMeta, err error)
	EnqueueCompact(ctx context.Context, vuid proto.Vuid)
	GcRubbishChunk(ctx context.Context) (mayBeLost []bnapi.ChunkId, err error)
	WalkChunksWithLock(ctx context.Context, fn func(cs ChunkAPI) error) (err error)
	ResetChunks(ctx context.Context)
	Close(ctx context.Context)
}
