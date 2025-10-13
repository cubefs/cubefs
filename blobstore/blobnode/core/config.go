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
	"errors"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	DefaultDiskReservedSpaceB           = int64(60 << 30)       // 60 GiB
	DefaultCompactReservedSpaceB        = int64(20 << 30)       // 20 GiB
	DefaultChunkSize                    = int64(16 << 30)       // 16 GiB
	DefaultMaxChunks                    = int32(1 << 13)        // 8192
	DefaultChunkReleaseProtectionM      = int64(30)             // 30 min
	DefaultChunkGcCreateTimeProtectionM = int64(1440)           // 1 days
	DefaultChunkGcModifyTimeProtectionM = int64(1440)           // 1 days
	DefaultChunkCompactIntervalSec      = int64(10 * 60)        // 10 min
	DefaultChunkCleanIntervalSec        = int64(60)             // 1 min
	DefaultDiskUsageIntervalSec         = int64(60)             // 1 min
	DefaultDiskCleanTrashIntervalSec    = int64(60)             // 1 min
	DefaultDiskTrashProtectionM         = int64(1440)           // 1 days
	DefaultCompactBatchSize             = 1024                  // 1024 counts
	DefaultCompactMinSizeThreshold      = int64(16 * (1 << 30)) // 16 GiB
	DefaultCompactTriggerThreshold      = int64(1 * (1 << 40))  // 1 TiB
	DefaultMetricReportIntervalS        = int64(300)            // 300 Sec
	DefaultBlockBufferSize              = int64(64 * 1024)      // 64k
	DefaultCompactEmptyRateThreshold    = float64(0.8)          // 80% rate
	DefaultBatchReadBufferSize          = int64(1024 * 1024)    // 1 MB
	DefaultBatchReadHoleThreshold       = int64(256 * 1024)     // hole data in batch
	defaultWriteThreadCnt               = 4
	defaultReadThreadCnt                = 4
	defaultDeleteThreadCnt              = 2
	defaultBackgroundThreadCnt          = 2
	defaultWriteQueueDepth              = 64
	defaultReadQueueDepth               = 64
	defaultDeleteQueueDepth             = 32
	defaultBackgroundQueueDepth         = 32
)

// Config for disk
type BaseConfig struct {
	Path        string `json:"path"`
	AutoFormat  bool   `json:"auto_format"`
	MaxChunks   int32  `json:"max_chunks"`
	DisableSync bool   `json:"disable_sync"`
}

type RuntimeConfig struct {
	DiskReservedSpaceB           int64   `json:"disk_reserved_space_B"`             // threshold
	CompactReservedSpaceB        int64   `json:"compact_reserved_space_B"`          // compact reserve
	ChunkReleaseProtectionM      int64   `json:"chunk_protection_M"`                // protect
	ChunkCompactIntervalSec      int64   `json:"chunk_compact_interval_S"`          // loop
	ChunkCleanIntervalSec        int64   `json:"chunk_clean_interval_S"`            // loop
	ChunkGcCreateTimeProtectionM int64   `json:"chunk_gc_create_time_protection_M"` // protect
	ChunkGcModifyTimeProtectionM int64   `json:"chunk_gc_modify_time_protection_M"` // protect
	DiskUsageIntervalSec         int64   `json:"disk_usage_interval_S"`             // loop
	DiskCleanTrashIntervalSec    int64   `json:"disk_clean_trash_interval_S"`       // loop
	DiskTrashProtectionM         int64   `json:"disk_trash_protection_M"`           // protect
	CompactMinSizeThreshold      int64   `json:"compact_min_size_threshold"`
	CompactTriggerThreshold      int64   `json:"compact_trigger_threshold"`
	CompactEmptyRateThreshold    float64 `json:"compact_empty_rate_threshold"`
	NeedCompactCheck             bool    `json:"need_compact_check"`
	AllowForceCompact            bool    `json:"allow_force_compact"`
	AllowCleanTrash              bool    `json:"allow_clean_trash"`
	DisableModifyInCompacting    bool    `json:"disable_modify_in_compacting"`
	MustMountPoint               bool    `json:"must_mount_point"`
	IOStatFileDryRun             bool    `json:"iostat_file_dryrun"`
	SetDefaultSwitch             bool    `json:"set_default_switch"`
	EnableDeleteShardVerify      bool    `json:"enable_delete_shard_verify"`
	CompactBatchSize             int     `json:"compact_batch_size"`
	MetricReportIntervalS        int64   `json:"metric_report_interval_S"`
	BlockBufferSize              int64   `json:"block_buffer_size"`
	BatchBufferSize              int64   `json:"batch_buffer_size"`
	BatchBufferHoleThreshold     int64   `json:"batch_buffer_hole_threshold"`

	WriteThreadCnt       int        `json:"write_thread_cnt"`
	ReadThreadCnt        int        `json:"read_thread_cnt"`
	DeleteThreadCnt      int        `json:"delete_thread_cnt"`
	BackgroundThreadCnt  int        `json:"background_thread_cnt"`
	WriteQueueDepth      int        `json:"write_queue_depth"`
	ReadQueueDepth       int        `json:"read_queue_depth"`
	DeleteQueueDepth     int        `json:"delete_queue_depth"`
	BackgroundQueueDepth int        `json:"background_queue_depth"`
	DataQos              qos.Config `json:"data_qos"`
}

type HostInfo struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	IDC       string          `json:"idc"`
	Rack      string          `json:"rack"`
	Host      string          `json:"host"`
	DiskType  proto.DiskType  `json:"disk_type,omitempty"` // On a node, there is only one type of disk, and no other types
	NodeID    proto.NodeID    `json:"-"`                   // A node is a process
}

type Config struct {
	BaseConfig
	RuntimeConfig
	HostInfo
	db.MetaConfig

	AllocDiskID      func(ctx context.Context) (proto.DiskID, error)
	HandleIOError    func(ctx context.Context, diskID proto.DiskID, diskErr error)
	NotifyCompacting func(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error)
	GetGlobalConfig  func(ctx context.Context, key string) (value string, err error)
}

func InitConfig(conf *Config) error {
	if conf.Path == "" {
		return errors.New("filename is not specified")
	}
	if conf.HandleIOError == nil {
		return errors.New("handleIOError is not specified")
	}
	if conf.AllocDiskID == nil {
		return errors.New("allocDiskID is not specified")
	}
	if conf.CompactReservedSpaceB > conf.DiskReservedSpaceB {
		return errors.New("CompactReservedSpaceB is larger than DiskReservedSpaceB")
	}
	defaulter.LessOrEqual(&conf.DiskReservedSpaceB, DefaultDiskReservedSpaceB)
	defaulter.LessOrEqual(&conf.CompactReservedSpaceB, DefaultCompactReservedSpaceB)
	defaulter.LessOrEqual(&conf.MaxChunks, DefaultMaxChunks)
	defaulter.LessOrEqual(&conf.ChunkCompactIntervalSec, DefaultChunkCompactIntervalSec)
	defaulter.LessOrEqual(&conf.ChunkGcCreateTimeProtectionM, DefaultChunkGcCreateTimeProtectionM)
	defaulter.LessOrEqual(&conf.ChunkGcModifyTimeProtectionM, DefaultChunkGcModifyTimeProtectionM)
	defaulter.LessOrEqual(&conf.DiskUsageIntervalSec, DefaultDiskUsageIntervalSec)
	defaulter.LessOrEqual(&conf.CompactTriggerThreshold, DefaultCompactTriggerThreshold)
	defaulter.LessOrEqual(&conf.CompactMinSizeThreshold, DefaultCompactMinSizeThreshold)
	defaulter.LessOrEqual(&conf.CompactEmptyRateThreshold, DefaultCompactEmptyRateThreshold)
	defaulter.LessOrEqual(&conf.CompactBatchSize, DefaultCompactBatchSize)
	defaulter.LessOrEqual(&conf.BatchBufferSize, DefaultBatchReadBufferSize)
	defaulter.LessOrEqual(&conf.BatchBufferHoleThreshold, DefaultBatchReadHoleThreshold)
	defaulter.LessOrEqual(&conf.BlockBufferSize, DefaultBlockBufferSize)

	defaulter.LessOrEqual(&conf.ChunkCleanIntervalSec, DefaultChunkCleanIntervalSec)
	defaulter.LessOrEqual(&conf.ChunkReleaseProtectionM, DefaultChunkReleaseProtectionM)
	defaulter.LessOrEqual(&conf.DiskCleanTrashIntervalSec, DefaultDiskCleanTrashIntervalSec)
	defaulter.LessOrEqual(&conf.DiskTrashProtectionM, DefaultDiskTrashProtectionM)
	defaulter.LessOrEqual(&conf.MetricReportIntervalS, DefaultMetricReportIntervalS)
	if conf.SetDefaultSwitch {
		conf.NeedCompactCheck = true
		conf.AllowForceCompact = true
		conf.AllowCleanTrash = true
	}

	defaulter.Equal(&conf.WriteThreadCnt, defaultWriteThreadCnt)
	defaulter.Equal(&conf.ReadThreadCnt, defaultReadThreadCnt)
	defaulter.Equal(&conf.DeleteThreadCnt, defaultDeleteThreadCnt)
	defaulter.Equal(&conf.BackgroundThreadCnt, defaultBackgroundThreadCnt)
	defaulter.LessOrEqual(&conf.WriteQueueDepth, defaultWriteQueueDepth)
	defaulter.LessOrEqual(&conf.ReadQueueDepth, defaultReadQueueDepth)
	defaulter.LessOrEqual(&conf.DeleteQueueDepth, defaultDeleteQueueDepth)
	defaulter.LessOrEqual(&conf.BackgroundQueueDepth, defaultBackgroundQueueDepth)
	return qos.FixQosConfigOnInit(&conf.DataQos)
}

func CheckNodeConf(conf *HostInfo) error {
	if conf.DiskType <= 0 {
		return errors.New("disk type is not specified")
	}

	return nil
}
