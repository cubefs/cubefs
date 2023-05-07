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
)

const (
	DefaultDiskReservedSpaceB           = int64(60 << 30) // 60 GiB
	DefaultCompactReservedSpaceB        = int64(20 << 30) // 20 GiB
	DefaultChunkSize                    = int64(16 << 30) // 16 GiB
	DefaultMaxChunks                    = int32(1 << 13)  // 8192
	DefaultChunkReleaseProtectionM      = 1440            // 1 days
	DefaultChunkGcCreateTimeProtectionM = 1440            // 1 days
	DefaultChunkGcModifyTimeProtectionM = 1440            // 1 days
	DefaultChunkCompactIntervalSec      = 10 * 60         // 10 min
	DefaultChunkCleanIntervalSec        = 60              // 1 min
	DefaultDiskUsageIntervalSec         = 60              // 1 min
	DefaultDiskCleanTrashIntervalSec    = 60 * 60         // 60 min
	DefaultDiskTrashProtectionM         = 2880            // 2 days
	DefaultCompactBatchSize             = 1024            // 1024 counts
	DefaultCompactMinSizeThreshold      = 16 * (1 << 30)  // 16 GiB
	DefaultCompactTriggerThreshold      = 1 * (1 << 40)   // 1 TiB
	DefaultMetricReportIntervalS        = 30              // 30 Sec
	DefaultBlockBufferSize              = 64 * 1024       // 64k
	DefaultCompactEmptyRateThreshold    = float64(0.8)    // 80% rate
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
	AllowCleanTrash              bool    `json:"allow_clean_trash"`
	DisableModifyInCompacting    bool    `json:"disable_modify_in_compacting"`
	CompactMinSizeThreshold      int64   `json:"compact_min_size_threshold"`
	CompactTriggerThreshold      int64   `json:"compact_trigger_threshold"`
	CompactEmptyRateThreshold    float64 `json:"compact_empty_rate_threshold"`
	NeedCompactCheck             bool    `json:"need_compact_check"`
	AllowForceCompact            bool    `json:"allow_force_compact"`
	CompactBatchSize             int     `json:"compact_batch_size"`
	MustMountPoint               bool    `json:"must_mount_point"`
	IOStatFileDryRun             bool    `json:"iostat_file_dryrun"`
	MetricReportIntervalS        int64   `json:"metric_report_interval_S"`
	BlockBufferSize              int64   `json:"block_buffer_size"`
	EnableDataInspect            bool    `json:"enable_data_inspect"`
	WriteThreadCnt               uint32  `json:"write_thread_count"`
	ReadThreadCnt                uint32  `json:"read_thread_count"`

	DataQos qos.Config `json:"data_qos"`
}

type HostInfo struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	IDC       string          `json:"idc"`
	Rack      string          `json:"rack"`
	Host      string          `json:"host"`
}

type Config struct {
	BaseConfig
	RuntimeConfig
	HostInfo
	db.MetaConfig

	AllocDiskID      func(ctx context.Context) (proto.DiskID, error)
	HandleIOError    func(ctx context.Context, diskID proto.DiskID, diskErr error)
	NotifyCompacting func(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error)
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
	if conf.DiskReservedSpaceB <= 0 {
		conf.DiskReservedSpaceB = DefaultDiskReservedSpaceB
	}
	if conf.CompactReservedSpaceB <= 0 {
		conf.CompactReservedSpaceB = DefaultCompactReservedSpaceB
	}
	if conf.CompactReservedSpaceB > conf.DiskReservedSpaceB {
		return errors.New("CompactReservedSpaceB is larger than DiskReservedSpaceB")
	}
	if conf.MaxChunks <= 0 {
		conf.MaxChunks = DefaultMaxChunks
	}
	if conf.ChunkReleaseProtectionM <= 0 {
		conf.ChunkReleaseProtectionM = DefaultChunkReleaseProtectionM
	}
	if conf.ChunkCompactIntervalSec <= 0 {
		conf.ChunkCompactIntervalSec = DefaultChunkCompactIntervalSec
	}
	if conf.ChunkCleanIntervalSec <= 0 {
		conf.ChunkCleanIntervalSec = DefaultChunkCleanIntervalSec
	}
	if conf.ChunkGcCreateTimeProtectionM <= 0 {
		conf.ChunkGcCreateTimeProtectionM = DefaultChunkGcCreateTimeProtectionM
	}
	if conf.ChunkGcModifyTimeProtectionM <= 0 {
		conf.ChunkGcModifyTimeProtectionM = DefaultChunkGcModifyTimeProtectionM
	}
	if conf.DiskUsageIntervalSec <= 0 {
		conf.DiskUsageIntervalSec = DefaultDiskUsageIntervalSec
	}
	if conf.DiskCleanTrashIntervalSec <= 0 {
		conf.DiskCleanTrashIntervalSec = DefaultDiskCleanTrashIntervalSec
	}
	if conf.DiskTrashProtectionM <= 0 {
		conf.DiskTrashProtectionM = DefaultDiskTrashProtectionM
	}

	if conf.CompactTriggerThreshold <= 0 {
		conf.CompactTriggerThreshold = int64(DefaultCompactTriggerThreshold)
	}
	if conf.CompactMinSizeThreshold <= 0 {
		conf.CompactMinSizeThreshold = int64(DefaultCompactMinSizeThreshold)
	}
	if conf.CompactEmptyRateThreshold <= 0 {
		conf.CompactEmptyRateThreshold = DefaultCompactEmptyRateThreshold
	}

	if conf.CompactBatchSize <= 0 {
		conf.CompactBatchSize = DefaultCompactBatchSize
	}

	if conf.MetricReportIntervalS <= 0 {
		conf.MetricReportIntervalS = DefaultMetricReportIntervalS
	}

	if conf.BlockBufferSize <= 0 {
		conf.BlockBufferSize = DefaultBlockBufferSize
	}

	return nil
}
