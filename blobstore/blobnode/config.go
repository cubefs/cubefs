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

package blobnode

import (
	"context"
	"os"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	DefaultHeartbeatIntervalSec        = 30           // 30 s
	DefaultChunkReportIntervalSec      = 1*60 - 3     // 1 min
	DefaultCleanExpiredStatIntervalSec = 60 * 60      // 60 min
	DefaultChunkGcIntervalSec          = 30 * 60      // 30 min
	DefaultChunkInspectIntervalSec     = 2 * 60 * 60  // 2 hour
	DefaultChunkProtectionPeriodSec    = 48 * 60 * 60 // 48 hour
	DefaultDiskStatusCheckIntervalSec  = 2 * 60       // 2 min

	DefaultPutQpsLimitPerDisk    = 128
	DefaultGetQpsLimitPerDisk    = 512
	DefaultGetQpsLimitPerKey     = 64
	DefaultDeleteQpsLimitPerDisk = 128
)

type Config struct {
	cmd.Config
	core.HostInfo
	WorkerConfig
	Disks         []core.Config      `json:"disks"`
	DiskConfig    core.RuntimeConfig `json:"disk_config"`
	MetaConfig    db.MetaConfig      `json:"meta_config"`
	FlockFilename string             `json:"flock_filename"`

	Clustermgr *cmapi.Config `json:"clustermgr"`

	HeartbeatIntervalSec        int `json:"heartbeat_interval_S"`
	ChunkReportIntervalSec      int `json:"chunk_report_interval_S"`
	ChunkGcIntervalSec          int `json:"chunk_gc_interval_S"`
	ChunkInspectIntervalSec     int `json:"chunk_inspect_interval_S"`
	ChunkProtectionPeriodSec    int `json:"chunk_protection_period_S"`
	CleanExpiredStatIntervalSec int `json:"clean_expired_stat_interval_S"`
	DiskStatusCheckIntervalSec  int `json:"disk_status_check_interval_S"`

	PutQpsLimitPerDisk    int `json:"put_qps_limit_per_disk"`
	GetQpsLimitPerDisk    int `json:"get_qps_limit_per_disk"`
	GetQpsLimitPerKey     int `json:"get_qps_limit_per_key"`
	DeleteQpsLimitPerDisk int `json:"delete_qps_limit_per_disk"`
}

func configInit(config *Config) {
	if len(config.Disks) == 0 {
		log.Fatalf("disk list is empty")
		os.Exit(1)
	}

	if config.HeartbeatIntervalSec <= 0 {
		config.HeartbeatIntervalSec = DefaultHeartbeatIntervalSec
	}

	if config.ChunkGcIntervalSec <= 0 {
		config.ChunkGcIntervalSec = DefaultChunkGcIntervalSec
	}

	if config.ChunkInspectIntervalSec <= 0 {
		config.ChunkInspectIntervalSec = DefaultChunkInspectIntervalSec
	}

	if config.ChunkProtectionPeriodSec <= 0 {
		config.ChunkProtectionPeriodSec = DefaultChunkProtectionPeriodSec
	}

	if config.DiskStatusCheckIntervalSec <= 0 {
		config.DiskStatusCheckIntervalSec = DefaultDiskStatusCheckIntervalSec
	}

	if config.ChunkReportIntervalSec <= 0 {
		config.ChunkReportIntervalSec = DefaultChunkReportIntervalSec
	}

	if config.CleanExpiredStatIntervalSec <= 0 {
		config.CleanExpiredStatIntervalSec = DefaultCleanExpiredStatIntervalSec
	}

	if config.PutQpsLimitPerDisk <= 0 {
		config.PutQpsLimitPerDisk = DefaultPutQpsLimitPerDisk
	}

	if config.GetQpsLimitPerDisk == 0 {
		config.GetQpsLimitPerDisk = DefaultGetQpsLimitPerDisk
	}

	if config.GetQpsLimitPerKey == 0 {
		config.GetQpsLimitPerKey = DefaultGetQpsLimitPerKey
	}

	if config.DeleteQpsLimitPerDisk <= 0 {
		config.DeleteQpsLimitPerDisk = DefaultDeleteQpsLimitPerDisk
	}
}

func (s *Service) changeLimit(ctx context.Context, c Config) {
	span := trace.SpanFromContextSafe(ctx)
	s.PutQpsLimitPerDisk.Reset(c.PutQpsLimitPerDisk)
	s.DeleteQpsLimitPerDisk.Reset(c.DeleteQpsLimitPerDisk)
	span.Info("hot reload limit config success.")
}

func (s *Service) changeQos(ctx context.Context, c Config) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	disks := s.copyDiskStorages(ctx)
	qosConfig := c.DiskConfig.DiskQos
	span.Infof("qos config:", qosConfig)
	priLevels := priority.GetLevels()
	for pri, name := range priLevels {
		for _, ds := range disks {
			levelQos := ds.GetIoQos().GetIOQosIns().LevelMgr.GetLevel(priority.Priority(pri))
			if levelQos == nil {
				if _, ok := qosConfig.LevelConfigs[name]; ok {
					return errors.New("level previously not config")
				}
				break
			}
			if _, ok := qosConfig.LevelConfigs[name]; !ok {
				return errors.New("level not config now")
			}
			levelQos.GetLevelQosIns().ChangeLevelQos(name, qosConfig)
		}
	}
	return
}
