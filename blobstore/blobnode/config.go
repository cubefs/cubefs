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
	"net/http"
	"os"
	"strconv"
	"strings"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	DefaultHeartbeatIntervalSec        = 30           // 30 s
	DefaultChunkReportIntervalSec      = 60           // 1 min
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

var (
	ErrNotSupportKey     = errors.New("not support this key")
	ErrValueType         = errors.New("value type not match this key")
	ErrNotConfigPrevious = errors.New("level previously not config")
	ErrNotConfigNow      = errors.New("level not config now")
	ErrValueOutOfLimit   = errors.New("value out of limit")
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
	configInit(&c)
	s.PutQpsLimitPerDisk.Reset(c.PutQpsLimitPerDisk)
	s.DeleteQpsLimitPerDisk.Reset(c.DeleteQpsLimitPerDisk)
	span.Info("hot reload limit config success.")
}

func (s *Service) changeQos(ctx context.Context, c Config) error {
	span := trace.SpanFromContextSafe(ctx)
	qosConf := c.DiskConfig.DataQos
	span.Infof("qos config:%v", qosConf)
	for k, v := range qosConf.LevelConfigs {
		para, err := qos.InitAndFixParaConfig(v)
		if err != nil {
			return err
		}
		qosConf.LevelConfigs[k] = para
	}
	return s.reloadQos(ctx, qosConf)
}

// key:disk_bandwidth_MBPS,disk_iops,level0.bandwidth_MBPS,level1.iops ...
func (s *Service) configReload(c *rpc.Context) {
	args := new(bnapi.ConfigReloadArgs)
	err := c.ParseArgs(args)
	if err != nil {
		c.RespondError(err)
		return
	}
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("config reload args:%v", args)

	levelKeys := strings.Split(args.Key, ".")
	switch len(levelKeys) {
	case 1:
		err := s.reloadDiskConf(ctx, args)
		if err != nil {
			c.RespondWith(http.StatusBadRequest, "", []byte(err.Error()))
			return
		}
	case 2:
		err := s.reloadLevelConf(ctx, args)
		if err != nil {
			c.RespondWith(http.StatusBadRequest, "", []byte(err.Error()))
			return
		}
	default:
		c.RespondWith(http.StatusBadRequest, "", []byte(ErrNotSupportKey.Error()))
		return
	}
}

func (s *Service) reloadQos(ctx context.Context, qosConf qos.Config) error {
	disks := s.copyDiskStorages(ctx)
	priLevels := priority.GetLevels()
	for pri, name := range priLevels {
		for _, ds := range disks {
			levelQos := ds.GetIoQos().GetIOQosIns().LevelMgr.GetLevel(priority.Priority(pri))
			if levelQos == nil {
				if _, ok := qosConf.LevelConfigs[name]; ok {
					return ErrNotConfigPrevious
				}
				break
			}
			if _, ok := qosConf.LevelConfigs[name]; !ok {
				return ErrNotConfigNow
			}
			levelQos.GetLevelQosIns().ChangeLevelQos(name, qosConf)
		}
	}
	return nil
}

func (s *Service) reloadDiskConf(ctx context.Context, args *bnapi.ConfigReloadArgs) (err error) {
	qosConf := s.Conf.DiskConfig.DataQos
	value, err := strconv.ParseInt(args.Value, 10, 64)
	if err != nil {
		return ErrValueType
	}
	if value <= 0 || value > 10000 {
		return ErrValueOutOfLimit
	}
	switch args.Key {
	case "disk_bandwidth_MBPS":
		qosConf.DiskBandwidthMBPS = value
	case "disk_iops":
		qosConf.DiskIOPS = value
	default:
		return ErrNotSupportKey
	}
	return s.reloadQos(ctx, qosConf)
}

func (s *Service) reloadLevelConf(ctx context.Context, args *bnapi.ConfigReloadArgs) (err error) {
	var value int64
	qosConf := s.Conf.DiskConfig.DataQos
	levelKeys := strings.Split(args.Key, ".")
	levelName, item := levelKeys[0], levelKeys[1]
	if _, ok := qosConf.LevelConfigs[levelName]; !ok {
		return ErrNotConfigPrevious
	}
	paraConf := qosConf.LevelConfigs[levelName]
	if args.Key != "factor" {
		value, err = strconv.ParseInt(args.Value, 10, 64)
		if err != nil {
			return ErrValueType
		}
		if value <= 0 || value > 10000 {
			return ErrValueOutOfLimit
		}
	}
	switch item {
	case "bandwidth_MBPS":
		paraConf.Bandwidth = value
	case "iops":
		paraConf.Iops = value
	case "factor":
		factor, err := strconv.ParseFloat(args.Value, 64)
		if err != nil {
			return ErrValueType
		}
		if factor <= 0 || factor > 1 {
			return ErrValueOutOfLimit
		}
		paraConf.Factor = factor
	default:
		return ErrNotSupportKey
	}
	qosConf.LevelConfigs[levelName] = paraConf
	return s.reloadQos(ctx, qosConf)
}
