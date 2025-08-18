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
	"strconv"
	"strings"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/cmd"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	DefaultHeartbeatIntervalSec        = 30           // 30 s
	DefaultChunkReportIntervalSec      = 60           // 1 min
	DefaultCleanExpiredStatIntervalSec = 60 * 60      // 60 min
	DefaultChunkGcIntervalSec          = 30 * 60      // 30 min
	DefaultChunkProtectionPeriodSec    = 48 * 60 * 60 // 48 hour
	DefaultDiskStatusCheckIntervalSec  = 2 * 60       // 2 min

	defaultInspectIntervalSec  = 90              // 90 second
	defaultInspectNextRoundSec = 2 * 60 * 60     // 2 hour
	defaultInspectRate         = 4 * 1024 * 1024 // rate limit max 4MB per second
	defaultInspectLogChunkSize = uint(27)        // 2<<27, 128MB
	defaultInspectLogBackup    = 2

	defaultServiceBothBlobNodeWorker = "BOTH_BLOBNODE_WORKER"
)

var (
	ErrNotSupportKey   = errors.New("not support this key")
	ErrValueType       = errors.New("value type not match this key")
	ErrValueOutOfLimit = errors.New("value out of limit")
)

type StartMode string

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
	ChunkProtectionPeriodSec    int `json:"chunk_protection_period_S"`
	CleanExpiredStatIntervalSec int `json:"clean_expired_stat_interval_S"`
	DiskStatusCheckIntervalSec  int `json:"disk_status_check_interval_S"`

	InspectConf DataInspectConf `json:"inspect_conf"`
	StartMode   StartMode       `json:"start_mode"`
}

func configInit(config *Config) {
	if config.StartMode == proto.ServiceNameWorker {
		return
	}

	if len(config.Disks) == 0 {
		log.Fatalf("disk list is empty")
	}

	if config.HeartbeatIntervalSec <= 0 {
		config.HeartbeatIntervalSec = DefaultHeartbeatIntervalSec
	}

	if config.ChunkGcIntervalSec <= 0 {
		config.ChunkGcIntervalSec = DefaultChunkGcIntervalSec
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

	defaulter.LessOrEqual(&config.InspectConf.IntervalSec, defaultInspectIntervalSec)
	defaulter.LessOrEqual(&config.InspectConf.RateLimit, defaultInspectRate)
	defaulter.LessOrEqual(&config.InspectConf.NexRoundSec, defaultInspectNextRoundSec)
	defaulter.LessOrEqual(&config.InspectConf.Record.ChunkBits, defaultInspectLogChunkSize)
	defaulter.LessOrEqual(&config.InspectConf.Record.Backup, defaultInspectLogBackup)

	defaulter.LessOrEqual(&config.HostInfo.DiskType, proto.DiskTypeHDD)

	defaulter.Empty((*string)(&config.StartMode), defaultServiceBothBlobNodeWorker)
	// Start the worker process service by the ${StartMode}: only worker, only blobnode, both
	if config.StartMode != defaultServiceBothBlobNodeWorker && config.StartMode != proto.ServiceNameWorker && config.StartMode != proto.ServiceNameBlobNode {
		log.Fatalf("fail to start service, mode is %s, please use valid start mode", config.StartMode)
	}
}

func (s *Service) changeLimit(ctx context.Context, c Config) {
	span := trace.SpanFromContextSafe(ctx)
	configInit(&c)
	span.Info("hot reload limit config success.")
}

func (s *Service) changeQos(ctx context.Context, qosConf qos.Config) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	if err = qos.FixQosConfigOnInit(&qosConf); err != nil {
		return errcode.ErrInternal
	}
	span.Infof("hot change qos config:%v", qosConf)

	for _, ioType := range bnapi.GetAllIOType() {
		if levelConf, exist := qosConf.Level[ioType]; exist {
			if err = s.reloadLevelQos(ctx, ioType, levelConf); err != nil {
				return err
			}
		}
	}
	return s.reloadDiskQos(ctx, qosConf.CommonDiskConfig)
}

func (s *Service) ConfigReload(c *rpc.Context) {
	args := new(bnapi.ConfigReloadArgs)
	err := c.ParseArgs(args)
	if err != nil {
		c.RespondError(err)
		return
	}
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("config reload args:%+v", args)

	// e.g. /config/reload?key=level.read.concurrency&value=10
	//      /config/reload?key=disk.disk_bandwidth_mb&value=128
	splitKeys := strings.Split(args.Key, ".")
	if len(splitKeys) < 2 {
		c.RespondWith(http.StatusBadRequest, "", []byte(ErrNotSupportKey.Error()))
		return
	}

	switch splitKeys[0] {
	case "disk":
		err = s.resetQosDiskConf(ctx, splitKeys[1], args.Value)
	case "level":
		err = s.resetQosLevelConf(ctx, args)
	default:
		err = ErrNotSupportKey
	}

	if err != nil {
		c.RespondWith(http.StatusBadRequest, "", []byte(err.Error()))
		return
	}
	c.Respond()
}

func (s *Service) ConfigGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	qosConf := s.getQosConfig(ctx)
	span.Infof("config get result:%v", qosConf)
	c.RespondJSON(qosConf)
}

func (s *Service) getQosConfig(ctx context.Context) qos.FlowConfig {
	disks := s.copyDiskStorages(ctx)
	if len(disks) > 0 {
		return disks[0].GetIoQos().GetConfig()
	}
	return qos.FlowConfig{}
}

func (s *Service) resetQosDiskConf(ctx context.Context, diskKey, diskVal string) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	diskConf := qos.CommonDiskConfig{}

	switch diskKey {
	case "disk_iops":
		diskConf.DiskIops, err = parseQosArgsInt(diskVal)
	case "disk_bandwidth_mb":
		diskConf.DiskBandwidthMB, err = parseQosArgsInt(diskVal)
	case "disk_idle_factor":
		diskConf.DiskIdleFactor, err = parseQosArgsFloat(diskVal)
	default:
		return ErrNotSupportKey
	}

	if err != nil {
		return err
	}
	span.Infof("qos reload disk conf:%v", diskConf)
	return s.reloadDiskQos(ctx, diskConf)
}

func (s *Service) reloadDiskQos(ctx context.Context, diskConf qos.CommonDiskConfig) (err error) {
	disks := s.copyDiskStorages(ctx)
	for _, ds := range disks {
		ds.GetIoQos().ResetDiskConfig(diskConf)
	}
	return nil
}

func (s *Service) resetQosLevelConf(ctx context.Context, args *bnapi.ConfigReloadArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	levelConf := qos.LevelFlowConfig{}

	splitKeys := strings.Split(args.Key, ".")
	if len(splitKeys) < 3 {
		return ErrNotSupportKey
	}

	levelName, field := splitKeys[1], splitKeys[2]
	if !bnapi.StringToIOType(levelName).IsValid() {
		return ErrNotSupportKey
	}

	switch field {
	case "mbps":
		levelConf.MBPS, err = parseQosArgsInt(args.Value)
	case "concurrency":
		levelConf.Concurrency, err = parseQosArgsInt(args.Value)
	case "bid_concurrency":
		levelConf.BidConcurrency, err = parseQosArgsInt(args.Value)
		levelConf.BidConcurrency = qos.FixQosBidConcurrency(levelName, levelConf.BidConcurrency)
	case "busy_factor":
		levelConf.BusyFactor, err = parseQosArgsFloat(args.Value)
	case "idle_factor":
		levelConf.IdleFactor, err = parseQosArgsFloat(args.Value)
	default:
		return ErrNotSupportKey
	}

	if err != nil {
		return err
	}
	span.Infof("qos reload level conf:(%s) %v", levelName, levelConf)
	return s.reloadLevelQos(ctx, levelName, levelConf)
}

func (s *Service) reloadLevelQos(ctx context.Context, level string, levelConf qos.LevelFlowConfig) (err error) {
	ioType := bnapi.StringToIOType(level)
	disks := s.copyDiskStorages(ctx)

	for _, ds := range disks {
		qosMgr := ds.GetIoQos()
		if !qosMgr.ResetLevelConfig(ioType, levelConf) {
			return errcode.ErrInternal
		}
	}
	return nil
}

func parseQosArgsInt(value string) (int64, error) {
	ret, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, ErrValueType
	}
	if ret <= 0 || ret > 10000 {
		return 0, ErrValueOutOfLimit
	}
	return ret, nil
}

func parseQosArgsFloat(value string) (float64, error) {
	ret, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, ErrValueType
	}
	if ret <= 0 || ret > 10 {
		return 0, ErrValueOutOfLimit
	}
	return ret, nil
}
