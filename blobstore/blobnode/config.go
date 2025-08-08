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

func (s *Service) changeQos(ctx context.Context, c Config) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	qosConf := c.DiskConfig.DataQos
	span.Infof("qos config:%v", qosConf)

	for _, ioType := range bnapi.GetAllIOType() {
		levelConf := qosConf.FlowConf.Level[ioType]
		if err = s.reloadLevelQos(ctx, ioType, levelConf); err != nil {
			return err
		}
	}

	return s.reloadDiskQos(ctx, qosConf.FlowConf.CommonDiskConfig)
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
	span.Infof("config reload args:%v", args)

	// e.g. /config/reload?key=level.read.concurrency&value=10
	//      /config/reload?key=disk.disk_bandwidth_mb&value=128
	splitKeys := strings.Split(args.Key, ".")
	if len(splitKeys) < 2 {
		c.RespondWith(http.StatusBadRequest, "", []byte(ErrNotSupportKey.Error()))
		return
	}

	switch splitKeys[0] {
	case "disk":
		err = s.reloadDiskConf(ctx, splitKeys[1], args.Value)
	case "level":
		err = s.reloadQosLevelConf(ctx, args)
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
	ret := qos.Config{}

	disks := s.copyDiskStorages(ctx)
	for _, ds := range disks {
		ret = ds.GetIoQos().GetConfig()
		break
	}

	span := trace.SpanFromContextSafe(ctx)
	span.Infof("config get result:%v", ret)
	c.RespondJSON(ret)
}

func (s *Service) reloadDiskConf(ctx context.Context, diskKey, diskVal string) (err error) {
	qosConf := &s.Conf.DiskConfig.DataQos
	if err = qos.FixQosConfigHotReset(qosConf); err != nil {
		return err
	}

	switch diskKey {
	case "disk_iops":
		if qosConf.FlowConf.DiskIops, err = parseQosArgsInt(diskVal); err != nil {
			return err
		}
	case "disk_bandwidth_mb":
		if qosConf.FlowConf.DiskBandwidthMB, err = parseQosArgsInt(diskVal); err != nil {
			return err
		}
	case "disk_idle_factor":
		if qosConf.FlowConf.DiskIdleFactor, err = parseQosArgsFloat(diskVal); err != nil {
			return err
		}
	default:
		return ErrNotSupportKey
	}

	return s.reloadDiskQos(ctx, qosConf.FlowConf.CommonDiskConfig)
}

func (s *Service) reloadDiskQos(ctx context.Context, diskConf qos.CommonDiskConfig) (err error) {
	disks := s.copyDiskStorages(ctx)
	for _, ds := range disks {
		qosMgr := ds.GetIoQos()
		for tp := range bnapi.GetAllIOType() {
			ioQ, exist := qosMgr.GetQueueQos(bnapi.SetIoType(ctx, bnapi.IOType(tp)))
			if !exist {
				return errcode.ErrInternal
			}
			ioQ.ResetDiskLimit(diskConf)
		}
	}
	return nil
}

func (s *Service) reloadQosLevelConf(ctx context.Context, args *bnapi.ConfigReloadArgs) (err error) {
	splitKeys := strings.Split(args.Key, ".")
	if len(splitKeys) < 3 {
		return ErrNotSupportKey
	}

	qosConf := &s.Conf.DiskConfig.DataQos
	if err = qos.FixQosConfigHotReset(qosConf); err != nil {
		return err
	}

	levelName, field := splitKeys[1], splitKeys[2]
	if !bnapi.StringToIOType(levelName).IsValid() {
		return ErrNotSupportKey
	}

	var value int64
	paraConf := qosConf.FlowConf.Level[levelName]
	if field != "factor" {
		if value, err = parseQosArgsInt(args.Value); err != nil {
			return err
		}
	}

	switch field {
	case "mbps":
		paraConf.MBPS = value
	case "concurrency":
		paraConf.Concurrency = value
	case "bid_concurrency":
		paraConf.BidConcurrency = qos.FixQosBidConcurrency(levelName, int(value))
	case "factor":
		if paraConf.Factor, err = parseQosArgsFloat(args.Value); err != nil {
			return err
		}
	default:
		return ErrNotSupportKey
	}

	return s.reloadLevelQos(ctx, levelName, paraConf)
}

func (s *Service) reloadLevelQos(ctx context.Context, level string, levelConf qos.LevelFlowConfig) (err error) {
	ioType := bnapi.StringToIOType(level)
	disks := s.copyDiskStorages(ctx)

	for _, ds := range disks {
		ioQ, exist := ds.GetIoQos().GetQueueQos(bnapi.SetIoType(ctx, ioType))
		if !exist {
			return errcode.ErrInternal
		}
		ioQ.ResetLevelLimit(levelConf)
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
	if ret <= 0 || ret > 1 {
		return 0, ErrValueOutOfLimit
	}
	return ret, nil
}
