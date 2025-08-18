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

package qos

import (
	"errors"
	"sync"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultMaxIops        = 2000
	defaultMaxMBps        = 250
	defaultIntervalMs     = 500
	defaultIdleFactor     = 0.2
	defaultBidConcurrency = 1
)

var (
	ErrQosWrongConfig = errors.New("wrong qos config")

	defaultConfs = LevelConfigMap{
		bnapi.ReadIO.String():       {Concurrency: 64, MBPS: 200, BusyFactor: 1.0, BidConcurrency: 32, IdleFactor: 1.0},
		bnapi.WriteIO.String():      {Concurrency: 64, MBPS: 120, BusyFactor: 1.0, BidConcurrency: 1, IdleFactor: 1.2},
		bnapi.DeleteIO.String():     {Concurrency: 32, MBPS: 60, BusyFactor: 0.8, BidConcurrency: 1, IdleFactor: 1.25},
		bnapi.BackgroundIO.String(): {Concurrency: 32, MBPS: 20, BusyFactor: 0.5, BidConcurrency: 1, IdleFactor: 3.0},
	}
)

type Config struct {
	StatGetter flow.StatGetter `json:"-"` // Identify: an io flow
	DiskViewer iostat.IOViewer `json:"-"` // Identify: io viewer
	FlowConfig
}

type LevelConfigMap map[string]LevelFlowConfig

type CommonDiskConfig struct {
	DiskIops         int64   `json:"disk_iops"`
	DiskBandwidthMB  int64   `json:"disk_bandwidth_mb"`  // disk total bandwidth MB/s
	UpdateIntervalMs int64   `json:"update_interval_ms"` // dynamic update limiter interval
	DiskIdleFactor   float64 `json:"disk_idle_factor"`   // disk is idle, raise rate factor
}

type FlowConfig struct {
	CommonDiskConfig
	Level LevelConfigMap `json:"level"` // every single limiter config
}

type LevelFlowConfig struct {
	BidConcurrency int64   `json:"bid_concurrency"` // limit bid concurrence
	Concurrency    int64   `json:"concurrency"`     // limit io type concurrence
	MBPS           int64   `json:"mbps"`            // limit MBPS concurrence
	BusyFactor     float64 `json:"busy_factor"`     // reduce rate factor (0.0, 1.0]
	IdleFactor     float64 `json:"idle_factor"`     // idle factor [1.0, xx)
}

func FixQosConfigOnInit(conf *Config) error {
	return initAndFixQosConfig(conf, true)
}

func FixQosConfigHotReset(conf *Config) error {
	return initAndFixQosConfig(conf, false)
}

// FixQosBidConcurrency special case: only ioType is read, can configure bid concurrency; other is 1
func FixQosBidConcurrency(ioType string, concurrency int64) int64 {
	if ioType != bnapi.ReadIO.String() {
		return defaultBidConcurrency
	}
	return concurrency
}

func initAndFixQosConfig(conf *Config, fillEmpty bool) error {
	if conf.DiskIdleFactor > 1 {
		return ErrQosWrongConfig
	}

	if fillEmpty {
		defaulter.IntegerLessOrEqual(&conf.DiskIops, defaultMaxIops)
		defaulter.IntegerLessOrEqual(&conf.DiskBandwidthMB, defaultMaxMBps)
		defaulter.IntegerLessOrEqual(&conf.UpdateIntervalMs, defaultIntervalMs)
		defaulter.FloatLessOrEqual(&conf.DiskIdleFactor, defaultIdleFactor)
	}

	for ioTypeStr := range conf.Level {
		if tp := bnapi.StringToIOType(ioTypeStr); !tp.IsValid() {
			return ErrQosWrongConfig
		}
	}

	// if it is nil, use default
	if conf.Level == nil {
		conf.Level = make(LevelConfigMap)
	}

	// check each type, if it is not configure, use default
	levelConf := make(LevelConfigMap, len(defaultConfs))
	for ioTypeStr, defaultVal := range defaultConfs {
		if tp := bnapi.StringToIOType(ioTypeStr); !tp.IsValid() {
			return ErrQosWrongConfig
		}

		// if not exists, fill use default
		userConfig, exists := conf.Level[ioTypeStr]
		if !exists {
			if fillEmpty {
				levelConf[ioTypeStr] = defaultVal
			}
			continue
		}

		// special case: only ioType is read, can configure bid concurrency; other is 1
		userConfig.BidConcurrency = FixQosBidConcurrency(ioTypeStr, userConfig.BidConcurrency)
		// if exist user config, use it
		fixedConfig, err := fixLevelFlowConfig(userConfig, defaultVal, fillEmpty)
		if err != nil {
			return ErrQosWrongConfig
		}
		levelConf[ioTypeStr] = fixedConfig
	}

	conf.Level = levelConf
	return nil
}

func fixLevelFlowConfig(conf, defaultVal LevelFlowConfig, fillEmpty bool) (LevelFlowConfig, error) {
	if fillEmpty {
		defaulter.IntegerLessOrEqual(&conf.Concurrency, defaultVal.Concurrency)
		defaulter.IntegerLessOrEqual(&conf.MBPS, defaultVal.MBPS)
		defaulter.IntegerLessOrEqual(&conf.BidConcurrency, defaultVal.BidConcurrency)
		defaulter.FloatLessOrEqual(&conf.BusyFactor, defaultVal.BusyFactor)
		defaulter.FloatLessOrEqual(&conf.IdleFactor, defaultVal.IdleFactor)
	}

	if conf.BusyFactor > 1 || (conf.IdleFactor != 0 && conf.IdleFactor < 1) {
		return LevelFlowConfig{}, ErrQosWrongConfig
	}

	return conf, nil
}

// config the qos for single/per io type
type perIOQosConfig struct {
	LevelFlowConfig
	bnapi.IOType
	lck sync.Mutex
}

func (c *CommonDiskConfig) resetDisk(conf CommonDiskConfig) {
	if conf.DiskBandwidthMB > 0 {
		c.DiskBandwidthMB = conf.DiskBandwidthMB
	}
	if conf.DiskIdleFactor > 0 && conf.DiskIdleFactor <= 1 {
		c.DiskIdleFactor = conf.DiskIdleFactor
	}
	if conf.DiskIops > 0 {
		c.DiskIops = conf.DiskIops
	}
}

func (t *perIOQosConfig) resetLevel(conf LevelFlowConfig) {
	t.lck.Lock()
	defer t.lck.Unlock()

	// if the config of user hot modify, is not zero, then use it
	if conf.BidConcurrency > 0 {
		t.LevelFlowConfig.BidConcurrency = conf.BidConcurrency
	}
	if conf.Concurrency > 0 {
		t.LevelFlowConfig.Concurrency = conf.Concurrency
	}
	if conf.MBPS > 0 {
		t.LevelFlowConfig.MBPS = conf.MBPS
	}
	if conf.BusyFactor > 0 && conf.BusyFactor <= 1 {
		t.LevelFlowConfig.BusyFactor = conf.BusyFactor
	}
	if conf.IdleFactor > 0 {
		t.LevelFlowConfig.IdleFactor = conf.IdleFactor
	}
}
