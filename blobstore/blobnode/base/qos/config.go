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
	defaultMaxIops        = 768
	defaultMaxMBps        = 1024
	defaultIntervalMs     = 500
	defaultIdleFactor     = 0.5
	defaultBidConcurrency = 1
)

var (
	ErrQosWrongConfig = errors.New("wrong qos config")

	defaultConfs = map[string]LevelFlowConfig{
		bnapi.ReadIO.String():       {Concurrency: 512, MBPS: 300, Factor: 1.0, BidConcurrency: 32},
		bnapi.WriteIO.String():      {Concurrency: 128, MBPS: 120, Factor: 1.0, BidConcurrency: 1},
		bnapi.DeleteIO.String():     {Concurrency: 128, MBPS: 60, Factor: 0.8, BidConcurrency: 1},
		bnapi.BackgroundIO.String(): {Concurrency: 64, MBPS: 20, Factor: 0.6, BidConcurrency: 1},
	}
)

type Config struct {
	StatGetter flow.StatGetter `json:"-"` // Identify: an io flow
	DiskViewer iostat.IOViewer `json:"-"` // Identify: io viewer
	FlowConf   FlowConfig      `json:"flow_conf"`
}

type CommonDiskConfig struct {
	DiskIops         int64   `json:"disk_iops"`
	DiskBandwidthMB  int64   `json:"disk_bandwidth_mb"`  // disk total bandwidth MB/s
	UpdateIntervalMs int64   `json:"update_interval_ms"` // dynamic update limiter interval
	IdleFactor       float64 `json:"idle_factor"`        // disk is idle, raise rate factor
}

type FlowConfig struct {
	CommonDiskConfig
	Level map[string]LevelFlowConfig `json:"level"` // every single limiter config
}

type LevelFlowConfig struct {
	BidConcurrency int     `json:"bid_concurrency"` // limit bid concurrence
	Concurrency    int64   `json:"concurrency"`     // limit io type concurrence
	MBPS           int64   `json:"mbps"`            // limit MBPS concurrence
	Factor         float64 `json:"factor"`          // factor (0.0, 1.0]
}

func FixQosConfigOnInit(conf *Config) error {
	return initAndFixQosConfig(conf, true)
}

func FixQosConfigHotReset(conf *Config) error {
	return initAndFixQosConfig(conf, false)
}

func initAndFixQosConfig(conf *Config, fillEmpty bool) error {
	if conf.FlowConf.IdleFactor > 1 {
		return ErrQosWrongConfig
	}

	if fillEmpty {
		defaulter.IntegerLessOrEqual(&conf.FlowConf.DiskIops, defaultMaxIops)
		defaulter.IntegerLessOrEqual(&conf.FlowConf.DiskBandwidthMB, defaultMaxMBps)
		defaulter.IntegerLessOrEqual(&conf.FlowConf.UpdateIntervalMs, defaultIntervalMs)
		defaulter.FloatLessOrEqual(&conf.FlowConf.IdleFactor, defaultIdleFactor)
	}

	for ioTypeStr := range conf.FlowConf.Level {
		if tp := bnapi.StringToIOType(ioTypeStr); !tp.IsValid() {
			return ErrQosWrongConfig
		}
	}

	// if it is nil, use default
	if conf.FlowConf.Level == nil {
		conf.FlowConf.Level = make(map[string]LevelFlowConfig)
	}

	// check each type, if it is not configure, use default
	levelConf := make(map[string]LevelFlowConfig, len(defaultConfs))
	for ioTypeStr, defaultVal := range defaultConfs {
		if tp := bnapi.StringToIOType(ioTypeStr); !tp.IsValid() {
			return ErrQosWrongConfig
		}

		// if not exists, fill use default
		userConfig, exists := conf.FlowConf.Level[ioTypeStr]
		if !exists {
			if fillEmpty {
				levelConf[ioTypeStr] = defaultVal
			}
			continue
		}

		// special case: only ioType is read, can configure bid concurrency; other is 1
		if ioTypeStr != bnapi.ReadIO.String() {
			userConfig.BidConcurrency = defaultBidConcurrency
		}
		// if exist user config, use it
		fixedConfig, err := fixLevelFlowConfig(userConfig, defaultVal, fillEmpty)
		if err != nil {
			return ErrQosWrongConfig
		}
		levelConf[ioTypeStr] = fixedConfig
	}

	conf.FlowConf.Level = levelConf
	return nil
}

func fixLevelFlowConfig(conf, defaultVal LevelFlowConfig, fillEmpty bool) (LevelFlowConfig, error) {
	if conf.Factor > 1 {
		return LevelFlowConfig{}, ErrQosWrongConfig
	}

	if fillEmpty {
		defaulter.IntegerLessOrEqual(&conf.Concurrency, defaultVal.Concurrency)
		defaulter.IntegerLessOrEqual(&conf.MBPS, defaultVal.MBPS)
		defaulter.IntegerLessOrEqual(&conf.BidConcurrency, defaultVal.BidConcurrency)
		defaulter.FloatLessOrEqual(&conf.Factor, defaultVal.Factor)
	}
	return conf, nil
}

// config the qos for single/per io type
type perIOQosConfig struct {
	CommonDiskConfig
	LevelFlowConfig
	lck sync.Mutex
}

func (t *perIOQosConfig) resetDisk(conf CommonDiskConfig) {
	t.lck.Lock()
	defer t.lck.Unlock()

	if conf.DiskBandwidthMB > 0 {
		t.CommonDiskConfig.DiskBandwidthMB = conf.DiskBandwidthMB
	}
	if conf.IdleFactor > 0 && conf.IdleFactor <= 1 {
		t.CommonDiskConfig.IdleFactor = conf.IdleFactor
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
	if conf.Factor > 0 && conf.Factor <= 1 {
		t.LevelFlowConfig.Factor = conf.Factor
	}
}
