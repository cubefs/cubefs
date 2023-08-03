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
	"sync"

	"github.com/dustin/go-humanize"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

const (
	MaxIops      = 1000 * 1000 // 1000k
	MaxBandwidth = 2 << 30     // 1GB/s
	defaultFator = 0.9         // limit threshold 90%
)

type Config struct {
	DiskBandwidthMBPS int64           `json:"disk_bandwidth_MBPS"`
	LevelConfigs      LevelConfig     `json:"-"`
	DiskViewer        iostat.IOViewer `json:"-"`
	StatGetter        flow.StatGetter `json:"-"`
	BackgroundMBPS    int64           `json:"background_MBPS"`
	ReadQueueLen      int             `json:"-"`
	WriteQueueLen     int             `json:"-"`
}

type ParaConfig struct {
	Bandwidth int64   `json:"bandwidth_MBPS"`
	Factor    float64 `json:"factor"`
}

type Threshold struct {
	sync.RWMutex
	ParaConfig
	DiskBandwidth int64
}

type LevelConfig map[string]ParaConfig

func (t *Threshold) reset(level string, c Config) {
	t.Lock()
	para := c.LevelConfigs[level]
	para.Bandwidth *= humanize.MiByte
	c.DiskBandwidthMBPS *= humanize.MiByte
	t.ParaConfig = para
	t.DiskBandwidth = c.DiskBandwidthMBPS
	t.Unlock()
}

func InitAndFixParaConfig(raw ParaConfig) (para ParaConfig, err error) {
	para = raw

	if para.Bandwidth < 0 || para.Factor < 0 {
		return para, ErrWrongConfig
	}
	if para.Bandwidth == 0 {
		para.Bandwidth = MaxBandwidth
	}
	if para.Factor == 0 || para.Factor > 1 {
		para.Factor = 1
	}

	return para, nil
}

func InitAndFixQosConfig(raw *Config) {
	if raw.BackgroundMBPS > raw.DiskBandwidthMBPS {
		raw.BackgroundMBPS = raw.DiskBandwidthMBPS
	}

	raw.LevelConfigs = LevelConfig{
		blobnode.NormalIO.String(): ParaConfig{
			Bandwidth: raw.DiskBandwidthMBPS,
			Factor:    defaultFator,
		},
		blobnode.BackgroundIO.String(): ParaConfig{
			Bandwidth: raw.BackgroundMBPS,
			Factor:    defaultFator,
		},
	}
}

func initConfig(conf *Config) (err error) {
	levelConf := LevelConfig{}
	for l, para := range conf.LevelConfigs {
		if !priority.IsValidPriName(l) {
			return ErrWrongConfig
		}

		para, err = InitAndFixParaConfig(para)
		if err != nil {
			return ErrWrongConfig
		}

		levelConf[l] = para
	}

	conf.LevelConfigs = levelConf

	return nil
}
