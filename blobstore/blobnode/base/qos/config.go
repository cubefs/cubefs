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
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultMaxBandwidthMBPS        = 1024
	defaultBackgroundBandwidthMBPS = 10
	defaultMaxWaitCount            = 1024
)

type Config struct {
	StatGetter      flow.StatGetter `json:"-"` // Identify: a io flow
	DiskViewer      iostat.IOViewer `json:"-"` // Identify: io viewer
	ReadQueueDepth  int             `json:"-"` // equal $queueDepth of io pool: The number of elements in the queue, must not zero
	WriteQueueDepth int             `json:"-"` // equal $queueDepth of io pool: The number of elements in the queue
	WriteChanQueCnt int             `json:"-"` // The number of chan queues, equal $chanCnt of write io pool
	MaxWaitCount    int             `json:"max_wait_count"`
	NormalMBPS      int64           `json:"normal_mbps"`
	BackgroundMBPS  int64           `json:"background_mbps"`
}

type ParaConfig struct {
	Bandwidth int64   `json:"bandwidth_MBPS"`
	Factor    float64 `json:"factor"`
}

type LevelConfig map[string]ParaConfig

func InitAndFixQosConfig(raw *Config) {
	defaulter.LessOrEqual(&raw.NormalMBPS, int64(defaultMaxBandwidthMBPS))
	defaulter.LessOrEqual(&raw.BackgroundMBPS, int64(defaultBackgroundBandwidthMBPS))
	defaulter.LessOrEqual(&raw.MaxWaitCount, defaultMaxWaitCount)

	if raw.BackgroundMBPS > raw.NormalMBPS {
		raw.BackgroundMBPS = raw.NormalMBPS // fix background
	}
}
