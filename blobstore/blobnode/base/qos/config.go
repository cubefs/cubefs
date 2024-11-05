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
	defaultReadBandwidthMBPS       = 200
	defaultWriteBandwidthMBPS      = 160
	defaultBackgroundBandwidthMBPS = 20
	defaultDiscardPercent          = 50
)

type Config struct {
	StatGetter      flow.StatGetter `json:"-"` // Identify: a io flow
	DiskViewer      iostat.IOViewer `json:"-"` // Identify: io viewer
	ReadQueueDepth  int32           `json:"-"` // equal $queueDepth of io pool: The number of elements in the queue, must not zero
	WriteQueueDepth int32           `json:"-"` // equal $queueDepth of io pool: The number of elements in the queue
	WriteChanQueCnt int32           `json:"-"` // The number of chan queues, equal $chanCnt of write io pool
	ReadMBPS        int64           `json:"read_mbps"`
	WriteMBPS       int64           `json:"write_mbps"`
	BackgroundMBPS  int64           `json:"background_mbps"`
	ReadDiscard     int32           `json:"read_discard"`
	WriteDiscard    int32           `json:"write_discard"`
}

type ParaConfig struct {
	Bandwidth int64   `json:"bandwidth_MBPS"`
	Factor    float64 `json:"factor"`
}

type LevelConfig map[string]ParaConfig

func InitAndFixQosConfig(raw *Config) {
	defaulter.LessOrEqual(&raw.ReadMBPS, int64(defaultReadBandwidthMBPS))
	defaulter.LessOrEqual(&raw.WriteMBPS, int64(defaultWriteBandwidthMBPS))
	defaulter.LessOrEqual(&raw.BackgroundMBPS, int64(defaultBackgroundBandwidthMBPS))
	defaulter.LessOrEqual(&raw.ReadDiscard, int32(defaultDiscardPercent))
	defaulter.LessOrEqual(&raw.WriteDiscard, int32(defaultDiscardPercent))

	// fix background, it should be the minimum
	raw.BackgroundMBPS = fixBackgroundMBPS(raw.BackgroundMBPS, raw.WriteMBPS, raw.ReadMBPS)
}

func fixBackgroundMBPS(background, writeMBPS, readMBPS int64) int64 {
	return min(background, min(writeMBPS, readMBPS))
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
