// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"regexp"

	"github.com/cubefs/cubefs/proto"
	"golang.org/x/time/rate"
)

const (
	configListen                       = proto.ListenPort
	configMasterAddr                   = proto.MasterAddr
	configSimpleQueueInitCapacityStr   = "simpleQueueInitCapacity"
	configScanCheckIntervalStr         = "scanCheckInterval"
	configLcScanRoutineNumPerTaskStr   = "lcScanRoutineNumPerTask"
	configLcScanLimitPerSecondStr      = "lcScanLimitPerSecond"
	configSnapshotRoutineNumPerTaskStr = "snapshotRoutineNumPerTask"
	configLcNodeTaskCountLimit         = "lcNodeTaskCountLimit"
	configDelayDelMinute               = "delayDelMinute"
	configUseCreateTime                = "useCreateTime"
)

// Default of configuration value
const (
	defaultListen                    = "80"
	ModuleName                       = "lcNode"
	defaultReadDirLimit              = 1000
	defaultScanCheckInterval         = 60
	defaultLcScanRoutineNumPerTask   = 20
	maxLcScanRoutineNumPerTask       = 500
	defaultLcScanLimitPerSecond      = rate.Inf
	defaultLcScanLimitBurst          = 1000
	defaultUnboundedChanInitCapacity = 10000
	defaultSimpleQueueInitCapacity   = 1000000
	defaultLcNodeTaskCountLimit      = 1
	maxLcNodeTaskCountLimit          = 20
	defaultDelayDelMinute            = 1440           // default retention min(1 day) of old eks after migration
	MaxSizePutOnce                   = int64(1) << 23 // 8MB
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen              = regexp.MustCompile(`^(\d)+$`)
	simpleQueueInitCapacity   int
	scanCheckInterval         int64
	lcScanRoutineNumPerTask   int
	lcScanLimitPerSecond      rate.Limit
	snapshotRoutineNumPerTask int
	lcNodeTaskCountLimit      int
	maxDirChanNum             = 1000000
	delayDelMinute            uint64
	useCreateTime             bool
)
