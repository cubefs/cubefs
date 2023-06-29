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
	configListen                     = proto.ListenPort
	configMasterAddr                 = proto.MasterAddr
	configBatchExpirationGetNumStr   = "batchExpirationGetNum"
	configScanCheckIntervalStr       = "scanCheckInterval"
	configLcScanRoutineNumPerTaskStr = "lcScanRoutineNumPerTask"
	configLcScanLimitPerSecondStr    = "lcScanLimitPerSecond"

	configSnapshotRoutineNumPerTaskStr = "snapshotRoutineNumPerTask"
)

// Default of configuration value
const (
	defaultListen                  = "80"
	ModuleName                     = "lcNode"
	defaultBatchExpirationGetNum   = 100
	maxBatchExpirationGetNum       = 10000
	defaultScanCheckInterval       = 60
	defaultLcScanRoutineNumPerTask = 100
	defaultLcScanLimitPerSecond    = rate.Inf
	defaultLcScanLimitBurst        = 1000

	maxLcScanRoutineNumPerTask = 5000
	maxDirChanNum              = 1000000
	defaultReadDirLimit        = 1000

	defaultMasterIntervalToCheckHeartbeat = 6
	noHeartBeatTimes                      = 3 // number of times that no heartbeat reported
	defaultLcNodeTimeOutSec               = noHeartBeatTimes * defaultMasterIntervalToCheckHeartbeat
	defaultIntervalToCheckRegister        = 2 * defaultLcNodeTimeOutSec

	defaultUnboundedChanInitCapacity = 10000
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen            = regexp.MustCompile("^(\\d)+$")
	batchExpirationGetNum   int
	scanCheckInterval       int64
	lcScanRoutineNumPerTask int
	lcScanLimitPerSecond    rate.Limit

	snapshotRoutineNumPerTask int
)
