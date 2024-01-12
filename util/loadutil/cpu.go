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

package loadutil

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/cpu"

	"github.com/cubefs/cubefs/util/log"
)

func GetCpuUtilPercent(sampleDuration time.Duration) (used float64, err error) {
	utils, err := cpu.Percent(sampleDuration, false)
	if err != nil {
		log.LogErrorf("[GetCpuUtilPercent] err: %v", err.Error())
		return
	}
	if utils == nil {
		err = fmt.Errorf("got nil result")
		log.LogErrorf("[GetCpuUtilPercent] err: %v", err.Error())
		return
	}
	if len(utils) == 0 {
		err = fmt.Errorf("got result len is 0")
		log.LogErrorf("[GetCpuUtilPercent] err: %v", err.Error())
		return
	}

	used = utils[0]
	return
}
