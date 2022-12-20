// Copyright 2018 The CubeFS Authors.
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

package flashnode

import (
	"github.com/cubefs/cubefs/util/log"
	"time"
)

func (f *FlashNode) startUpdateNodeInfo() {
	flashInfoTicker := time.NewTicker(UpdateRateLimitInfoIntervalSec * time.Second)
	defer func() {
		flashInfoTicker.Stop()
	}()
	// call once on init before first tick
	f.updateFlashNodeBaseInfo()
	for {
		select {
		case <-f.stopCh:
			log.LogInfo("flashNode UpdateNodeInfo goroutine stopped")
			return
		case <-flashInfoTicker.C:
			f.updateFlashNodeBaseInfo()
		}
	}
}

// updateFlashNodeBaseInfo
// 1. set tmpfs ratio
// 2. set rate limit info
// todo: add rate limit in cache read
func (f *FlashNode) updateFlashNodeBaseInfo() {
	_, err := masterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogErrorf("[updateFlashNodeBaseInfo] %s", err.Error())
		return
	}
}
