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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
	"reflect"
	"time"
)

func (f *FlashNode) startUpdateScheduler() {
	limitTicker := time.NewTicker(UpdateRateLimitInfoInterval)
	defer func() {
		limitTicker.Stop()
	}()
	f.updateFlashNodeRateLimit()
	for {
		select {
		case <-f.stopCh:
			log.LogInfo("flashNode startUpdateScheduler goroutine stopped")
			return
		case <-limitTicker.C:
			f.updateFlashNodeRateLimit()
		}
	}
}

func (f *FlashNode) updateFlashNodeRateLimit() {
	limitInfo, err := masterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogWarnf("[updateRateLimitInfo] get limit info err: %s", err.Error())
		return
	}
	f.updateZoneLimiter(limitInfo)
	f.updateZoneVolLimiter(limitInfo)
}

// updateZoneLimiter update limiter for zone
func (f *FlashNode) updateZoneLimiter(limitInfo *proto.LimitInfo) {
	r, ok := limitInfo.FlashNodeLimitMap[f.zoneName]
	if !ok {
		r, ok = limitInfo.FlashNodeLimitMap[""]
	}
	if !ok {
		f.nodeLimit = 0
		f.nodeLimiter.SetLimit(rate.Inf)
		return
	}
	if r == 0 && f.nodeLimit == 0 {
		return
	}
	if f.nodeLimit != r {
		f.nodeLimit = r
		l := rate.Inf
		if r > 0 {
			l = rate.Limit(r)
		}
		f.nodeLimiter.SetLimit(l)
	}
}

func (f *FlashNode) updateZoneVolLimiter(limitInfo *proto.LimitInfo) {
	volLimitMap, ok := limitInfo.FlashNodeVolLimitMap[f.zoneName]
	if !ok {
		volLimitMap, ok = limitInfo.FlashNodeVolLimitMap[""]
	}
	if !ok {
		f.volLimitMap = make(map[string]uint64)
		f.volLimiterMap = make(map[string]*rate.Limiter)
		return
	}
	if !reflect.DeepEqual(f.volLimitMap, volLimitMap) {
		f.volLimitMap = volLimitMap
		tmpVolLimiterMap := make(map[string]*rate.Limiter)
		for vol, r := range volLimitMap {
			l := rate.Limit(r)
			tmpVolLimiterMap[vol] = rate.NewLimiter(l, DefaultBurst)
		}
		f.volLimiterMap = tmpVolLimiterMap
	}
}
