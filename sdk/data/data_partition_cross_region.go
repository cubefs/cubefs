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

package data

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/iputil"
	"github.com/chubaofs/chubaofs/util/log"
)

type RegionRankType uint8

func (t RegionRankType) String() string {
	switch t {
	case SameZoneRank:
		return "same-zone"
	case SameRegionRank:
		return "same-region"
	case CrossRegionRank:
		return "cross-region"
	case UnknownRegionRank:
		return "unknown-region"
	default:
	}
	return "undefined"
}

const (
	SameZoneRank RegionRankType = iota
	SameRegionRank
	CrossRegionRank
	UnknownRegionRank
)

const (
	pingTimeout         = 50 * time.Millisecond
	pingCount           = 3
	sameZoneTimeout     = 400 * time.Microsecond
	sameRegionTimeout   = 2 * time.Millisecond
	errCounterThreshold = 10
)

type CrossRegionMetrics struct {
	sync.RWMutex
	CrossRegionHosts map[RegionRankType][]string
	HostErrCounter   map[string]int
}

func (crossRegionMetrics *CrossRegionMetrics) String() string {
	if crossRegionMetrics == nil {
		return ""
	}
	return fmt.Sprintf("Host(%v) ErrCount(%v)", crossRegionMetrics.CrossRegionHosts, crossRegionMetrics.HostErrCounter)
}

func NewCrossRegionMetrics() *CrossRegionMetrics {
	return &CrossRegionMetrics{
		CrossRegionHosts: make(map[RegionRankType][]string, 0),
		HostErrCounter:   make(map[string]int, 0),
	}
}

func (w *Wrapper) initCrossRegionHostStatus(dp *DataPartition) {
	for _, host := range dp.Hosts {
		if _, ok := w.crossRegionHostLatency.Load(host); !ok {
			log.LogDebugf("initCrossRegionHostStatus: store host(%v)", host)
			w.crossRegionHostLatency.Store(host, time.Duration(0))
		}
	}
}

func (w *Wrapper) refreshCrossRegionHostStatus(pingHosts []string) {
	for _, host := range pingHosts {
		avgTime, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
		if err != nil {
			avgTime = time.Duration(0)
		}
		log.LogDebugf("refreshCrossRegionHostStatus: host(%v) ping time(%v) err(%v)", host, avgTime, err)
		w.crossRegionHostLatency.Store(host, avgTime)
	}
}

func (w *Wrapper) updateCrossRegionHostStatus() {
	defer w.wg.Done()
	w.getAllCrossRegionHostStatus()
	for {
		err := w.updateCrossRegionHostStatusWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("updateCrossRegionHostStatus: err(%v) try next update", err)
	}
}

func (w *Wrapper) updateCrossRegionHostStatusWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("updateCrossRegionHostStatus panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("updateCrossRegionHostStatus panic: err(%v)", r)
			handleUmpAlarm(w.clusterName, w.volName, "updateCrossRegionHostStatus", msg)
			err = errors.New(msg)
		}
	}()

	pingTicker := time.NewTicker(time.Minute)
	defer pingTicker.Stop()
	refreshTicker := time.NewTicker(24 * time.Hour)
	defer refreshTicker.Stop()

	for {
		select {
		case <-w.stopC:
			return
		case <-pingTicker.C:
			w.getUnknownCrossRegionHostStatus()
		case <-refreshTicker.C:
			w.getAllCrossRegionHostStatus()
		}
	}
}

func (w *Wrapper) getAllCrossRegionHostStatus() {
	if w.CrossRegionHATypeQuorum() {
		hostStatus := w.HostsStatus
		pingHosts := make([]string, 0)
		w.crossRegionHostLatency.Range(func(key, value interface{}) bool {
			host := key.(string)
			if _, exist := hostStatus[host]; !exist {
				log.LogInfof("getAllCrossRegionHostStatus: remove host(%v)", host)
				w.crossRegionHostLatency.Delete(host)
				return true
			}
			pingHosts = append(pingHosts, host)
			return true
		})
		log.LogDebugf("getAllCrossRegionHostStatus: ping host num(%v)", len(pingHosts))
		w.refreshCrossRegionHostStatus(pingHosts)
	}
}

func (w *Wrapper) getUnknownCrossRegionHostStatus() {
	if w.CrossRegionHATypeQuorum() {
		pingHosts := make([]string, 0)
		w.crossRegionHostLatency.Range(func(key, value interface{}) bool {
			host := key.(string)
			avgTime := value.(time.Duration)
			if avgTime == 0 {
				pingHosts = append(pingHosts, host)
			}
			return true
		})
		log.LogDebugf("getUnknownCrossRegionHostStatus: ping host num(%v)", len(pingHosts))
		w.refreshCrossRegionHostStatus(pingHosts)
	}
}

func (w *Wrapper) classifyCrossRegionHosts(hosts []string) (crossRegionHosts map[RegionRankType][]string) {
	crossRegionHosts = make(map[RegionRankType][]string)
	for _, host := range hosts {
		var pingTime = time.Duration(0)
		value, ok := w.crossRegionHostLatency.Load(host)
		if ok {
			pingTime = value.(time.Duration)
		}
		if pingTime <= time.Duration(0) {
			crossRegionHosts[UnknownRegionRank] = append(crossRegionHosts[UnknownRegionRank], host)
		} else if pingTime <= sameZoneTimeout {
			crossRegionHosts[SameZoneRank] = append(crossRegionHosts[SameZoneRank], host)
		} else if pingTime <= sameRegionTimeout {
			crossRegionHosts[SameRegionRank] = append(crossRegionHosts[SameRegionRank], host)
		} else {
			crossRegionHosts[CrossRegionRank] = append(crossRegionHosts[CrossRegionRank], host)
		}
	}
	return crossRegionHosts
}

func (dp *DataPartition) getNearestCrossRegionHost() string {
	dp.CrossRegionMetrics.RLock()
	defer dp.CrossRegionMetrics.RUnlock()
	for i := SameZoneRank; i <= UnknownRegionRank; i++ {
		if len(dp.CrossRegionMetrics.CrossRegionHosts[i]) > 0 {
			err, host := dp.getEpochReadHost(dp.CrossRegionMetrics.CrossRegionHosts[i])
			if err == nil {
				log.LogDebugf("getNearestCrossRegionHost: dp[%v] crossRegionMetrics[%v] get nearest host[%v] from rank[%v:%v]",
					dp, dp.CrossRegionMetrics, host, i, dp.CrossRegionMetrics.CrossRegionHosts[i])
				return host
			}
		}
	}
	return dp.GetLeaderAddr()
}

func (dp *DataPartition) getSortedCrossRegionHosts() (dpHosts []string) {
	dp.CrossRegionMetrics.RLock()
	for i := SameZoneRank; i <= UnknownRegionRank; i++ {
		dpHosts = append(dpHosts, dp.CrossRegionMetrics.CrossRegionHosts[i]...)
	}
	dp.CrossRegionMetrics.RUnlock()
	return dpHosts
}

func (dp *DataPartition) updateCrossRegionMetrics(addr string, netFail bool) {
	dp.CrossRegionMetrics.Lock()
	defer dp.CrossRegionMetrics.Unlock()
	if netFail {
		counter := dp.CrossRegionMetrics.HostErrCounter[addr]
		if counter+1 >= errCounterThreshold {
			dp.ClientWrapper.crossRegionHostLatency.Store(addr, time.Duration(0))
			dp.CrossRegionMetrics.adjustHostToUnknownRank(addr)
			log.LogWarnf("updateCrossRegionMetrics: degrade host(%v) errCounter(%v) dp(%v) crossRegionMetrics(%v)",
				addr, counter, dp, dp.CrossRegionMetrics)
		}
		dp.CrossRegionMetrics.HostErrCounter[addr]++
		log.LogDebugf("updateCrossRegionMetrics: host(%v) crossRegionMetrics(%v) dp(%v)", addr, dp.CrossRegionMetrics, dp)
	} else {
		delete(dp.CrossRegionMetrics.HostErrCounter, addr)
	}
}

func (crossRegionMetrics *CrossRegionMetrics) adjustHostToUnknownRank(host string) {
	exist := false
	for i := SameZoneRank; i <= CrossRegionRank; i++ {
		crossRegionMetrics.CrossRegionHosts[i], exist = containsAndDelete(crossRegionMetrics.CrossRegionHosts[i], host)
		if exist {
			crossRegionMetrics.CrossRegionHosts[UnknownRegionRank] = append(crossRegionMetrics.CrossRegionHosts[UnknownRegionRank], host)
			return
		}
	}
	return
}

func containsAndDelete(arr []string, element string) (newArr []string, ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}
	for i, e := range arr {
		if e == element {
			ok = true
			arr = append(arr[:i], arr[i+1:]...)
			break
		}
	}
	return arr, ok
}
