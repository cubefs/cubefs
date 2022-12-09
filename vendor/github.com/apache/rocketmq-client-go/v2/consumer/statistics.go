/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consumer

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type StatsManager struct {
	startOnce                     sync.Once
	closeOnce                     sync.Once
	topicAndGroupConsumeOKTPS     *statsItemSet
	topicAndGroupConsumeRT        *statsItemSet
	topicAndGroupConsumeFailedTPS *statsItemSet
	topicAndGroupPullTPS          *statsItemSet
	topicAndGroupPullRT           *statsItemSet
}

func NewStatsManager() *StatsManager {
	mgr := &StatsManager{}
	mgr.topicAndGroupConsumeOKTPS = newStatsItemSet("CONSUME_OK_TPS")
	mgr.topicAndGroupConsumeRT = newStatsItemSet("CONSUME_RT")
	mgr.topicAndGroupConsumeFailedTPS = newStatsItemSet("CONSUME_FAILED_TPS")
	mgr.topicAndGroupPullTPS = newStatsItemSet("PULL_TPS")
	mgr.topicAndGroupPullRT = newStatsItemSet("PULL_RT")
	return mgr
}

type ConsumeStatus struct {
	PullRT            float64
	PullTPS           float64
	ConsumeRT         float64
	ConsumeOKTPS      float64
	ConsumeFailedTPS  float64
	ConsumeFailedMsgs int64
}

func (mgr *StatsManager) increasePullRT(group, topic string, rt int64) {
	mgr.topicAndGroupPullRT.addValue(topic+"@"+group, rt, 1)
}

func (mgr *StatsManager) increasePullTPS(group, topic string, msgs int) {
	mgr.topicAndGroupPullTPS.addValue(topic+"@"+group, int64(msgs), 1)
}

func (mgr *StatsManager) increaseConsumeRT(group, topic string, rt int64) {
	mgr.topicAndGroupConsumeRT.addValue(topic+"@"+group, rt, 1)
}

func (mgr *StatsManager) increaseConsumeOKTPS(group, topic string, msgs int) {
	mgr.topicAndGroupConsumeOKTPS.addValue(topic+"@"+group, int64(msgs), 1)
}

func (mgr *StatsManager) increaseConsumeFailedTPS(group, topic string, msgs int) {
	mgr.topicAndGroupConsumeFailedTPS.addValue(topic+"@"+group, int64(msgs), 1)
}

func (mgr *StatsManager) GetConsumeStatus(group, topic string) ConsumeStatus {
	cs := ConsumeStatus{}
	ss := mgr.getPullRT(group, topic)
	cs.PullTPS = ss.tps

	ss = mgr.getPullTPS(group, topic)
	cs.PullTPS = ss.tps

	ss = mgr.getConsumeRT(group, topic)
	cs.ConsumeRT = ss.avgpt

	ss = mgr.getConsumeOKTPS(group, topic)
	cs.ConsumeOKTPS = ss.tps

	ss = mgr.getConsumeFailedTPS(group, topic)

	cs.ConsumeFailedTPS = ss.tps

	ss = mgr.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group)
	cs.ConsumeFailedMsgs = ss.sum
	return cs
}

func (mgr *StatsManager) ShutDownStat() {
	mgr.closeOnce.Do(func() {
		close(mgr.topicAndGroupConsumeOKTPS.closed)
		close(mgr.topicAndGroupConsumeRT.closed)
		close(mgr.topicAndGroupConsumeFailedTPS.closed)
		close(mgr.topicAndGroupPullTPS.closed)
		close(mgr.topicAndGroupPullRT.closed)
	})
}

func (mgr *StatsManager) getPullRT(group, topic string) statsSnapshot {
	return mgr.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group)
}

func (mgr *StatsManager) getPullTPS(group, topic string) statsSnapshot {
	return mgr.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group)
}

func (mgr *StatsManager) getConsumeRT(group, topic string) statsSnapshot {
	ss := mgr.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group)
	if ss.sum == 0 {
		return mgr.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group)
	}
	return ss
}

func (mgr *StatsManager) getConsumeOKTPS(group, topic string) statsSnapshot {
	return mgr.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group)
}

func (mgr *StatsManager) getConsumeFailedTPS(group, topic string) statsSnapshot {
	return mgr.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group)
}

func computeStatsData(csListLock *sync.Mutex, csList *list.List) statsSnapshot {
	csListLock.Lock()
	defer csListLock.Unlock()
	tps, avgpt, sum := 0.0, 0.0, int64(0)
	if csList.Len() > 0 {
		first := csList.Front().Value.(callSnapshot)
		last := csList.Back().Value.(callSnapshot)
		sum = last.value - first.value
		tps = float64(sum*1000.0) / float64(last.timestamp-first.timestamp)
		timesDiff := last.time - first.time
		if timesDiff > 0 {
			avgpt = float64(sum*1.0) / float64(timesDiff)
		}
	}
	return statsSnapshot{
		tps:   tps,
		avgpt: avgpt,
		sum:   sum,
	}
}

type statsItemSet struct {
	statsName      string
	statsItemTable sync.Map
	closed         chan struct{}
}

func newStatsItemSet(statsName string) *statsItemSet {
	sis := &statsItemSet{
		statsName: statsName,
		closed:    make(chan struct{}),
	}
	sis.init()
	return sis
}

func (sis *statsItemSet) init() {
	go primitive.WithRecover(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.samplingInSeconds()
			}
		}
	})

	go primitive.WithRecover(func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.samplingInMinutes()
			}
		}
	})

	go primitive.WithRecover(func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.samplingInHour()
			}
		}
	})

	go primitive.WithRecover(func() {
		time.Sleep(nextMinutesTime().Sub(time.Now()))
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.printAtMinutes()
			}
		}
	})

	go primitive.WithRecover(func() {
		time.Sleep(nextHourTime().Sub(time.Now()))
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.printAtHour()
			}
		}
	})

	go primitive.WithRecover(func() {
		time.Sleep(nextMonthTime().Sub(time.Now()))
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-sis.closed:
				return
			case <-ticker.C:
				sis.printAtDay()
			}
		}
	})
}

func (sis *statsItemSet) samplingInSeconds() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.samplingInSeconds()
		return true
	})
}

func (sis *statsItemSet) samplingInMinutes() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.samplingInMinutes()
		return true
	})
}

func (sis *statsItemSet) samplingInHour() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.samplingInHour()
		return true
	})
}

func (sis *statsItemSet) printAtMinutes() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.printAtMinutes()
		return true
	})
}

func (sis *statsItemSet) printAtHour() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.printAtHour()
		return true
	})
}

func (sis *statsItemSet) printAtDay() {
	sis.statsItemTable.Range(func(key, value interface{}) bool {
		si := value.(*statsItem)
		si.printAtDay()
		return true
	})
}

func (sis *statsItemSet) addValue(key string, incValue, incTimes int64) {
	si := sis.getAndCreateStateItem(key)
	atomic.AddInt64(&si.value, incValue)
	atomic.AddInt64(&si.times, incTimes)
}

func (sis *statsItemSet) getAndCreateStateItem(key string) *statsItem {
	if val, ok := sis.statsItemTable.Load(key); ok {
		return val.(*statsItem)
	} else {
		si := newStatsItem(sis.statsName, key)
		sis.statsItemTable.Store(key, si)
		return si
	}
}

func (sis *statsItemSet) getStatsDataInMinute(key string) statsSnapshot {
	if val, ok := sis.statsItemTable.Load(key); ok {
		si := val.(*statsItem)
		return si.getStatsDataInMinute()
	}
	return statsSnapshot{}
}

func (sis *statsItemSet) getStatsDataInHour(key string) statsSnapshot {
	if val, ok := sis.statsItemTable.Load(key); ok {
		si := val.(*statsItem)
		return si.getStatsDataInHour()
	}
	return statsSnapshot{}
}

func (sis *statsItemSet) getStatsDataInDay(key string) statsSnapshot {
	if val, ok := sis.statsItemTable.Load(key); ok {
		si := val.(*statsItem)
		return si.getStatsDataInDay()
	}
	return statsSnapshot{}
}

func (sis *statsItemSet) getStatsItem(key string) *statsItem {
	val, _ := sis.statsItemTable.Load(key)
	return val.(*statsItem)
}

type statsItem struct {
	value            int64
	times            int64
	csListMinute     *list.List
	csListHour       *list.List
	csListDay        *list.List
	statsName        string
	statsKey         string
	csListMinuteLock sync.Mutex
	csListHourLock   sync.Mutex
	csListDayLock    sync.Mutex
}

func (si *statsItem) getStatsDataInMinute() statsSnapshot {
	return computeStatsData(&si.csListMinuteLock, si.csListMinute)
}

func (si *statsItem) getStatsDataInHour() statsSnapshot {
	return computeStatsData(&si.csListHourLock, si.csListHour)
}

func (si *statsItem) getStatsDataInDay() statsSnapshot {
	return computeStatsData(&si.csListDayLock, si.csListDay)
}

func newStatsItem(statsName, statsKey string) *statsItem {
	return &statsItem{
		statsName:    statsName,
		statsKey:     statsKey,
		csListMinute: list.New(),
		csListHour:   list.New(),
		csListDay:    list.New(),
	}
}

func (si *statsItem) samplingInSeconds() {
	si.csListMinuteLock.Lock()
	defer si.csListMinuteLock.Unlock()
	si.csListMinute.PushBack(callSnapshot{
		timestamp: time.Now().Unix() * 1000,
		time:      atomic.LoadInt64(&si.times),
		value:     atomic.LoadInt64(&si.value),
	})
	if si.csListMinute.Len() > 7 {
		si.csListMinute.Remove(si.csListMinute.Front())
	}
}

func (si *statsItem) samplingInMinutes() {
	si.csListHourLock.Lock()
	defer si.csListHourLock.Unlock()
	si.csListHour.PushBack(callSnapshot{
		timestamp: time.Now().Unix() * 1000,
		time:      atomic.LoadInt64(&si.times),
		value:     atomic.LoadInt64(&si.value),
	})
	if si.csListHour.Len() > 7 {
		si.csListHour.Remove(si.csListHour.Front())
	}
}

func (si *statsItem) samplingInHour() {
	si.csListDayLock.Lock()
	defer si.csListDayLock.Unlock()
	si.csListDay.PushBack(callSnapshot{
		timestamp: time.Now().Unix() * 1000,
		time:      atomic.LoadInt64(&si.times),
		value:     atomic.LoadInt64(&si.value),
	})
	if si.csListDay.Len() > 25 {
		si.csListHour.Remove(si.csListDay.Front())
	}
}

func (si *statsItem) printAtMinutes() {
	ss := computeStatsData(&si.csListMinuteLock, si.csListMinute)
	rlog.Info("Stats In One Minute, SUM: %d TPS:  AVGPT: %.2f", map[string]interface{}{
		"statsName": si.statsName,
		"statsKey":  si.statsKey,
		"SUM":       ss.sum,
		"TPS":       fmt.Sprintf("%.2f", ss.tps),
		"AVGPT":     ss.avgpt,
	})
}

func (si *statsItem) printAtHour() {
	ss := computeStatsData(&si.csListHourLock, si.csListHour)
	rlog.Info("Stats In One Hour, SUM: %d TPS:  AVGPT: %.2f", map[string]interface{}{
		"statsName": si.statsName,
		"statsKey":  si.statsKey,
		"SUM":       ss.sum,
		"TPS":       fmt.Sprintf("%.2f", ss.tps),
		"AVGPT":     ss.avgpt,
	})
}

func (si *statsItem) printAtDay() {
	ss := computeStatsData(&si.csListDayLock, si.csListDay)
	rlog.Info("Stats In One Day, SUM: %d TPS:  AVGPT: %.2f", map[string]interface{}{
		"statsName": si.statsName,
		"statsKey":  si.statsKey,
		"SUM":       ss.sum,
		"TPS":       fmt.Sprintf("%.2f", ss.tps),
		"AVGPT":     ss.avgpt,
	})
}

func nextMinutesTime() time.Time {
	now := time.Now()
	m, _ := time.ParseDuration("1m")
	return now.Add(m)
}

func nextHourTime() time.Time {
	now := time.Now()
	m, _ := time.ParseDuration("1h")
	return now.Add(m)
}

func nextMonthTime() time.Time {
	now := time.Now()
	return now.AddDate(0, 1, 0)
}

type callSnapshot struct {
	timestamp int64
	time      int64
	value     int64
}

type statsSnapshot struct {
	sum   int64
	tps   float64
	avgpt float64
}
