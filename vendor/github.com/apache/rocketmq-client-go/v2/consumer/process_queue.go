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
	"strconv"
	"sync"

	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	gods_util "github.com/emirpasic/gods/utils"
	"go.uber.org/atomic"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

const (
	_RebalanceLockMaxTime = 30 * time.Second
	_RebalanceInterval    = 20 * time.Second
	_PullMaxIdleTime      = 120 * time.Second
)

type processQueue struct {
	cachedMsgCount             *atomic.Int64
	cachedMsgSize              *atomic.Int64
	tryUnlockTimes             int64
	queueOffsetMax             int64
	msgAccCnt                  int64
	msgCache                   *treemap.Map
	mutex                      sync.RWMutex
	consumeLock                sync.Mutex
	consumingMsgOrderlyTreeMap *treemap.Map
	dropped                    *atomic.Bool
	lastPullTime               atomic.Value
	lastConsumeTime            atomic.Value
	locked                     *atomic.Bool
	lastLockTime               atomic.Value
	consuming                  bool
	lockConsume                sync.Mutex
	msgCh                      chan []*primitive.MessageExt
	order                      bool
	closeChanOnce              *sync.Once
	closeChan                  chan struct{}
}

func newProcessQueue(order bool) *processQueue {
	consumingMsgOrderlyTreeMap := treemap.NewWith(gods_util.Int64Comparator)

	lastConsumeTime := atomic.Value{}
	lastConsumeTime.Store(time.Now())

	lastLockTime := atomic.Value{}
	lastLockTime.Store(time.Now())

	lastPullTime := atomic.Value{}
	lastPullTime.Store(time.Now())

	pq := &processQueue{
		cachedMsgCount:             atomic.NewInt64(0),
		cachedMsgSize:              atomic.NewInt64(0),
		msgCache:                   treemap.NewWith(utils.Int64Comparator),
		lastPullTime:               lastPullTime,
		lastConsumeTime:            lastConsumeTime,
		lastLockTime:               lastLockTime,
		msgCh:                      make(chan []*primitive.MessageExt, 32),
		consumingMsgOrderlyTreeMap: consumingMsgOrderlyTreeMap,
		order:                      order,
		closeChanOnce:              &sync.Once{},
		closeChan:                  make(chan struct{}),
		locked:                     atomic.NewBool(false),
		dropped:                    atomic.NewBool(false),
	}
	return pq
}

func (pq *processQueue) putMessage(messages ...*primitive.MessageExt) {
	if len(messages) == 0 {
		return
	}
	pq.mutex.Lock()
	if pq.IsDroppd() {
		pq.mutex.Unlock()
		return
	}
	if !pq.order {
		select {
		case <-pq.closeChan:
			return
		case pq.msgCh <- messages:
		}
	}
	validMessageCount := 0
	for idx := range messages {
		msg := messages[idx]
		_, found := pq.msgCache.Get(msg.QueueOffset)
		if found {
			continue
		}
		_, found = pq.consumingMsgOrderlyTreeMap.Get(msg.QueueOffset)
		if found {
			continue
		}
		pq.msgCache.Put(msg.QueueOffset, msg)
		validMessageCount++
		pq.queueOffsetMax = msg.QueueOffset

		pq.cachedMsgSize.Add(int64(len(msg.Body)))
	}

	pq.cachedMsgCount.Add(int64(validMessageCount))
	pq.mutex.Unlock()

	if pq.cachedMsgCount.Load() > 0 && !pq.consuming {
		pq.consuming = true
	}

	msg := messages[len(messages)-1]
	maxOffset, err := strconv.ParseInt(msg.GetProperty(primitive.PropertyMaxOffset), 10, 64)
	if err != nil {
		acc := maxOffset - msg.QueueOffset
		if acc > 0 {
			pq.msgAccCnt = acc
		}
	}
}

func (pq *processQueue) WithLock(lock bool) {
	pq.locked.Store(lock)
}

func (pq *processQueue) IsLock() bool {
	return pq.locked.Load()
}

func (pq *processQueue) WithDropped(dropped bool) {
	pq.dropped.Store(dropped)
	pq.closeChanOnce.Do(func() {
		close(pq.closeChan)
	})
}

func (pq *processQueue) IsDroppd() bool {
	return pq.dropped.Load()
}

func (pq *processQueue) UpdateLastConsumeTime() {
	pq.lastConsumeTime.Store(time.Now())
}

func (pq *processQueue) LastConsumeTime() time.Time {
	return pq.lastConsumeTime.Load().(time.Time)
}

func (pq *processQueue) UpdateLastLockTime() {
	pq.lastLockTime.Store(time.Now())
}

func (pq *processQueue) LastLockTime() time.Time {
	return pq.lastLockTime.Load().(time.Time)
}

func (pq *processQueue) LastPullTime() time.Time {
	return pq.lastPullTime.Load().(time.Time)
}

func (pq *processQueue) UpdateLastPullTime() {
	pq.lastPullTime.Store(time.Now())
}

func (pq *processQueue) makeMessageToCosumeAgain(messages ...*primitive.MessageExt) {
	pq.mutex.Lock()
	for _, msg := range messages {
		pq.consumingMsgOrderlyTreeMap.Remove(msg.QueueOffset)
		pq.msgCache.Put(msg.QueueOffset, msg)
	}

	pq.mutex.Unlock()
}

func (pq *processQueue) removeMessage(messages ...*primitive.MessageExt) int64 {
	result := int64(-1)
	pq.mutex.Lock()
	pq.UpdateLastConsumeTime()
	if !pq.msgCache.Empty() {
		result = pq.queueOffsetMax + 1
		removedCount := 0
		for idx := range messages {
			msg := messages[idx]
			_, found := pq.msgCache.Get(msg.QueueOffset)
			if !found {
				continue
			}

			pq.msgCache.Remove(msg.QueueOffset)
			removedCount++

			pq.cachedMsgSize.Sub(int64(len(msg.Body)))
		}

		pq.cachedMsgCount.Sub(int64(removedCount))
	}
	if !pq.msgCache.Empty() {
		first, _ := pq.msgCache.Min()
		result = first.(int64)
	}
	pq.mutex.Unlock()
	return result
}

func (pq *processQueue) isLockExpired() bool {
	return time.Now().Sub(pq.LastLockTime()) > _RebalanceLockMaxTime
}

func (pq *processQueue) isPullExpired() bool {
	return time.Now().Sub(pq.LastPullTime()) > _PullMaxIdleTime
}

func (pq *processQueue) cleanExpiredMsg(consumer *defaultConsumer) {
	if consumer.option.ConsumeOrderly {
		return
	}
	var loop = 16
	if pq.msgCache.Size() < 16 {
		loop = pq.msgCache.Size()
	}

	for i := 0; i < loop; i++ {
		pq.mutex.RLock()
		if pq.msgCache.Empty() {
			pq.mutex.RLock()
			return
		}
		_, firstValue := pq.msgCache.Min()
		msg := firstValue.(*primitive.MessageExt)
		startTime := msg.GetProperty(primitive.PropertyConsumeStartTime)
		if startTime != "" {
			st, err := strconv.ParseInt(startTime, 10, 64)
			if err != nil {
				rlog.Warning("parse message start consume time error", map[string]interface{}{
					"time":                   startTime,
					rlog.LogKeyUnderlayError: err,
				})
				continue
			}
			if time.Now().Unix()-st <= int64(consumer.option.ConsumeTimeout) {
				pq.mutex.RLock()
				return
			}
		}
		pq.mutex.RLock()

		err := consumer.sendBack(msg, 3)
		if err != nil {
			rlog.Error("send message back to broker error when clean expired messages", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			continue
		}
		pq.removeMessage(msg)
	}
}

func (pq *processQueue) getMaxSpan() int {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	if pq.msgCache.Size() == 0 {
		return 0
	}
	firstKey, _ := pq.msgCache.Min()
	lastKey, _ := pq.msgCache.Max()
	return int(lastKey.(int64) - firstKey.(int64))
}

func (pq *processQueue) getMessages() []*primitive.MessageExt {
	select {
	case <-pq.closeChan:
		return nil
	case mq := <-pq.msgCh:
		return mq
	}
}

func (pq *processQueue) takeMessages(number int) []*primitive.MessageExt {
	sleepCount := 0
	for pq.msgCache.Empty() {
		time.Sleep(10 * time.Millisecond)
		if sleepCount > 500 {
			return nil
		}
		sleepCount++
	}
	result := make([]*primitive.MessageExt, number)
	i := 0
	pq.mutex.Lock()
	for ; i < number; i++ {
		k, v := pq.msgCache.Min()
		if v == nil {
			break
		}
		result[i] = v.(*primitive.MessageExt)
		pq.consumingMsgOrderlyTreeMap.Put(k, v)
		pq.msgCache.Remove(k)
	}
	pq.mutex.Unlock()
	return result[:i]
}

func (pq *processQueue) Min() int64 {
	if pq.msgCache.Empty() {
		return -1
	}
	k, _ := pq.msgCache.Min()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) Max() int64 {
	if pq.msgCache.Empty() {
		return -1
	}
	k, _ := pq.msgCache.Max()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) MinOrderlyCache() int64 {
	if pq.consumingMsgOrderlyTreeMap.Empty() {
		return -1
	}
	k, _ := pq.consumingMsgOrderlyTreeMap.Min()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) MaxOrderlyCache() int64 {
	if pq.consumingMsgOrderlyTreeMap.Empty() {
		return -1
	}
	k, _ := pq.consumingMsgOrderlyTreeMap.Max()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) clear() {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	pq.msgCache.Clear()
	pq.cachedMsgCount.Store(0)
	pq.cachedMsgSize.Store(0)
	pq.queueOffsetMax = 0
}

func (pq *processQueue) commit() int64 {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	var offset int64
	iter, _ := pq.consumingMsgOrderlyTreeMap.Max()
	if iter != nil {
		offset = iter.(int64)
	}
	pq.cachedMsgCount.Sub(int64(pq.consumingMsgOrderlyTreeMap.Size()))

	pq.consumingMsgOrderlyTreeMap.Each(func(key interface{}, value interface{}) {
		msg := value.(*primitive.MessageExt)
		pq.cachedMsgSize.Sub(int64(len(msg.Body)))
	})

	pq.consumingMsgOrderlyTreeMap.Clear()
	return offset + 1
}

func (pq *processQueue) currentInfo() internal.ProcessQueueInfo {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	info := internal.ProcessQueueInfo{
		Locked:               pq.locked.Load(),
		TryUnlockTimes:       pq.tryUnlockTimes,
		LastLockTimestamp:    pq.LastLockTime().UnixNano() / int64(time.Millisecond),
		Dropped:              pq.dropped.Load(),
		LastPullTimestamp:    pq.LastPullTime().UnixNano() / int64(time.Millisecond),
		LastConsumeTimestamp: pq.LastConsumeTime().UnixNano() / int64(time.Millisecond),
	}

	if !pq.msgCache.Empty() {
		info.CachedMsgMinOffset = pq.Min()
		info.CachedMsgMaxOffset = pq.Max()
		info.CachedMsgCount = pq.msgCache.Size()
		info.CachedMsgSizeInMiB = pq.cachedMsgSize.Load() / int64(1024*1024)
	}

	if !pq.consumingMsgOrderlyTreeMap.Empty() {
		info.TransactionMsgMinOffset = pq.MinOrderlyCache()
		info.TransactionMsgMaxOffset = pq.MaxOrderlyCache()
		info.TransactionMsgCount = pq.consumingMsgOrderlyTreeMap.Size()
	}

	return info
}
