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

package repl

import (
	"sync"
	"time"
)

type OperatorStats struct {
	lock               sync.RWMutex
	flowEnqueCounter   uint64
	flowDequeCounter   uint64
	flowDropCounter    uint64
	packetEnqueCounter uint64
	packetDequeCounter uint64
	packetDropCounter  uint64
	waitTime           time.Duration
	firstEnqueTime     time.Time
	lastDequeTime      time.Time
}

type OperatorStatsSnapshot struct {
	FlowEnqueCounter   uint64
	FlowDequeCounter   uint64
	FlowDropCounter    uint64
	PacketEnqueCounter uint64
	PacketDequeCounter uint64
	PacketDropCounter  uint64
	WaitTime           time.Duration
	SnapshotTime       time.Time
}

type OperatorStatsSample struct {
	first         OperatorStatsSnapshot
	second        OperatorStatsSnapshot
	queueLength   int
	queueCapacity int
}

func (s *OperatorStats) OnEnque(flow uint64) {
	now := time.Now()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.flowEnqueCounter += flow
	s.packetEnqueCounter += 1
	var zeroTime time.Time
	if s.firstEnqueTime == zeroTime || (s.lastDequeTime != zeroTime && s.lastDequeTime.After(zeroTime)) {
		s.firstEnqueTime = now
	}
}

func (s *OperatorStats) OnDrop(flow uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.flowDropCounter += flow
	s.packetDropCounter += 1
}

func (s *OperatorStats) OnDeque(flow uint64, last bool) {
	now := time.Now()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.flowDequeCounter += flow
	s.packetDequeCounter += 1
	if last {
		var zeroTime time.Time
		// if s.firstEnqueTime equal with zero or before s.lastDequeTime
		// it means that OnDeque acquire the lock before OnEnque
		// we doesn't need to record the wait time, because the duration is too short
		if s.firstEnqueTime != zeroTime && s.firstEnqueTime.After(s.lastDequeTime) {
			duration := now.Sub(s.firstEnqueTime)
			s.waitTime += duration
		}
		s.lastDequeTime = now
	}
}

func (s *OperatorStats) GetSnapshot() *OperatorStatsSnapshot {
	now := time.Now()
	s.lock.RLock()
	defer s.lock.RUnlock()
	waitTime := s.waitTime
	var zeroTime time.Time
	if s.firstEnqueTime != zeroTime && (s.lastDequeTime == zeroTime || s.firstEnqueTime.After(s.lastDequeTime)) {
		duration := now.Sub(s.firstEnqueTime)
		waitTime += duration
	}
	snapshot := &OperatorStatsSnapshot{
		FlowEnqueCounter:   s.flowEnqueCounter,
		FlowDequeCounter:   s.flowDequeCounter,
		FlowDropCounter:    s.flowDropCounter,
		PacketEnqueCounter: s.packetEnqueCounter,
		PacketDequeCounter: s.packetDequeCounter,
		PacketDropCounter:  s.packetDropCounter,
		WaitTime:           waitTime,
		SnapshotTime:       now,
	}
	return snapshot
}

func (s *OperatorStats) Sample(sampleDuration time.Duration, queue chan *Packet) *OperatorStatsSample {
	first := s.GetSnapshot()
	time.Sleep(sampleDuration)
	second := s.GetSnapshot()
	return &OperatorStatsSample{
		first:         *first,
		second:        *second,
		queueLength:   len(queue),
		queueCapacity: cap(queue),
	}
}

func (s *OperatorStatsSample) GetEnqueFlow() uint64 {
	return s.second.FlowEnqueCounter - s.first.FlowEnqueCounter
}

func (s *OperatorStatsSample) GetDequeFlow() uint64 {
	return s.second.FlowDequeCounter - s.first.FlowDequeCounter
}

func (s *OperatorStatsSample) GetWaitFlow() uint64 {
	return s.second.FlowEnqueCounter - s.second.FlowDequeCounter
}

func (s *OperatorStatsSample) GetEnquePacket() uint64 {
	return s.second.PacketEnqueCounter - s.first.PacketEnqueCounter
}

func (s *OperatorStatsSample) GetDequePacket() uint64 {
	return s.second.PacketDequeCounter - s.first.PacketDequeCounter
}

func (s *OperatorStatsSample) GetWaitPacket() uint64 {
	return s.second.FlowEnqueCounter - s.second.FlowDequeCounter
}

func (s *OperatorStatsSample) GetSampleDuration() time.Duration {
	return s.second.SnapshotTime.Sub(s.first.SnapshotTime)
}

func (s *OperatorStatsSample) GetWaitTime() time.Duration {
	return s.second.WaitTime - s.first.WaitTime
}

func (s *OperatorStatsSample) GetUtil() float64 {
	sampleDuration := s.GetSampleDuration()
	waitTime := s.GetWaitTime()
	return float64(waitTime.Milliseconds()) / float64(sampleDuration.Milliseconds())
}

func (s *OperatorStatsSample) GetQueueLength() int {
	return s.queueLength
}

func (s *OperatorStatsSample) GetQueueCapacity() int {
	return s.queueCapacity
}

func (s *OperatorStatsSample) GetDropPacket() uint64 {
	return s.second.PacketDropCounter - s.first.PacketDropCounter
}

func (s *OperatorStatsSample) GetDropFlow() uint64 {
	return s.second.FlowDropCounter - s.first.FlowDropCounter
}

func (s *OperatorStatsSample) GetDropPacketRate() float64 {
	if s.GetDropPacket() == 0 {
		return 0
	}
	return float64(s.GetDropPacket()) / float64(s.GetDropPacket()+s.GetEnquePacket())
}

func (s *OperatorStatsSample) GetDropFlowRate() float64 {
	if s.GetDequeFlow() == 0 {
		return 0
	}
	return float64(s.GetDropFlow()) / float64(s.GetDropFlow()+s.GetEnqueFlow())
}
