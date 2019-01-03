// Copyright 2018 The CFS Authors.
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

package datanode

import (
	"sync"
	"sync/atomic"
	"time"
)

// TODO add comments
type DiskMetrics struct {
	Status               int32 // TODO we need to add comments to each entry here
	ReadErrs             int32
	WriteErrs            int32
	MaxDiskErrs          int32
	MinRestWeight        int64
	TotalWeight          int64
	RealAvailWeight      int64
	PartitionAvailWeight int64
	Path                 string
}

// Stats defines various metrics that will be collected during the execution
type Stats struct {
	inDataSize  uint64
	outDataSize uint64
	inFlow      uint64
	outFlow     uint64

	Zone                               string
	ConnectionCnt                      int64
	ClusterID                          string
	TCPAddr                            string
	Start                              time.Time
	Total                              uint64
	Used                               uint64
	Available                          uint64 // TODO what is available?
	CreatedPartitionWeights            uint64 // TODO what is CreatedPartitionWeights dataPartitionCnt * dataPartitionSize
	RemainingWeightsForCreatePartition uint64 // TODO what is RemainingWeightsForCreatePartition all used dataPartitionsWieghts
	CreatedPartitionCnt                uint64
	MaxWeightsForCreatePartition       uint64

	sync.Mutex
}

// Create a new Stats
func NewStats(zone string) (s *Stats) {
	s = new(Stats)
	s.Zone = zone
	return s
}

// AddConnection adds a connection
func (s *Stats) AddConnection() {
	atomic.AddInt64(&s.ConnectionCnt, 1)
}

// RemoveConnection removes a connection
func (s *Stats) RemoveConnection() {
	atomic.AddInt64(&s.ConnectionCnt, -1)
}

// GetConnectionCount gets the connection count
func (s *Stats) GetConnectionCount() int64 {
	return atomic.LoadInt64(&s.ConnectionCnt)
}

// TODO what is AddInDataSize
func (s *Stats) AddInDataSize(size uint64) {
	atomic.AddUint64(&s.inDataSize, size)
}

// TODO what is AddOutDataSize
func (s *Stats) AddOutDataSize(size uint64) {
	atomic.AddUint64(&s.outDataSize, size)
}

func (s *Stats) updateMetrics(
	total, used, available, createdPartitionWeights, remainWeightsForCreatePartition,
	maxWeightsForCreatePartition, dataPartitionCnt uint64) {
	s.Lock()
	defer s.Unlock()
	s.Total = total
	s.Used = used
	s.Available = available
	s.CreatedPartitionWeights = createdPartitionWeights
	s.RemainingWeightsForCreatePartition = remainWeightsForCreatePartition
	s.MaxWeightsForCreatePartition = maxWeightsForCreatePartition
	s.CreatedPartitionCnt = dataPartitionCnt
}
