// Copyright 2018 The Containerfs Authors.
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

type DiskMetrics struct {
	Status               int32
	ReadErrs             int32
	WriteErrs            int32
	MaxDiskErrs          int32
	MinRestWeight        int64
	TotalWeight          int64
	RealAvailWeight      int64
	PartitionAvailWeight int64
	Path                 string
}

//various metrics such free and total storage space, traffic, etc
type Stats struct {
	inDataSize  uint64
	outDataSize uint64
	inFlow      uint64
	outFlow     uint64

	Zone                            string
	CurrentConns                    int64
	ClusterID                       string
	TcpAddr                         string
	Start                           time.Time
	Total                           uint64
	Used                            uint64
	Available                       uint64
	CreatedPartitionWeights         uint64 //dataPartitionCnt*dataPartitionSize
	RemainWeightsForCreatePartition uint64 //all-useddataPartitionsWieghts
	CreatedPartitionCnt             uint64
	MaxWeightsForCreatePartition    uint64

	sync.Mutex
}

func NewStats(zone string) (s *Stats) {
	s = new(Stats)
	s.Zone = zone
	return s
}

func (s *Stats) AddConnection() {
	atomic.AddInt64(&s.CurrentConns, 1)
}

func (s *Stats) RemoveConnection() {
	atomic.AddInt64(&s.CurrentConns, -1)
}

func (s *Stats) GetConnectionNum() int64 {
	return atomic.LoadInt64(&s.CurrentConns)
}

func (s *Stats) AddInDataSize(size uint64) {
	atomic.AddUint64(&s.inDataSize, size)
}

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
	s.RemainWeightsForCreatePartition = remainWeightsForCreatePartition
	s.MaxWeightsForCreatePartition = maxWeightsForCreatePartition
	s.CreatedPartitionCnt = dataPartitionCnt
}
