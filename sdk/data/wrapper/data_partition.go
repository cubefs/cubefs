// Copyright 2018 The Chubao Authors.
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

package wrapper

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite   bool
	PartitionType string
	NearHosts     []string
	ClientWrapper *Wrapper
	Metrics       *DataPartitionMetrics
}

// DataPartitionMetrics defines the wrapper of the metrics related to the data partition.
type DataPartitionMetrics struct {
	sync.RWMutex
	AvgReadLatencyNano  int64
	AvgWriteLatencyNano int64
	SumReadLatencyNano  int64
	SumWriteLatencyNano int64
	ReadOpNum           int64
	WriteOpNum          int64
}

// If the connection fails, take punitive measures. Punish time is 5s.
func (dp *DataPartition) RecordWrite(startT int64, punish bool) {
	if startT == 0 {
		log.LogWarnf("RecordWrite: invalid start time")
		return
	}

	cost := time.Now().UnixNano() - startT
	if punish {
		cost += 5 * 1e9
		log.LogWarnf("RecordWrite: dp[%v] punish write time[5s] because of error, avg[%v]ns", dp.PartitionID, dp.GetAvgWrite())
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	dp.Metrics.WriteOpNum++
	dp.Metrics.SumWriteLatencyNano += cost

	return
}

func (dp *DataPartition) MetricsRefresh() {
	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if dp.Metrics.ReadOpNum != 0 {
		dp.Metrics.AvgReadLatencyNano = dp.Metrics.SumReadLatencyNano / dp.Metrics.ReadOpNum
	} else {
		dp.Metrics.AvgReadLatencyNano = 0
	}

	if dp.Metrics.WriteOpNum != 0 {
		dp.Metrics.AvgWriteLatencyNano = (9*dp.Metrics.AvgWriteLatencyNano + dp.Metrics.SumWriteLatencyNano/dp.Metrics.WriteOpNum) / 10
	} else {
		dp.Metrics.AvgWriteLatencyNano = (9 * dp.Metrics.AvgWriteLatencyNano) / 10
	}

	dp.Metrics.SumReadLatencyNano = 0
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.ReadOpNum = 0
	dp.Metrics.WriteOpNum = 0
}

func (dp *DataPartition) GetAvgRead() int64 {
	dp.Metrics.RLock()
	defer dp.Metrics.RUnlock()

	return dp.Metrics.AvgReadLatencyNano
}

func (dp *DataPartition) GetAvgWrite() int64 {
	dp.Metrics.RLock()
	defer dp.Metrics.RUnlock()

	return dp.Metrics.AvgWriteLatencyNano
}

type DataPartitionSorter []*DataPartition

func (ds DataPartitionSorter) Len() int {
	return len(ds)
}
func (ds DataPartitionSorter) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}
func (ds DataPartitionSorter) Less(i, j int) bool {
	return ds[i].Metrics.AvgWriteLatencyNano < ds[j].Metrics.AvgWriteLatencyNano
}

// NewDataPartitionMetrics returns a new DataPartitionMetrics instance.
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	return metrics
}

// String returns the string format of the data partition.
func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v) NearHosts(%v)",
		dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts, dp.NearHosts)
}

func (dp *DataPartition) CheckAllHostsIsAvail(exclude map[string]struct{}) {
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for i := 0; i < len(dp.Hosts); i++ {
		host := dp.Hosts[i]
		wg.Add(1)
		go func(addr string) {
			var (
				conn net.Conn
				err  error
			)
			defer wg.Done()
			if conn, err = util.DailTimeOut(addr, time.Second); err != nil {
				log.LogWarnf("Dail to Host (%v) err(%v)", addr, err.Error())
				if strings.Contains(err.Error(), syscall.ECONNREFUSED.Error()) {
					lock.Lock()
					exclude[addr] = struct{}{}
					lock.Unlock()
				}
			} else {
				conn.Close()
			}
		}(host)
	}
	wg.Wait()
	log.LogDebugf("CheckAllHostsIsAvail: dp(%v) exclude(%v)", dp.PartitionID, exclude)
}

// GetAllAddrs returns the addresses of all the replicas of the data partition.
func (dp *DataPartition) GetAllAddrs() string {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func isExcluded(dp *DataPartition, exclude map[string]struct{}) bool {
	for _, host := range dp.Hosts {
		if _, exist := exclude[host]; exist {
			return true
		}
	}
	return false
}
