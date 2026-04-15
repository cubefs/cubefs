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

package wrapper

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite   bool
	NearHosts     []string
	ClientWrapper *Wrapper
	Metrics       *DataPartitionMetrics

	pingElapsedSortedHosts *util.PingElapsedSortedHosts
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

func (dp *DataPartition) RecordWrite(startT int64) {
	if startT == 0 {
		log.LogWarnf("RecordWrite: invalid start time")
		return
	}
	cost := time.Now().UnixNano() - startT

	dp.Metrics.Lock()
	dp.Metrics.WriteOpNum++
	dp.Metrics.SumWriteLatencyNano += cost
	dp.Metrics.Unlock()
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
		dp.Metrics.AvgWriteLatencyNano = dp.Metrics.SumWriteLatencyNano / dp.Metrics.WriteOpNum
	} else {
		dp.Metrics.AvgWriteLatencyNano = 0
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

func dataPartitionResponseStatusString(s int8) string {
	switch s {
	case proto.Recovering:
		return "Recovering"
	case proto.ReadOnly:
		return "ReadOnly"
	case proto.ReadWrite:
		return "ReadWrite"
	case proto.Unavailable:
		return "Unavailable"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// String implements fmt.Stringer for concise logs (use %v on *DataPartition, not on embedded DataPartitionResponse).
func (dp *DataPartition) String() string {
	if dp == nil {
		return "<nil>*DataPartition"
	}
	return fmt.Sprintf("PartitionID(%v) PoolId(%v) Type(%v) Status(%v) ReplicaNum(%v) Epoch(%v) Recover(%v) Discard(%v) "+
		"Hosts(%v) Leader(%v) RandomWrite(%v) NearHosts(%v) MediaType(%v)",
		dp.PartitionID, dp.PoolId, dp.PartitionType, dataPartitionResponseStatusString(dp.Status), dp.ReplicaNum, dp.Epoch,
		dp.IsRecover, dp.IsDiscard, dp.Hosts, dp.LeaderAddr, dp.RandomWrite, dp.NearHosts, proto.MediaTypeString(dp.MediaType))
}

func (dp *DataPartition) CheckAllHostsIsAvail(exclude map[string]struct{}) {
	var (
		conn net.Conn
		err  error
	)
	for i := 0; i < len(dp.Hosts); i++ {
		host := dp.Hosts[i]
		if conn, err = util.DailTimeOut(host, proto.ReadDeadlineTime*time.Second); err != nil {
			log.LogWarnf("CheckAllHostsIsAvail: dial host (%v) err(%v)", host, err)
			if strings.Contains(err.Error(), syscall.ECONNREFUSED.Error()) {
				exclude[host] = struct{}{}
			}
			continue
		}
		conn.Close()
	}
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

func (dp *DataPartition) SortHostsByPingElapsed() []string {
	if !dp.ClientWrapper.FollowerRead() {
		return []string{dp.LeaderAddr}
	}
	if dp.pingElapsedSortedHosts == nil {
		getHosts := func() []string {
			return dp.Hosts
		}
		getElapsed := func(host string) (time.Duration, bool) {
			delay, ok := dp.ClientWrapper.HostsDelay.Load(host)
			if !ok {
				return 0, false
			}
			return delay.(time.Duration), true
		}
		dp.pingElapsedSortedHosts = util.NewPingElapsedSortHosts(getHosts, getElapsed)
	}
	return dp.pingElapsedSortedHosts.GetSortedHosts()
}
