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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"strings"
	"syscall"
	"time"
)

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite   bool
	PartitionType string
	ClientWrapper *Wrapper
	Metrics       *DataPartitionMetrics
}

// DataPartitionMetrics defines the wrapper of the metrics related to the data partition.
type DataPartitionMetrics struct {
	WriteLatency float64
	ReadLatency  float64
}

type DataPartitionSorter []*DataPartition

func (ds DataPartitionSorter) Len() int {
	return len(ds)
}
func (ds DataPartitionSorter) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}
func (ds DataPartitionSorter) Less(i, j int) bool {
	return ds[i].Metrics.WriteLatency < ds[j].Metrics.WriteLatency
}

// NewDataPartitionMetrics returns a new DataPartitionMetrics instance.
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	return metrics
}

// String returns the string format of the data partition.
func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v)",
		dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts)
}

func (dp *DataPartition) CheckAllHostsIsAvail(exclude map[string]struct{}) {
	var (
		conn net.Conn
		err  error
	)
	for i := 0; i < len(dp.Hosts); i++ {
		host := dp.Hosts[i]
		if conn, err = util.DailTimeOut(host, proto.ReadDeadlineTime*time.Second); err != nil {
			log.LogWarnf("Dail to Host (%v) err(%v)", host, err.Error())
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
