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
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	KFasterRandomSelectorName = "kfaster"
)

func init() {
	_ = RegisterDataPartitionSelector(KFasterRandomSelectorName, newKFasterRandomSelector)
}

func newKFasterRandomSelector(param *DpSelectorParam) (selector DataPartitionSelector, e error) {
	kValue, err := strconv.Atoi(param.kValue)
	if err != nil {
		return nil, fmt.Errorf("KFasterRandomSelector: get kValue param failed[%v]", err)
	}

	if (kValue <= 0) || (kValue >= 100) {
		return nil, fmt.Errorf("KFasterRandomSelector: invalid kValue[%v]", kValue)
	}

	selector = &KFasterRandomSelector{
		kValueHundred: kValue,
		partitions:    make([]*DataPartition, 0),
		param:         param,
	}
	log.LogInfof("KFasterRandomSelector: init selector success, kValueHundred is %v", kValue)
	return
}

type KFasterRandomSelector struct {
	sync.RWMutex
	kValueHundred int
	kValue        int
	partitions    []*DataPartition
	param         *DpSelectorParam
}

func (s *KFasterRandomSelector) Name() string {
	return KFasterRandomSelectorName
}

func (s *KFasterRandomSelector) Refresh(partitions []*DataPartition) (err error) {
	kValue := s.updateKValue(partitions)
	selectKminDataPartition(partitions, kValue)

	s.Lock()
	defer s.Unlock()

	s.kValue = kValue
	s.partitions = partitions

	return
}

func (s *KFasterRandomSelector) updateKValue(partitions []*DataPartition) (kValue int) {
	kValue = (len(partitions)-1)*s.kValueHundred/100 + 1
	return
}

func (s *KFasterRandomSelector) Select(exclude map[string]struct{}) (dp *DataPartition, err error) {
	s.RLock()
	partitions := s.partitions
	kValue := s.kValue
	s.RUnlock()

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no writable data partition")
	}

	// select random dataPartition from fasterRwPartitions
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(kValue)
	dp = partitions[index]
	if !isExcluded(dp, exclude, s.param.quorum) {
		log.LogDebugf("KFasterRandomSelector: select faster dp[%v], index %v, kValue(%v/%v)",
			dp, index, kValue, len(partitions))
		return dp, nil
	}

	log.LogWarnf("KFasterRandomSelector: first random fasterRwPartition was excluded, get partition from other faster")

	// if partitions[index] is excluded, select next in fasterRwPartitions
	for i := 1; i < kValue; i++ {
		dp = partitions[(index+i)%kValue]
		if !isExcluded(dp, exclude, s.param.quorum) {
			log.LogDebugf("KFasterRandomSelector: select faster dp[%v], index %v, kValue(%v/%v)",
				dp, (index+i)%kValue, kValue, len(partitions))
			return dp, nil
		}
	}

	log.LogWarnf("KFasterRandomSelector: all fasterRwPartitions were excluded, get partition from slower")

	// if all fasterRwPartitions are excluded, select random dataPartition in slowerRwPartitions
	slowerRwPartitionsNum := len(partitions) - kValue
	for i := 0; i < slowerRwPartitionsNum; i++ {
		dp = partitions[(index+i)%slowerRwPartitionsNum+kValue]
		if !isExcluded(dp, exclude, s.param.quorum) {
			log.LogDebugf("KFasterRandomSelector: select slower dp[%v], index %v, kValue(%v/%v)",
				dp, (index+i)%slowerRwPartitionsNum+kValue, kValue, len(partitions))
			return dp, nil
		}
	}

	return nil, fmt.Errorf("no writable data partition")
}

func (s *KFasterRandomSelector) RemoveDP(partitionID uint64) {
	s.Lock()
	defer s.Unlock()

	partitions := s.partitions

	var i int
	for i = 0; i < len(partitions); i++ {
		if partitions[i].PartitionID == partitionID {
			break
		}
	}
	if i >= len(partitions) {
		return
	}
	newRwPartition := make([]*DataPartition, 0)
	newRwPartition = append(newRwPartition, partitions[:i]...)
	newRwPartition = append(newRwPartition, partitions[i+1:]...)

	kValue := s.updateKValue(newRwPartition)
	s.kValue = kValue
	s.partitions = newRwPartition
	log.LogWarnf("RemoveDP: dp(%v), count(%v)", partitionID, len(s.partitions))

	return
}

func (s *KFasterRandomSelector) SummaryMetrics() []*proto.DataPartitionMetrics {
	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	summaryMetricsArray := make([]*proto.DataPartitionMetrics, 0)
	for _, dp := range partitions {
		metrics := dp.RemoteMetricsSummary()
		if metrics != nil {
			summaryMetricsArray = append(summaryMetricsArray, metrics)
		}
	}

	return summaryMetricsArray
}

func (s *KFasterRandomSelector) RefreshMetrics(enableRemote bool, dpMetricsMap map[uint64]*proto.DataPartitionMetrics) (err error) {
	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	if len(partitions) == 0 {
		return fmt.Errorf("no writable data partition")
	}
	for _, dp := range partitions {
		if enableRemote {
			newMetrics, _ := dpMetricsMap[dp.PartitionID]
			dp.RemoteMetricsRefresh(newMetrics)
		} else {
			dp.LocalMetricsRefresh()
		}
	}
	return
}

func swap(s []*DataPartition, i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func partByPrivot(partitions []*DataPartition, low, high int) int {
	var i, j int
	for {
		for i = low + 1; i < high; i++ {
			if partitions[i].GetAvgWrite() > partitions[low].GetAvgWrite() {
				break
			}
		}
		for j = high; j > low; j-- {
			if partitions[j].GetAvgWrite() <= partitions[low].GetAvgWrite() {
				break
			}
		}
		if i >= j {
			break
		}
		swap(partitions, i, j)
	}
	if low != j {
		swap(partitions, low, j)
	}
	return j
}

func selectKminDataPartition(partitions []*DataPartition, k int) int {
	if len(partitions) <= 1 {
		return k
	}
	low, high := 0, len(partitions)-1
	for {
		privot := partByPrivot(partitions, low, high)
		if privot < k {
			low = privot + 1
		} else if privot > k {
			high = privot - 1
		} else {
			return k
		}
	}
}
