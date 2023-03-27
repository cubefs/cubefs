// Copyright 2020 The CubeFS Authors.
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
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultRandomSelectorName = "default"
)

func init() {
	_ = RegisterDataPartitionSelector(DefaultRandomSelectorName, newDefaultRandomSelector)
}

func newDefaultRandomSelector(param *DpSelectorParam) (selector DataPartitionSelector, e error) {
	selector = &DefaultRandomSelector{
		localLeaderPartitions: 	make([]*DataPartition, 0),
		partitions:            	make([]*DataPartition, 0),
		param: 					param,
	}
	return
}

type DefaultRandomSelector struct {
	sync.RWMutex
	localLeaderPartitions 	[]*DataPartition
	partitions            	[]*DataPartition
	param					*DpSelectorParam
}

func (s *DefaultRandomSelector) Name() string {
	return DefaultRandomSelectorName
}

func (s *DefaultRandomSelector) Refresh(partitions []*DataPartition) (err error) {
	var localLeaderPartitions []*DataPartition
	for i := 0; i < len(partitions); i++ {
		if strings.Split(partitions[i].Hosts[0], ":")[0] == LocalIP {
			localLeaderPartitions = append(localLeaderPartitions, partitions[i])
		}
	}

	s.Lock()
	defer s.Unlock()

	s.localLeaderPartitions = localLeaderPartitions
	s.partitions = partitions
	return
}

func (s *DefaultRandomSelector) Select(exclude map[string]struct{}) (dp *DataPartition, err error) {
	//dp = s.getLocalLeaderDataPartition(exclude)
	//if dp != nil {
	//	return dp, nil
	//}

	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	dp = s.getRandomDataPartition(partitions, exclude)

	if dp != nil {
		return dp, nil
	}

	return nil, fmt.Errorf("no writable data partition")
}

func (s *DefaultRandomSelector) RemoveDP(partitionID uint64) {
	s.Lock()
	defer s.Unlock()
	rwPartitionGroups := s.partitions
	localLeaderPartitions := s.localLeaderPartitions

	var i int
	for i = 0; i < len(rwPartitionGroups); i++ {
		if rwPartitionGroups[i].PartitionID == partitionID {
			break
		}
	}
	if i >= len(rwPartitionGroups) {
		return
	}
	newRwPartition := make([]*DataPartition, 0)
	newRwPartition = append(newRwPartition, rwPartitionGroups[:i]...)
	newRwPartition = append(newRwPartition, rwPartitionGroups[i+1:]...)
	s.partitions = newRwPartition
	log.LogWarnf("RemoveDP: dp(%v), count(%v)", partitionID, len(s.partitions))

	for i = 0; i < len(localLeaderPartitions); i++ {
		if localLeaderPartitions[i].PartitionID == partitionID {
			break
		}
	}
	if i >= len(localLeaderPartitions) {
		return
	}
	newLocalLeaderPartitions := make([]*DataPartition, 0)
	newLocalLeaderPartitions = append(newLocalLeaderPartitions, localLeaderPartitions[:i]...)
	newLocalLeaderPartitions = append(newLocalLeaderPartitions, localLeaderPartitions[i+1:]...)
	s.localLeaderPartitions = newLocalLeaderPartitions

	return
}

func (s *DefaultRandomSelector) SummaryMetrics() []*proto.DataPartitionMetrics {
	return nil
}

func (s *DefaultRandomSelector) RefreshMetrics(enableRemote bool, dpMetrics map[uint64]*proto.DataPartitionMetrics) error {
	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	if len(partitions) == 0 {
		return fmt.Errorf("no writable data partition")
	}
	for _, dp := range partitions {
		dp.LocalMetricsClear()
	}
	return nil
}

//func (s *DefaultRandomSelector) getLocalLeaderDataPartition(exclude map[string]struct{}) *DataPartition {
//	s.RLock()
//	localLeaderPartitions := s.localLeaderPartitions
//	s.RUnlock()
//	return s.getRandomDataPartition(localLeaderPartitions, exclude)
//}

func (s *DefaultRandomSelector) getRandomDataPartition(partitions []*DataPartition, exclude map[string]struct{}) (
	dp *DataPartition) {
	length := len(partitions)
	if length == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(length)
	dp = partitions[index]
	if !isExcluded(dp, exclude, s.param.quorum) {
		log.LogDebugf("DefaultRandomSelector: select dp[%v], index %v", dp, index)
		return dp
	}

	log.LogWarnf("DefaultRandomSelector: first random partition was excluded, get partition from others")

	var currIndex int
	for i := 0; i < length; i++ {
		currIndex = (index + i) % length
		if !isExcluded(partitions[currIndex], exclude, s.param.quorum) {
			log.LogDebugf("DefaultRandomSelector: select dp[%v], index %v", partitions[currIndex], currIndex)
			return partitions[currIndex]
		}
	}
	return nil
}
