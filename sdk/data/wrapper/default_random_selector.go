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

package wrapper

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultRandomSelectorName = "default"
)

func init() {
	_ = RegisterDataPartitionSelector(DefaultRandomSelectorName, newDefaultRandomSelector)
}

func newDefaultRandomSelector(_ string) (selector DataPartitionSelector, e error) {
	selector = &DefaultRandomSelector{
		localLeaderPartitions: make([]*DataPartition, 0),
		partitions:            make([]*DataPartition, 0),
	}
	return
}

type DefaultRandomSelector struct {
	sync.RWMutex
	localLeaderPartitions []*DataPartition
	partitions            []*DataPartition
	removeDpMutex         sync.Mutex
}

func (s *DefaultRandomSelector) Name() string {
	return DefaultRandomSelectorName
}

func (s *DefaultRandomSelector) Refresh(partitions []*DataPartition) (err error) {
	var localLeaderPartitions []*DataPartition
	for i := 0; i < len(partitions); i++ {
		//TODO:tangjingyu test only
		log.LogInfof("############ DefaultRandomSelector[Refresh] dpId(%v) mediaType(%v)",
			partitions[i].PartitionID, proto.MediaTypeString(partitions[i].MediaType))
		if strings.Split(partitions[i].Hosts[0], ":")[0] == LocalIP {
			localLeaderPartitions = append(localLeaderPartitions, partitions[i])
		}
	}

	s.Lock()
	defer s.Unlock()

	s.localLeaderPartitions = localLeaderPartitions
	s.partitions = partitions
	log.LogDebugf("DefaultRandomSelector[Refresh] complete: localLeaderPartitions(%v) partitions(%v)",
		len(s.localLeaderPartitions), len(s.partitions))
	return
}

func (s *DefaultRandomSelector) Select(exclude map[string]struct{}, mediaType uint32, ehID uint64) (dp *DataPartition, err error) {
	dp = s.getLocalLeaderDataPartition(exclude, mediaType, ehID)
	if dp != nil {
		log.LogDebugf("Select: select dp[%v] address[%p] from LocalLeaderDataPartition", dp, dp)
		return dp, nil
	}

	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	dp = s.getRandomDataPartition(partitions, exclude, mediaType, ehID)

	if dp != nil {
		//TODO:tangjingyu test only
		log.LogInfof("############ DefaultRandomSelector[Select]: eh(%v) targetMediaType(%v), selected dpId(%v) mediaType(%v)",
			ehID, proto.MediaTypeString(mediaType), dp.PartitionID, proto.MediaTypeString(dp.MediaType))
		return dp, nil
	}

	log.LogErrorf("DefaultRandomSelector: ehID(%v) no writable data partition with %v partitions and exclude(%v)mediaType(%v)",
		ehID, len(partitions), exclude, proto.MediaTypeString(mediaType))
	return nil, fmt.Errorf("en(%v) no writable data partition", ehID)
}

func (s *DefaultRandomSelector) RemoveDP(partitionID uint64) {
	s.removeDpMutex.Lock()
	defer s.removeDpMutex.Unlock()

	s.RLock()
	rwPartitionGroups := s.partitions
	localLeaderPartitions := s.localLeaderPartitions
	log.LogDebugf("RemoveDP: partitionID[%v], len(s.partitions)=%v len(s.localLeaderPartitions)=%v\n", partitionID, len(s.partitions), len(s.localLeaderPartitions))
	s.RUnlock()

	var i int
	for i = 0; i < len(rwPartitionGroups); i++ {
		if rwPartitionGroups[i].PartitionID == partitionID {
			log.LogDebugf("RemoveDP: found partitionID[%v] in rwPartitionGroups. dp[%v] address[%p]\n", partitionID, rwPartitionGroups[i], rwPartitionGroups[i])
			break
		}
	}
	if i >= len(rwPartitionGroups) {
		log.LogDebugf("RemoveDP: not found partitionID[%v] in rwPartitionGroups", partitionID)
		return
	}

	newRwPartition := make([]*DataPartition, 0)
	newRwPartition = append(newRwPartition, rwPartitionGroups[:i]...)
	newRwPartition = append(newRwPartition, rwPartitionGroups[i+1:]...)

	defer func() {
		s.Lock()
		s.partitions = newRwPartition
		log.LogDebugf("RemoveDP: finish, partitionID[%v], len(s.partitions)=%v\n", partitionID, len(s.partitions))
		s.Unlock()
	}()

	for i = 0; i < len(localLeaderPartitions); i++ {
		if localLeaderPartitions[i].PartitionID == partitionID {
			log.LogDebugf("RemoveDP: found partitionID[%v] in localLeaderPartitions. dp[%v] address[%p]\n", partitionID, localLeaderPartitions[i], localLeaderPartitions[i])
			break
		}
	}
	if i >= len(localLeaderPartitions) {
		log.LogDebugf("RemoveDP: not found partitionID[%v] in localLeaderPartitions", partitionID)
		return
	}
	newLocalLeaderPartitions := make([]*DataPartition, 0)
	newLocalLeaderPartitions = append(newLocalLeaderPartitions, localLeaderPartitions[:i]...)
	newLocalLeaderPartitions = append(newLocalLeaderPartitions, localLeaderPartitions[i+1:]...)

	s.Lock()
	defer s.Unlock()
	s.localLeaderPartitions = newLocalLeaderPartitions
	log.LogDebugf("RemoveDP: finish, partitionID[%v], len(s.localLeaderPartitions)=%v\n", partitionID, len(s.localLeaderPartitions))

	return
}

func (s *DefaultRandomSelector) Count() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.partitions)
}

func (s *DefaultRandomSelector) getLocalLeaderDataPartition(exclude map[string]struct{}, mediaType uint32, ehID uint64) *DataPartition {
	s.RLock()
	localLeaderPartitions := s.localLeaderPartitions
	s.RUnlock()
	return s.getRandomDataPartition(localLeaderPartitions, exclude, mediaType, ehID)
}

func (s *DefaultRandomSelector) getRandomDataPartition(partitions []*DataPartition, exclude map[string]struct{}, mediaType uint32, ehID uint64) (
	dp *DataPartition,
) {
	length := len(partitions)
	if length == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(length)
	dp = partitions[index]
	if !isExcluded(dp, exclude) && dp.MediaType == mediaType {
		log.LogDebugf("DefaultRandomSelector: eh(%v) select dp[%v] address[%p], index %v", ehID, dp, dp, index)
		return dp
	}

	log.LogDebugf("DefaultRandomSelector: eh(%v)first random partition was excluded, get partition from others", ehID)

	var currIndex int
	for i := 0; i < length; i++ {
		currIndex = (index + i) % length
		dp = partitions[currIndex]
		if !isExcluded(dp, exclude) && dp.MediaType == mediaType {
			log.LogDebugf("DefaultRandomSelector: eh(%v) select dp[%v], index %v", ehID, partitions[currIndex], currIndex)
			return partitions[currIndex]
		}
	}
	return nil
}

func (s *DefaultRandomSelector) GetAllDp() (dps []*DataPartition) {
	s.RLock()
	defer s.RUnlock()
	dps = make([]*DataPartition, len(s.partitions))
	copy(dps, s.partitions)
	return
}
