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
	hosts2PartitionMap    map[string][]*DataPartition
}

func (s *DefaultRandomSelector) Name() string {
	return DefaultRandomSelectorName
}

func (s *DefaultRandomSelector) Refresh(partitions []*DataPartition) (err error) {
	var localLeaderPartitions []*DataPartition
	hosts2PartitionMap := map[string][]*DataPartition{}
	dpLen := len(partitions)
	for i := 0; i < dpLen; i++ {
		dp := partitions[i]
		//TODO:tangjingyu test only
		log.LogInfof("############ DefaultRandomSelector[Refresh] dpId(%v) mediaType(%v)",
			dp.PartitionID, proto.MediaTypeString(dp.MediaType))
		if strings.Split(dp.Hosts[0], ":")[0] == LocalIP {
			localLeaderPartitions = append(localLeaderPartitions, dp)
		}
		hosts2PartitionMap[dp.GetHostsString()] = append(hosts2PartitionMap[dp.GetHostsString()], dp)
	}

	s.Lock()
	defer s.Unlock()

	s.localLeaderPartitions = localLeaderPartitions
	s.partitions = partitions
	s.hosts2PartitionMap = hosts2PartitionMap
	log.LogDebugf("DefaultRandomSelector[Refresh] complete: localLeaderPartitions(%v) partitions(%v)",
		len(s.localLeaderPartitions), len(s.partitions))
	return
}

func (s *DefaultRandomSelector) Select(exclude map[string]struct{}, preferredHosts string, mediaType uint32, ehID uint64) (dp *DataPartition, err error) {
	if len(preferredHosts) > 0 {
		s.RLock()
		hosts2PartitionMap := s.hosts2PartitionMap
		s.RUnlock()

		preferredDps, exists := hosts2PartitionMap[preferredHosts]
		if exists {
			for _, preferredDp := range preferredDps {
				if !isExcluded(preferredDp, exclude) && preferredDp.MediaType == mediaType {
					return preferredDp, nil
				}
			}
		}
	}

	dp = s.getLocalLeaderDataPartition(exclude, mediaType, ehID)
	if dp != nil {
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
	s.RLock()
	rwPartitionGroups := s.partitions
	localLeaderPartitions := s.localLeaderPartitions
	hosts2PartitionMap := s.hosts2PartitionMap
	s.RUnlock()

	var i int
	for i = 0; i < len(rwPartitionGroups); i++ {
		if rwPartitionGroups[i].PartitionID == partitionID {
			break
		}
	}
	if i >= len(rwPartitionGroups) {
		return
	}

	dpToDelete := rwPartitionGroups[i]
	if dpsWithHosts, exists := hosts2PartitionMap[dpToDelete.GetHostsString()]; exists {
		var newDpsWithHosts []*DataPartition
		for _, dp := range dpsWithHosts {
			if dp.PartitionID != dpToDelete.PartitionID {
				newDpsWithHosts = append(newDpsWithHosts, dp)
			}
		}
		hosts2PartitionMap[dpToDelete.GetHostsString()] = newDpsWithHosts
	}

	newRwPartition := make([]*DataPartition, 0)
	newRwPartition = append(newRwPartition, rwPartitionGroups[:i]...)
	newRwPartition = append(newRwPartition, rwPartitionGroups[i+1:]...)

	defer func() {
		s.Lock()
		s.partitions = newRwPartition
		s.Unlock()
	}()

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

	s.Lock()
	defer s.Unlock()
	s.localLeaderPartitions = newLocalLeaderPartitions

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

func (s *DefaultRandomSelector) getRandomDataPartition(partitions []*DataPartition, exclude map[string]struct{},
	mediaType uint32, ehID uint64) (
	dp *DataPartition) {
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
