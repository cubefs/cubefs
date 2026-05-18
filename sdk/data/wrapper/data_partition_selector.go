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
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type RefreshDpPolicy int32

const (
	MergeDpPolicy RefreshDpPolicy = iota
	UpdateDpPolicy
)

// This type defines the constructor used to create and initialize the selector.
type DataPartitionSelectorConstructor = func(param string) (DataPartitionSelector, error)

// DataPartitionSelector is the interface defines the methods necessary to implement
// a selector for data partition selecting.
type DataPartitionSelector interface {
	// Name return name of current selector instance.
	Name() string

	// Refresh refreshes current selector instance by specified data partitions.
	Refresh(partitions []*DataPartition) error

	// Select returns an data partition picked by selector.
	Select(excludes map[string]struct{}, poolId uint8, ehID uint64) (*DataPartition, error)

	// RemoveDP removes specified data partition.
	RemoveDP(partitionID uint64)

	// Count return number of data partitions held by selector.
	Count() int

	// CountByPoolId return number of data partitions held by selector for a specific pool.
	CountByPoolId(poolId uint8) int

	// GetAllDp return data partitions held by selector
	GetAllDp() (dp []*DataPartition)
}

var (
	dataPartitionSelectorConstructors = make(map[string]DataPartitionSelectorConstructor)

	ErrDuplicatedDataPartitionSelectorConstructor = errors.New("duplicated data partition selector constructor")
	ErrDataPartitionSelectorConstructorNotExist   = errors.New("data partition selector constructor not exist")
)

// RegisterDataPartitionSelector registers a selector constructor.
// Users can register their own defined selector through this method.
func RegisterDataPartitionSelector(name string, constructor DataPartitionSelectorConstructor) error {
	clearName := strings.TrimSpace(strings.ToLower(name))
	if _, exist := dataPartitionSelectorConstructors[clearName]; exist {
		return ErrDuplicatedDataPartitionSelectorConstructor
	}
	dataPartitionSelectorConstructors[clearName] = constructor
	return nil
}

func newDataPartitionSelector(name string, param string) (newDpSelector DataPartitionSelector, err error) {
	clearName := strings.TrimSpace(strings.ToLower(name))
	constructor, exist := dataPartitionSelectorConstructors[clearName]
	if !exist {
		return nil, ErrDataPartitionSelectorConstructorNotExist
	}
	return constructor(param)
}

func (w *Wrapper) initDpSelector() (err error) {
	w.dpSelectorChanged = false
	selectorName := w.dpSelectorName
	if strings.TrimSpace(selectorName) == "" {
		log.LogInfof("initDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
		selectorName = DefaultRandomSelectorName
	}
	var selector DataPartitionSelector
	if selector, err = newDataPartitionSelector(selectorName, w.dpSelectorParm); err != nil {
		log.LogErrorf("initDpSelector: dpSelector[%v] init failed caused by [%v], use default selector", w.dpSelectorName,
			err)
		return
	}
	w.dpSelector = selector
	return
}

func (w *Wrapper) refreshMinDpCount(oldDpCount int) (count int) {
	tmp := float64(oldDpCount) * 2 / 3
	count = int(tmp)
	return
}

func (w *Wrapper) refreshDpSelector(refreshPolicy RefreshDpPolicy, partitions []*DataPartition) {
	w.Lock.RLock()
	dpSelector := w.dpSelector
	dpSelectorChanged := w.dpSelectorChanged
	w.Lock.RUnlock()

	if dpSelectorChanged {
		selectorName := w.dpSelectorName
		if strings.TrimSpace(selectorName) == "" {
			log.LogWarnf("refreshDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
			selectorName = DefaultRandomSelectorName
		}
		newDpSelector, err := newDataPartitionSelector(selectorName, w.dpSelectorParm)
		if err != nil {
			log.LogErrorf("refreshDpSelector: change dpSelector to [%v %v] failed caused by [%v],"+
				" use last valid selector. Please change dpSelector config through master.",
				w.dpSelectorName, w.dpSelectorParm, err)
		} else {
			w.Lock.Lock()
			log.LogInfof("refreshDpSelector: change dpSelector to [%v %v]", w.dpSelectorName, w.dpSelectorParm)
			w.dpSelector = newDpSelector
			w.dpSelectorChanged = false
			dpSelector = newDpSelector
			w.Lock.Unlock()
		}
	}

	log.LogInfof("[refreshDpSelector] refresh dp, partition count(%v)", len(partitions))
	if refreshPolicy == UpdateDpPolicy {
		oldDps := dpSelector.GetAllDp()

		newPoolDps := make(map[uint8][]*DataPartition)
		for _, dp := range partitions {
			newPoolDps[dp.PoolId] = append(newPoolDps[dp.PoolId], dp)
		}

		oldPoolDps := make(map[uint8][]*DataPartition)
		for _, dp := range oldDps {
			oldPoolDps[dp.PoolId] = append(oldPoolDps[dp.PoolId], dp)
		}

		mergeTable := make(map[uint64]int)
		for _, dp := range oldDps {
			mergeTable[dp.PartitionID] = 1
		}

		for _, dp := range partitions {
			mergeTable[dp.PartitionID] = mergeTable[dp.PartitionID] + 1
		}

		// NOTE: take some old dps and put it back
		randGen := rand.New(rand.NewSource(time.Now().Unix()))
		for poolId, oldDpsInPool := range oldPoolDps {
			if len(oldDpsInPool) == 0 {
				continue
			}
			newDps := newPoolDps[poolId]
			minDpCount := w.refreshMinDpCount(dpSelector.CountByPoolId(poolId))

			poolNewDpCount := len(newDps)
			for poolNewDpCount < minDpCount {
				index := randGen.Intn(len(oldDpsInPool))
				selectedDp := oldDpsInPool[index]
				if mergeTable[selectedDp.PartitionID] == 2 {
					continue
				}
				mergeTable[selectedDp.PartitionID] = 2
				partitions = append(partitions, selectedDp)
				poolNewDpCount++
				log.LogWarnf("[refreshDpSelector] put dp(%v) pool(%v) to rw dp table, dp(%v) maybe readonly", selectedDp.PartitionID, selectedDp.PoolId, selectedDp.PartitionID)
			}
		}
	} else if refreshPolicy == MergeDpPolicy {
		oldDps := dpSelector.GetAllDp()
		mergeTable := make(map[uint64]int)
		for _, dp := range oldDps {
			mergeTable[dp.PartitionID] = 1
		}

		for _, dp := range partitions {
			if _, ok := mergeTable[dp.PartitionID]; !ok {
				oldDps = append(oldDps, dp)
			}
		}
		partitions = oldDps
	}
	if log.EnableDebug() {
		for _, dp := range partitions {
			log.LogDebugf("[refreshDpSelector] refresh dp(%v) to rw partition", dp.PartitionID)
		}
	}
	log.LogInfof("[refreshDpSelector] finally refresh dp count(%v) to rw partitions", len(partitions))
	_ = dpSelector.Refresh(partitions)
}

// getDataPartitionForWrite returns an available data partition for write.
func (w *Wrapper) GetDataPartitionForWrite(exclude map[string]struct{}, poolId uint8, ehID uint64) (*DataPartition, error) {
	w.Lock.RLock()
	dpSelector := w.dpSelector
	w.Lock.RUnlock()

	return dpSelector.Select(exclude, poolId, ehID)
}

func (w *Wrapper) RemoveDataPartitionForWrite(partitionID uint64, poolId uint8) error {
	w.Lock.RLock()
	dpSelector := w.dpSelector
	w.Lock.RUnlock()

	if dpSelector.CountByPoolId(poolId) <= 1 {
		return errors.New("not enough data partitions")
	}

	dpSelector.RemoveDP(partitionID)
	return nil
}
