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
	"errors"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// This type defines the constructor used to create and initialize the selector.
type DataPartitionSelectorConstructor = func(param *DpSelectorParam) (DataPartitionSelector, error)

// DataPartitionSelector is the interface defines the methods necessary to implement
// a selector for data partition selecting.
type DataPartitionSelector interface {
	// Name return name of current selector instance.
	Name() string

	// Refresh refreshes current selector instance by specified data partitions.
	Refresh(partitions []*DataPartition) error

	// Select returns a data partition picked by selector.
	Select(excludes map[string]struct{}) (*DataPartition, error)

	// RemoveDP removes specified data partition.
	RemoveDP(partitionID uint64)

	// SummaryMetrics summaries the metrics of dp write operate
	SummaryMetrics() []*proto.DataPartitionMetrics

	// RefreshMetrics refreshes the metrics of dp write operate
	RefreshMetrics(enableRemote bool, dpMetrics map[uint64]*proto.DataPartitionMetrics) error
}

type DpSelectorParam struct {
	kValue	string
	quorum	int
}

var (
	dataPartitionSelectorConstructors = make(map[string]DataPartitionSelectorConstructor)

	ErrDuplicatedDataPartitionSelectorConstructor = errors.New("duplicated data partition selector constructor")
	ErrDataPartitionSelectorConstructorNotExist   = errors.New("data partition selector constructor not exist")
)

// RegisterDataPartitionSelector registers a selector constructor.
// Users can register their own defined selector through this method.
func RegisterDataPartitionSelector(name string, constructor DataPartitionSelectorConstructor) error {
	var clearName = strings.TrimSpace(strings.ToLower(name))
	if _, exist := dataPartitionSelectorConstructors[clearName]; exist {
		return ErrDuplicatedDataPartitionSelectorConstructor
	}
	dataPartitionSelectorConstructors[clearName] = constructor
	return nil
}

func newDataPartitionSelector(name string, param *DpSelectorParam) (newDpSelector DataPartitionSelector, err error) {
	var clearName = strings.TrimSpace(strings.ToLower(name))
	constructor, exist := dataPartitionSelectorConstructors[clearName]
	if !exist {
		return nil, ErrDataPartitionSelectorConstructorNotExist
	}
	return constructor(param)
}

func (w *Wrapper) initDpSelector() (err error) {
	w.dpSelectorChanged = false
	var selectorName = w.dpSelectorName
	if strings.TrimSpace(selectorName) == "" {
		log.LogDebugf("initDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
		selectorName = DefaultRandomSelectorName
	}
	dpSelectorParam := &DpSelectorParam{
		kValue: w.dpSelectorParm,
		quorum: w.quorum,
	}
	var selector DataPartitionSelector
	if selector, err = newDataPartitionSelector(selectorName, dpSelectorParam); err != nil {
		log.LogErrorf("initDpSelector: dpSelector[%v] init failed caused by [%v], use default selector", w.dpSelectorName,
			err)
		return
	}
	w.dpSelector = selector
	return
}

func (w *Wrapper) refreshDpSelector(partitions []*DataPartition) {
	w.RLock()
	dpSelector := w.dpSelector
	dpSelectorChanged := w.dpSelectorChanged
	w.RUnlock()

	if dpSelectorChanged {
		var selectorName = w.dpSelectorName
		if strings.TrimSpace(selectorName) == "" {
			log.LogWarnf("refreshDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
			selectorName = DefaultRandomSelectorName
		}
		dpSelectorParam := &DpSelectorParam{
			kValue: w.dpSelectorParm,
			quorum: w.quorum,
		}
		newDpSelector, err := newDataPartitionSelector(selectorName, dpSelectorParam)
		if err != nil {
			log.LogErrorf("refreshDpSelector: change dpSelector to [%v %v %v] failed caused by [%v],"+
				" use last valid selector. Please change dpSelector config through master.",
				w.dpSelectorName, w.dpSelectorParm, w.quorum, err)
		} else {
			_ = newDpSelector.Refresh(partitions)
			w.Lock()
			log.LogInfof("refreshDpSelector: change dpSelector to [%v %v %v]", w.dpSelectorName, w.dpSelectorParm, w.quorum)
			w.dpSelector = newDpSelector
			w.dpSelectorChanged = false
			w.Unlock()
			return
		}
	}

	_ = dpSelector.Refresh(partitions)
}

// getDataPartitionForWrite returns an available data partition for write.
func (w *Wrapper) GetDataPartitionForWrite(exclude map[string]struct{}) (*DataPartition, error) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.Select(exclude)
}

func (w *Wrapper) RemoveDataPartitionForWrite(partitionID uint64) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.RemoveDP(partitionID)
}

func (w *Wrapper) RefreshDataPartitionMetrics(enableRemote bool, dpMetricsMap map[uint64]*proto.DataPartitionMetrics) error {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.RefreshMetrics(enableRemote, dpMetricsMap)
}

func (w *Wrapper) SummaryDataPartitionMetrics() []*proto.DataPartitionMetrics {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.SummaryMetrics()
}
