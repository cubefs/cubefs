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
	"context"
	"errors"
	"github.com/cubefs/cubefs/proto"
	"strings"
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
	Select(ctx context.Context, excludes map[string]struct{}) (*DataPartition, error)

	// RemoveDP removes specified data partition.
	RemoveDP(partitionID uint64)

	// Count return number of data partitions held by selector.
	Count() int
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

func (w *Wrapper) initDpSelector(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	w.dpSelectorChanged = false
	selectorName := w.dpSelectorName
	if strings.TrimSpace(selectorName) == "" {
		span.Infof("initDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
		selectorName = DefaultRandomSelectorName
	}
	var selector DataPartitionSelector
	if selector, err = newDataPartitionSelector(selectorName, w.dpSelectorParm); err != nil {
		span.Errorf("initDpSelector: dpSelector[%v] init failed caused by [%v], use default selector", w.dpSelectorName,
			err)
		return
	}
	w.dpSelector = selector
	return
}

func (w *Wrapper) refreshDpSelector(ctx context.Context, partitions []*DataPartition) {
	span := proto.SpanFromContext(ctx)
	w.Lock.RLock()
	dpSelector := w.dpSelector
	dpSelectorChanged := w.dpSelectorChanged
	w.Lock.RUnlock()

	if dpSelectorChanged {
		selectorName := w.dpSelectorName
		if strings.TrimSpace(selectorName) == "" {
			span.Warnf("refreshDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
			selectorName = DefaultRandomSelectorName
		}
		newDpSelector, err := newDataPartitionSelector(selectorName, w.dpSelectorParm)
		if err != nil {
			span.Errorf("refreshDpSelector: change dpSelector to [%v %v] failed caused by [%v],"+
				" use last valid selector. Please change dpSelector config through master.",
				w.dpSelectorName, w.dpSelectorParm, err)
		} else {
			w.Lock.Lock()
			span.Infof("refreshDpSelector: change dpSelector to [%v %v]", w.dpSelectorName, w.dpSelectorParm)
			w.dpSelector = newDpSelector
			w.dpSelectorChanged = false
			dpSelector = newDpSelector
			w.Lock.Unlock()
		}
	}

	_ = dpSelector.Refresh(partitions)
}

// getDataPartitionForWrite returns an available data partition for write.
func (w *Wrapper) GetDataPartitionForWrite(ctx context.Context, exclude map[string]struct{}) (*DataPartition, error) {
	w.Lock.RLock()
	dpSelector := w.dpSelector
	w.Lock.RUnlock()

	return dpSelector.Select(ctx, exclude)
}

func (w *Wrapper) RemoveDataPartitionForWrite(partitionID uint64) {
	w.Lock.RLock()
	dpSelector := w.dpSelector
	w.Lock.RUnlock()

	if dpSelector.Count() <= 1 {
		return
	}

	dpSelector.RemoveDP(partitionID)
}
