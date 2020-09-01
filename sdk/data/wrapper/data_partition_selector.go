// Copyright 2020 The Chubao Authors.
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
	"strings"

	"github.com/chubaofs/chubaofs/util/log"
)

type DataPartitionSelector interface {
	InitFunc(string) error
	RefreshFunc([]*DataPartition) error
	SelectFunc(map[string]struct{}) (*DataPartition, error)
	RemoveDpFunc(uint64)
}

func (w *Wrapper) newDpSelector() (newDpSelector DataPartitionSelector, err error) {
	if strings.EqualFold(w.dpSelectorName, "kfaster") {
		newDpSelector = &KFasterRandomSelector{}
	} else if strings.EqualFold(w.dpSelectorName, "default") {
		newDpSelector = &DefaultRandomSelector{}
	} else {
		return nil, errors.New("no match dataPartitionSelector type")
	}

	return newDpSelector, newDpSelector.InitFunc(w.dpSelectorParm)
}

func (w *Wrapper) initDpSelector() (err error) {
	w.dpSelectorChanged = false
	if strings.EqualFold(w.dpSelectorName, "kfaster") {
		w.dpSelector = &KFasterRandomSelector{}
	}

	if w.dpSelector != nil {
		err = w.dpSelector.InitFunc(w.dpSelectorParm)
		if err == nil {
			return
		}
		log.LogErrorf("initDpSelector: dpSelector[%v] init failed caused by [%v], use default selector", w.dpSelectorName,
			err)
	}

	if w.dpSelectorName != "" {
		log.LogErrorf("initDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
	}

	w.dpSelector = &DefaultRandomSelector{}

	return w.dpSelector.InitFunc(w.dpSelectorParm)
}

func (w *Wrapper) refreshDpSelector(partitions []*DataPartition) {
	w.RLock()
	dpSelector := w.dpSelector
	dpSelectorChanged := w.dpSelectorChanged
	w.RUnlock()

	if dpSelectorChanged {
		newDpSelector, err := w.newDpSelector()
		if err != nil {
			log.LogErrorf("refreshDpSelector: change dpSelector to [%v %v] failed caused by [%v],"+
				" use last valid selector. Please change dpSelector config through master.",
				w.dpSelectorName, w.dpSelectorParm, err)
		} else {
			w.Lock()
			log.LogInfof("refreshDpSelector: change dpSelector to [%v %v]", w.dpSelectorName, w.dpSelectorParm)
			w.dpSelector = newDpSelector
			w.dpSelectorChanged = false
			dpSelector = newDpSelector
			w.Unlock()
		}
	}

	dpSelector.RefreshFunc(partitions)
}

// getDataPartitionForWrite returns an available data partition for write.
func (w *Wrapper) GetDataPartitionForWrite(exclude map[string]struct{}) (*DataPartition, error) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.SelectFunc(exclude)
}

func (w *Wrapper) RemoveDataPartitionForWrite(partitionID uint64) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.RemoveDpFunc(partitionID)
}
