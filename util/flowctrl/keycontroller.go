// Copyright 2023 The CubeFS Authors.
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

package flowctrl

import (
	"sync"
)

type ctrlWrapper struct {
	c        *Controller
	refCount int
}

func newCtrlWrapper(rate int) *ctrlWrapper {
	return &ctrlWrapper{
		c:        NewController(rate),
		refCount: 0,
	}
}

type KeyFlowCtrl struct {
	mutex   sync.RWMutex
	current map[string]*ctrlWrapper // uid -> controller
}

func NewKeyFlowCtrl() *KeyFlowCtrl {
	return &KeyFlowCtrl{current: make(map[string]*ctrlWrapper)}
}

func (k *KeyFlowCtrl) Acquire(key string, rate int) *Controller {
	k.mutex.Lock()
	ctrl, ok := k.current[key]
	if !ok {
		ctrl = newCtrlWrapper(rate)
		k.current[key] = ctrl
	}
	ctrl.refCount++
	k.mutex.Unlock()
	return ctrl.c
}

func (k *KeyFlowCtrl) Release(key string) {
	k.mutex.Lock()
	defer k.mutex.Unlock()
	ctrl, ok := k.current[key]
	if !ok {
		panic("key not in map. Possible reason: Release without Acquire.")
	}
	ctrl.refCount--
	if ctrl.refCount < 0 {
		panic("internal error: refs < 0")
	}
	if ctrl.refCount == 0 {
		ctrl.c.Close() // avoid goroutine leak
		delete(k.current, key)
	}
}
