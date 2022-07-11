// Copyright 2022 The CubeFS Authors.
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

package core

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type Request struct {
	item      interface{}
	timestamp uint64
}

type ConsistencyController struct {
	lock      sync.Mutex
	cond      *sync.Cond
	reqs      list.List
	timestamp uint64
}

func (cc *ConsistencyController) Begin(item interface{}) (elem *list.Element) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	curtime := atomic.AddUint64(&cc.timestamp, 1)

	req := Request{
		item:      item,
		timestamp: curtime,
	}

	elem = cc.reqs.PushBack(req)

	return elem
}

func (cc *ConsistencyController) End(elem *list.Element) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	cc.reqs.Remove(elem)
	cc.cond.Broadcast()
}

func (cc *ConsistencyController) synchronize(anchor uint64) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

loopCheck:
	for e := cc.reqs.Front(); e != nil; {
		req := e.Value.(Request)
		if req.timestamp <= anchor {
			cc.cond.Wait()
			goto loopCheck
		}
		break
	}
}

func (cc *ConsistencyController) CurrentTime() uint64 {
	cc.lock.Lock()
	timestamp := atomic.LoadUint64(&cc.timestamp)
	cc.lock.Unlock()
	return timestamp
}

func (cc *ConsistencyController) Synchronize() uint64 {
	timestamp := cc.CurrentTime()
	cc.synchronize(timestamp)
	return timestamp
}

func NewConsistencyController() (cc *ConsistencyController) {
	cc = &ConsistencyController{}
	cc.cond = sync.NewCond(&cc.lock)
	return cc
}
