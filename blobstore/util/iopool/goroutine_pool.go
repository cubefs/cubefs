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

package iopool

import "runtime"

type GoroutineWrap struct {
	routineChan chan func()
}

func (routine *GoroutineWrap) start() {
	go func() {
		for {
			task := <-routine.routineChan
			if task == nil {
				return
			}
			task()
		}
	}()
}

func NewGoroutineWarp() *GoroutineWrap {
	routine := &GoroutineWrap{
		routineChan: make(chan func()),
	}
	routine.start()
	return routine
}

func (routine *GoroutineWrap) Close() {
	close(routine.routineChan)
}

func (routine *GoroutineWrap) Submit(task func()) {
	routine.routineChan <- task
}

type GoroutinePool struct {
	routines  []*GoroutineWrap
	freeChans chan *GoroutineWrap
}

func NewGoroutinePool(count uint32) *GoroutinePool {
	if count == 0 {
		count = uint32(runtime.NumCPU())
	}
	routines := make([]*GoroutineWrap, count)
	freeChains := make(chan *GoroutineWrap, count)
	for i := uint32(0); i < count; i++ {
		routine := NewGoroutineWarp()
		routines[i] = routine
		freeChains <- routine
	}
	return &GoroutinePool{
		routines:  routines,
		freeChans: freeChains,
	}
}

func (pool *GoroutinePool) GetCount() uint32 {
	return uint32(len(pool.routines))
}

func (pool *GoroutinePool) RequireOne() *GoroutineWrap {
	return <-pool.freeChans
}

func (pool *GoroutinePool) ReleaseOne(routine *GoroutineWrap) {
	pool.freeChans <- routine
}

func (pool *GoroutinePool) Close() {
	for _, routine := range pool.routines {
		routine.Close()
	}
}
