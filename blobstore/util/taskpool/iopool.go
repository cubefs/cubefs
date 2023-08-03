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

package taskpool

type IoPool interface {
	Submit(taskId uint64, taskFn func())
	WaitDone(func())
	Close()
}

type IoPoolImpl struct {
	pools []*IoPoolSimple
}

func NewIoPool(poolCnt, workCnt, queueLen int) IoPool {
	p := &IoPoolImpl{
		pools: make([]*IoPoolSimple, poolCnt),
	}

	for i := range p.pools {
		p.pools[i] = newIoPoolSimple(workCnt, queueLen)
	}

	return p
}

func (p *IoPoolImpl) Submit(taskId uint64, taskFn func()) {
	idx := taskId % uint64(len(p.pools))
	p.pools[idx].queue <- taskFn
}

func (p *IoPoolImpl) WaitDone(fn func()) {
	fn()
}

func (p *IoPoolImpl) Close() {
	for _, p := range p.pools {
		p.tp.Close()
	}
}

type IoPoolSimple struct {
	queue chan func()
	tp    TaskPool
}

func newIoPoolSimple(workCnt, queueLen int) *IoPoolSimple {
	pool := &IoPoolSimple{
		queue: make(chan func(), queueLen),
		tp:    New(workCnt, workCnt),
	}

	for j := 0; j < workCnt; j++ {
		pool.tp.Run(pool.runLoop)
	}
	return pool
}

func (s *IoPoolSimple) runLoop() {
	for task := range s.queue {
		task()
	}
}
