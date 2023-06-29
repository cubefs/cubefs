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

package routinepool

import (
	"sync/atomic"

	"github.com/cubefs/cubefs/util/errors"
)

const (
	DefaultMaxRoutineNum = 100
)

type RoutinePool struct {
	maxRoutineNum int
	ticketBucket  chan int
	runningNum    int32
}

var (
	ErrorClosed = errors.New("pool is closed")
)

func NewRoutinePool(maxRoutineNum int) *RoutinePool {
	if maxRoutineNum <= 0 {
		maxRoutineNum = DefaultMaxRoutineNum
	}

	pool := &RoutinePool{
		maxRoutineNum: maxRoutineNum,
		ticketBucket:  make(chan int, maxRoutineNum),
		runningNum:    0,
	}

	for i := 0; i < pool.maxRoutineNum; i++ {
		pool.ticketBucket <- i
	}
	return pool
}

func (p *RoutinePool) RunningNum() int32 {
	return atomic.LoadInt32(&p.runningNum)
}

func (p *RoutinePool) Submit(task func()) (int, error) {
	ticket, ok := <-p.ticketBucket
	if !ok {
		return -1, ErrorClosed
	}

	atomic.AddInt32(&p.runningNum, 1)

	go func() {
		defer func() {
			p.ticketBucket <- ticket
			atomic.AddInt32(&p.runningNum, -1)
		}()

		task()
	}()
	return ticket, nil
}

func (p *RoutinePool) WaitAndClose() {
	for i := 0; i < p.maxRoutineNum; i++ {
		<-p.ticketBucket
	}
	p.close()
}

func (p *RoutinePool) close() {
	close(p.ticketBucket)
}
