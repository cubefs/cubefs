package routinepool

import (
	"github.com/cubefs/cubefs/util/errors"
	"sync/atomic"
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
