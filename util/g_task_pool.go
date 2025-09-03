package util

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPoolFactor      = 8
	waitTimeout            = 100 * time.Millisecond
	defaultMaxDeltaRunning = 2000
)

type GTaskPool struct {
	wg              sync.WaitGroup
	once            sync.Once
	running         int32
	concurrency     int
	stopCh          chan struct{}
	queue           chan *PoolTask
	busyThreshold   int32
	deltaRunning    int32
	maxDeltaRunning int32
	waitTime        time.Duration
}

type PoolTask struct {
	fn         func()
	done       chan struct{}
	err        error
	aNoHang    bool
	submitTime time.Time
}

type PoolStatus struct {
	Concurrency int
	QueueSize   int
	Running     int
	Waiting     int
}

var PoolClosedError = errors.New("pool is closed")

func NewGTaskPool(concurrency int) *GTaskPool {
	return NewGTaskPoolEx(concurrency, 0)
}

func NewGTaskPoolEx(concurrency, factor int) *GTaskPool {
	pool := &GTaskPool{
		concurrency:     concurrency,
		busyThreshold:   int32(concurrency - 4),
		maxDeltaRunning: defaultMaxDeltaRunning,
		waitTime:        waitTimeout,
	}
	if pool.concurrency <= 0 {
		return pool
	}
	if factor <= 0 {
		factor = defaultPoolFactor
	}
	pool.stopCh = make(chan struct{})
	pool.queue = make(chan *PoolTask, factor*concurrency)
	pool.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer pool.wg.Done()
			for {
				select {
				case <-pool.stopCh:
					return
				case task := <-pool.queue:
					cRunning := atomic.AddInt32(&pool.running, 1)
					if task.aNoHang && time.Since(task.submitTime) > pool.waitTime && cRunning > pool.busyThreshold && atomic.LoadInt32(&pool.deltaRunning) < pool.maxDeltaRunning {
						atomic.AddInt32(&pool.deltaRunning, 1)
						go func() {
							task.fn()
							atomic.AddInt32(&pool.deltaRunning, -1)
						}()
					} else {
						task.fn()
					}
					atomic.AddInt32(&pool.running, -1)
					close(task.done)
				}
			}
		}()
	}
	return pool
}

func (p *GTaskPool) AsyncRunNoHang(taskFn func()) error {
	select {
	case <-p.stopCh:
		return PoolClosedError
	default:
	}
	t := &PoolTask{
		fn:         taskFn,
		done:       make(chan struct{}),
		aNoHang:    true,
		submitTime: time.Now(),
	}
	p.queue <- t
	return nil
}

func (p *GTaskPool) AsyncRun(taskFn func()) error {
	select {
	case <-p.stopCh:
		return PoolClosedError
	default:
	}
	t := &PoolTask{
		fn:   taskFn,
		done: make(chan struct{}),
	}
	p.queue <- t
	return nil
}

func (p *GTaskPool) Run(taskFn func()) error {
	select {
	case <-p.stopCh:
		return PoolClosedError
	default:
	}
	t := &PoolTask{
		fn:   taskFn,
		done: make(chan struct{}),
	}
	p.queue <- t
	<-t.done
	return t.err
}

func (p *GTaskPool) Status() *PoolStatus {
	return &PoolStatus{
		Concurrency: p.concurrency,
		QueueSize:   cap(p.queue),
		Running:     int(atomic.LoadInt32(&p.running)),
		Waiting:     len(p.queue),
	}
}

func (p *GTaskPool) Close() {
	p.once.Do(func() {
		if p.concurrency > 0 {
			close(p.stopCh)
		}
	})
	p.wg.Wait()

	go func() {
		waitTimer := time.NewTimer(time.Minute)
		defer waitTimer.Stop()
		for {
			select {
			case t := <-p.queue:
				t.fn()
				close(t.done)
				waitTimer.Reset(time.Minute)
			case <-waitTimer.C:
				return
			}
		}
	}()
}

func (p *GTaskPool) SetMaxDeltaRunning(max int32) {
	if max <= 0 {
		max = defaultMaxDeltaRunning
	}
	atomic.StoreInt32(&p.maxDeltaRunning, max)
}

func (p *GTaskPool) SetWaitTime(wait time.Duration) {
	if wait <= 0 {
		wait = waitTimeout
	}
	p.waitTime = wait
}
