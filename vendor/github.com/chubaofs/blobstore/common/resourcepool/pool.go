package resourcepool

// sync.Pool cache will be released by runtime.GC()
// see sync/pool.go: runtime_registerPoolCleanup(poolCleanup)

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrPoolLimit pool elements exceed its capacity
	ErrPoolLimit = errors.New("resource pool limit")
)

// Pool resource pool support for sync.pool and capacity limit
// release resource if no used anymore
// no limit if capacity is negative
type Pool interface {
	// Get return nil and error if exceed pool's capacity
	Get() (interface{}, error)
	Put(x interface{})
	Cap() int
	Len() int
}

type pool struct {
	sp      sync.Pool
	cap     int32
	current int32
}

// NewPool return Pool with capacity, no limit if capacity is negative
func NewPool(new func() interface{}, cap int) Pool {
	return &pool{
		sp:      sync.Pool{New: new},
		cap:     int32(cap),
		current: int32(0),
	}
}

func (p *pool) Get() (interface{}, error) {
	if current := atomic.AddInt32(&p.current, 1); p.cap >= 0 && current > p.cap {
		atomic.AddInt32(&p.current, -1)
		return nil, ErrPoolLimit
	}
	return p.sp.Get(), nil
}

func (p *pool) Put(x interface{}) {
	p.sp.Put(x)
	atomic.AddInt32(&p.current, -1)
}

func (p *pool) Cap() int {
	return int(p.cap)
}

func (p *pool) Len() int {
	return int(atomic.LoadInt32(&p.current))
}
