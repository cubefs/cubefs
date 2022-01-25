package common

import (
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util/config"
)

const (
	StateStandby uint32 = iota
	StateStart
	StateRunning
	StateShutdown
	StateStopped
)

type Control struct {
	state uint32
	wg    sync.WaitGroup
}

type Server interface {
	Start(cfg *config.Config) error
	Shutdown()
	// Sync will block invoker goroutine until this MetaNode shutdown.
	Sync()
}

type DoStartFunc func(s Server, cfg *config.Config) (err error)
type DoShutdownFunc func(s Server)

func (c *Control) Start(s Server, cfg *config.Config, do DoStartFunc) (err error) {
	if atomic.CompareAndSwapUint32(&c.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&c.state, newState)
		}()
		if err = do(s, cfg); err != nil {
			return
		}
		c.wg.Add(1)
	}
	return

}

func (c *Control) Shutdown(s Server, do DoShutdownFunc) {
	if atomic.CompareAndSwapUint32(&c.state, StateRunning, StateShutdown) {
		do(s)
		c.wg.Done()
		atomic.StoreUint32(&c.state, StateStopped)
	}

}

func (c *Control) Sync() {
	if atomic.LoadUint32(&c.state) == StateRunning {
		c.wg.Wait()
	}
}
