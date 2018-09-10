// Copyright 2018 The Containerfs Authors.
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

package pool

import (
	"fmt"
	"sync"
	"time"
)

type PoolConfig struct {
	Para        interface{}
	InitialCap  int
	MaxCap      int
	Factory     func(para interface{}) (interface{}, error)
	Test        func(item interface{}) bool
	Close       func(interface{}) error
	IdleTimeout time.Duration
}

// ChannelPool is an implementation of Pool based on go chan.
type ChannelPool struct {
	para        interface{}
	mu          sync.Mutex
	conns       chan *IdleConn
	factory     func(para interface{}) (interface{}, error)
	test        func(item interface{}) bool
	close       func(interface{}) error
	idleTimeout time.Duration
}

type IdleConn struct {
	conn interface{}
	t    time.Time
}

// NewChannelPool create and returns an new Pool instance with specified configuration.
func NewChannelPool(poolConfig *PoolConfig) (Pool, error) {
	c := &ChannelPool{
		conns:       make(chan *IdleConn, poolConfig.MaxCap),
		factory:     poolConfig.Factory,
		close:       poolConfig.Close,
		idleTimeout: poolConfig.IdleTimeout,
		para:        poolConfig.Para,
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory(poolConfig.Para)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &IdleConn{conn: conn, t: time.Now()}
	}

	return c, nil
}

func (c *ChannelPool) getConns() chan *IdleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get a resource from resource pool.
func (c *ChannelPool) Get() (interface{}, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return c.factory(c.para)
			}
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					c.Close(wrapConn.conn)
					continue
				}
			}
			return wrapConn.conn, nil
		default:
			conn, err := c.factory(c.para)
			if err != nil {
				return nil, err
			}

			return conn, nil
		}
	}
}

func (c *ChannelPool) AutoRelease() {
	conns := c.getConns()
	if conns == nil {
		return
	}
	connLen := len(conns)
	for i := 0; i < connLen; i++ {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return
			}
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					c.Close(wrapConn.conn)
					continue
				}
			}
			c.putNotModifyTime(wrapConn)
		default:
			return
		}
	}
}

func (c *ChannelPool) putNotModifyTime(conn *IdleConn) {
	select {
	case c.getConns() <- conn:
	default:
		return
	}
}

// Put a resource to resource pool.
func (c *ChannelPool) Put(conn interface{}) error {
	if conn == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		return c.Close(conn)
	}

	select {
	case c.conns <- &IdleConn{conn: conn, t: time.Now()}:
		return nil
	default:
		return c.Close(conn)
	}
}

// Close tries close specified resource.
func (c *ChannelPool) Close(conn interface{}) error {
	if conn == nil {
		return nil
	}
	return c.close(conn)
}

// Release all resource entity stored in pool.
func (c *ChannelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}
}

// Len returns number of resource stored in pool.
func (c *ChannelPool) Len() int {
	return len(c.getConns())
}
