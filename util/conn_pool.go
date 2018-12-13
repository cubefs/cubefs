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

package util

import (
	"net"
	"strings"
	"sync"
	"time"
)

type ConnectObject struct {
	conn *net.TCPConn
	idle int64
}

type ConnectPool struct {
	sync.RWMutex
	pools   map[string]*Pool
	mincap  int
	maxcap  int
	timeout int64
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{pools: make(map[string]*Pool), mincap: 5, maxcap: 100, timeout: int64(time.Minute)}
	go cp.autoRelease()

	return cp
}

func (cp *ConnectPool) Get(targetAddr string) (c *net.TCPConn, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, targetAddr)
		cp.pools[targetAddr] = pool
		cp.Unlock()
	}

	return pool.Get()
}

func (cp *ConnectPool) Put(c *net.TCPConn, forceClose bool) {
	if c == nil {
		return
	}
	if forceClose {
		c.Close()
		return
	}
	addr := c.RemoteAddr().String()
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		c.CloseWrite()
		c.CloseRead()
		c.Close()
		return
	}
	object := &ConnectObject{conn: c, idle: time.Now().UnixNano()}
	pool.put(object)

	return
}

func (cp *ConnectPool) CheckErrorForceClose(c *net.TCPConn, target string, err error) {
	if c == nil {
		return
	}

	if err != nil {
		if strings.Contains(err.Error(), "use of closed network connection") {
			c.Close()
			cp.ReleaseAllConnect(target)
			return
		} else {
			c.Close()
			return
		}
	}
}

func (cp *ConnectPool) CheckErrorForPutConnect(c *net.TCPConn, target string, err error) {
	if c == nil {
		return
	}

	if err != nil {
		if strings.Contains(err.Error(), "use of closed network connection") {
			c.Close()
			cp.ReleaseAllConnect(target)
			return
		} else {
			c.Close()
			return
		}
	}
	addr := c.RemoteAddr().String()
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		c.Close()
		return
	}
	object := &ConnectObject{conn: c, idle: time.Now().UnixNano()}
	pool.put(object)
}

func (cp *ConnectPool) ReleaseAllConnect(target string) {
	cp.RLock()
	pool := cp.pools[target]
	cp.RUnlock()
	if pool != nil {
		pool.ForceReleaseAllConnect()
	}
}

func (cp *ConnectPool) autoRelease() {
	for {
		pools := make([]*Pool, 0)
		cp.RLock()
		for _, pool := range cp.pools {
			pools = append(pools, pool)
		}
		cp.RUnlock()
		for _, pool := range pools {
			pool.AutoRelease()
		}
		time.Sleep(time.Minute)
	}

}

type Pool struct {
	pool    chan *ConnectObject
	mincap  int
	maxcap  int
	target  string
	timeout int64
}

func NewPool(min, max int, timeout int64, target string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.pool = make(chan *ConnectObject, max)
	p.timeout = timeout
	p.initAllConnect()
	return p
}

func (p *Pool) initAllConnect() {
	for i := 0; i < p.mincap; i++ {
		c, err := net.Dial("tcp", p.target)
		if err == nil {
			conn := c.(*net.TCPConn)
			conn.SetKeepAlive(true)
			conn.SetNoDelay(true)
			obj := &ConnectObject{conn: conn, idle: time.Now().UnixNano()}
			p.put(obj)
		}
	}
}

func (p *Pool) put(c *ConnectObject) {
	select {
	case p.pool <- c:
		return
	default:
		if c.conn != nil {
			c.conn.Close()
		}
		return
	}
}

func (p *Pool) get() (c *ConnectObject) {
	select {
	case c = <-p.pool:
		return
	default:
		return
	}
}

func (p *Pool) AutoRelease() {
	connectLen := len(p.pool)
	for i := 0; i < connectLen; i++ {
		select {
		case c := <-p.pool:
			if time.Now().UnixNano()-int64(c.idle) > p.timeout {
				c.conn.Close()
			} else {
				p.put(c)
			}
		default:
			return
		}
	}
}

func (p *Pool) ForceReleaseAllConnect() {
	for {
		select {
		case c := <-p.pool:
			c.conn.Close()
		default:
			return
		}
	}
}

func (p *Pool) Get() (c *net.TCPConn, err error) {
	obj := p.get()
	if obj != nil {
		return obj.conn, nil
	}
	var connect net.Conn
	connect, err = net.Dial("tcp", p.target)
	if err == nil {
		conn := connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		c = conn
	}
	return
}
