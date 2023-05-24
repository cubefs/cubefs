// Copyright 2018 The CubeFS Authors.
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
	"sync"
	"time"
)

type Object struct {
	conn *net.TCPConn
	idle int64
}

const (
	ConnectIdleTime       = 30
	defaultConnectTimeout = 1
)

type ConnectPool struct {
	sync.RWMutex
	pools          map[string]*Pool
	mincap         int
	maxcap         int
	timeout        int64
	connectTimeout int64
	closeCh        chan struct{}
	closeOnce      sync.Once
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:          make(map[string]*Pool),
		mincap:         5,
		maxcap:         500,
		timeout:        int64(time.Second * ConnectIdleTime),
		connectTimeout: defaultConnectTimeout,
		closeCh:        make(chan struct{}),
	}
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout int64) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:          make(map[string]*Pool),
		mincap:         5,
		maxcap:         80,
		timeout:        int64(idleConnTimeout * time.Second),
		connectTimeout: connectTimeout,
		closeCh:        make(chan struct{}),
	}
	go cp.autoRelease()

	return cp
}

func DailTimeOut(target string, timeout time.Duration) (c *net.TCPConn, err error) {
	var connect net.Conn
	connect, err = net.DialTimeout("tcp", target, timeout)
	if err == nil {
		conn := connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		c = conn
	}
	return
}

func (cp *ConnectPool) GetConnect(targetAddr string) (c *net.TCPConn, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		newPool := NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
		cp.Lock()
		pool, ok = cp.pools[targetAddr]
		if !ok {
			//pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
			pool = newPool
			cp.pools[targetAddr] = pool
		}
		cp.Unlock()
	}

	return pool.GetConnectFromPool()
}

func (cp *ConnectPool) PutConnect(c *net.TCPConn, forceClose bool) {
	if c == nil {
		return
	}
	if forceClose {
		_ = c.Close()
		return
	}
	select {
	case <-cp.closeCh:
		_ = c.Close()
		return
	default:
	}
	addr := c.RemoteAddr().String()
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		c.Close()
		return
	}
	object := &Object{conn: c, idle: time.Now().UnixNano()}
	pool.PutConnectObjectToPool(object)
	return
}

func (cp *ConnectPool) autoRelease() {
	var timer = time.NewTimer(time.Second)
	for {
		select {
		case <-cp.closeCh:
			timer.Stop()
			return
		case <-timer.C:
		}
		pools := make([]*Pool, 0)
		cp.RLock()
		for _, pool := range cp.pools {
			pools = append(pools, pool)
		}
		cp.RUnlock()
		for _, pool := range pools {
			pool.autoRelease()
		}
		timer.Reset(time.Second)
	}
}

func (cp *ConnectPool) releaseAll() {
	pools := make([]*Pool, 0)
	cp.RLock()
	for _, pool := range cp.pools {
		pools = append(pools, pool)
	}
	cp.RUnlock()
	for _, pool := range pools {
		pool.ReleaseAll()
	}
}

func (cp *ConnectPool) Close() {
	cp.closeOnce.Do(func() {
		close(cp.closeCh)
		cp.releaseAll()
	})
}

type Pool struct {
	objects        chan *Object
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
}

func NewPool(min, max int, timeout, connectTimeout int64, target string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *Object, max)
	p.timeout = timeout
	p.connectTimeout = connectTimeout
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
			o := &Object{conn: conn, idle: time.Now().UnixNano()}
			p.PutConnectObjectToPool(o)
		}
	}
}

func (p *Pool) PutConnectObjectToPool(o *Object) {
	select {
	case p.objects <- o:
		return
	default:
		if o.conn != nil {
			o.conn.Close()
		}
		return
	}
}

func (p *Pool) autoRelease() {
	connectLen := len(p.objects)
	for i := 0; i < connectLen; i++ {
		select {
		case o := <-p.objects:
			if time.Now().UnixNano()-int64(o.idle) > p.timeout {
				o.conn.Close()
			} else {
				p.PutConnectObjectToPool(o)
			}
		default:
			return
		}
	}
}

func (p *Pool) ReleaseAll() {
	connectLen := len(p.objects)
	for i := 0; i < connectLen; i++ {
		select {
		case o := <-p.objects:
			o.conn.Close()
		default:
			return
		}
	}
}

func (p *Pool) NewConnect(target string) (c *net.TCPConn, err error) {
	var connect net.Conn
	connect, err = net.DialTimeout("tcp", p.target, time.Duration(p.connectTimeout)*time.Second)
	if err == nil {
		conn := connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		c = conn
	}
	return
}

func (p *Pool) GetConnectFromPool() (c *net.TCPConn, err error) {
	var (
		o *Object
	)
	for {
		select {
		case o = <-p.objects:
		default:
			return p.NewConnect(p.target)
		}
		if time.Now().UnixNano()-int64(o.idle) > p.timeout {
			_ = o.conn.Close()
			o = nil
			continue
		}
		return o.conn, nil
	}
}
