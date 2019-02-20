// Copyright 2018 The Chubao Authors.
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

type Object struct {
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

func (cp *ConnectPool) GetConnect(targetAddr string) (c *net.TCPConn, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, targetAddr)
		cp.pools[targetAddr] = pool
		cp.Unlock()
	}

	return pool.GetConnectFromPool()
}

func (cp *ConnectPool) PutConnect(c *net.TCPConn, forceClose bool) {
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
		c.Close()
		return
	}
	object := &Object{conn: c, idle: time.Now().UnixNano()}
	pool.PutConnectObjectToPool(object)

	return
}

func (cp *ConnectPool) ForceDestory(c *net.TCPConn) {
	if c == nil {
		return
	}
	c.Close()
}

func (cp *ConnectPool) ForceDestoryWholePool(c *net.TCPConn, target string, err error) {
	if c == nil {
		return
	}
	c.Close()
	if err != nil && strings.Contains(err.Error(), "use of") {
		cp.ReleaseAllConnect(target)
	}
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
		time.Sleep(time.Second)
	}

}

type Pool struct {
	objects chan *Object
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
	p.objects = make(chan *Object, max)
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

func (p *Pool) AutoRelease() {
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

func (p *Pool) ForceReleaseAllConnect() {
	for {
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
	connect, err = net.Dial("tcp", p.target)
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
		obj *Object
	)
	select {
	case obj = <-p.objects:
		break
	default:
		break
	}
	if obj != nil {
		return obj.conn, nil
	}
	return p.NewConnect(p.target)
}
