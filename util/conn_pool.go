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
	"container/list"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type Object struct {
	conn *net.TCPConn
	idle int64
	cost int64
}

const (
	ConnectIdleTime       = 30
	defaultConnectTimeout = 1
)

type ConnectPool struct {
	sync.RWMutex
	pools          map[string]PoolInterface
	mincap         int
	maxcap         int
	timeout        int64
	connectTimeout int64
	closeCh        chan struct{}
	closeOnce      sync.Once
	useMilliSecond bool
	useCostPool    bool
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:          make(map[string]PoolInterface),
		mincap:         5,
		maxcap:         500,
		timeout:        int64(time.Second * ConnectIdleTime),
		connectTimeout: defaultConnectTimeout,
		closeCh:        make(chan struct{}),
	}
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout int64, useMilliSecond bool) (cp *ConnectPool) {
	return NewConnectPoolWithTimeoutAndCap(5, 80, idleConnTimeout, connectTimeout, useMilliSecond)
}

func NewConnectPoolWithTimeoutAndCap(minCap, maxCap int, idleConnTimeout time.Duration, connectTimeout int64, useMilliSecond bool) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:          make(map[string]PoolInterface),
		mincap:         minCap,
		maxcap:         maxCap,
		timeout:        int64(idleConnTimeout * time.Second),
		connectTimeout: connectTimeout,
		closeCh:        make(chan struct{}),
		useMilliSecond: useMilliSecond,
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

func (cp *ConnectPool) SetPoolArgs(timeout int64, minCap int) {
	cp.timeout = int64(time.Duration(timeout) * time.Second)
	cp.mincap = minCap
}

func (cp *ConnectPool) SetUseCostPool(useCostPool bool) {
	cp.useCostPool = useCostPool
}

func (cp *ConnectPool) GetConnect(targetAddr string) (c *net.TCPConn, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool, ok = cp.pools[targetAddr]
		if !ok {
			var newPool PoolInterface
			if cp.useCostPool {
				newPool = NewPoolWithCost(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr, cp.useMilliSecond)
			} else {
				newPool = NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr, cp.useMilliSecond)
			}
			pool = newPool
			cp.pools[targetAddr] = pool
		}
		cp.Unlock()
	}

	return pool.GetConnectFromPool()
}

func (cp *ConnectPool) ReleaseAll(addr net.Addr) {
	pool, ok := func() (pool PoolInterface, ok bool) {
		cp.RLock()
		defer cp.RUnlock()
		pool, ok = cp.pools[addr.String()]
		return
	}()
	if !ok {
		return
	}
	pool.ReleaseAll()
}

func (cp *ConnectPool) PutConnect(c *net.TCPConn, forceClose bool) {
	cp.PutConnectV2(c, forceClose, "", 0)
}

func (cp *ConnectPool) PutConnectV2(c *net.TCPConn, forceClose bool, addr string, cost int64) {
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
	if addr == "" {
		addr = c.RemoteAddr().String()
	}
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		c.Close()
		return
	}
	object := &Object{conn: c, idle: time.Now().UnixNano(), cost: cost}
	pool.PutConnectObjectToPool(object)
}

func (cp *ConnectPool) PutConnectEx(c *net.TCPConn, err error) {
	if c == nil {
		return
	}
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			cp.ReleaseAll(c.RemoteAddr())
		}
	}
	cp.PutConnect(c, err != nil)
}

func (cp *ConnectPool) autoRelease() {
	timer := time.NewTimer(time.Second)
	for {
		select {
		case <-cp.closeCh:
			timer.Stop()
			return
		case <-timer.C:
		}
		pools := make([]PoolInterface, 0)
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
	pools := make([]PoolInterface, 0)
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

type PoolInterface interface {
	GetConnectFromPool() (c *net.TCPConn, err error)
	PutConnectObjectToPool(o *Object)
	autoRelease()
	ReleaseAll()
}

type Pool struct {
	objects        chan *Object
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
	useMilliSecond bool
}

func NewPool(min, max int, timeout, connectTimeout int64, target string, useMilliSecond bool) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *Object, max)
	p.timeout = timeout
	p.connectTimeout = connectTimeout
	go p.initAllConnect()
	return p
}

func (p *Pool) initAllConnect() {
	var (
		c   net.Conn
		err error
	)
	for i := 0; i < p.mincap; i++ {
		if p.connectTimeout != 0 {
			if p.useMilliSecond {
				c, err = net.DialTimeout("tcp", p.target, time.Duration(p.connectTimeout)*time.Millisecond)
			} else {
				c, err = net.DialTimeout("tcp", p.target, time.Duration(p.connectTimeout)*time.Second)
			}
		} else {
			c, err = net.Dial("tcp", p.target)
		}
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
	var o *Object
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

type PoolWithCost struct {
	*Pool
	conns *list.List
	sync.RWMutex
}

func NewPoolWithCost(min, max int, timeout, connectTimeout int64, target string, useMilliSecond bool) (p *PoolWithCost) {
	p = &PoolWithCost{
		Pool:  NewPool(min, max, timeout, connectTimeout, target, useMilliSecond),
		conns: list.New(),
	}

	if log.EnableDebug() {
		log.LogDebugf("PoolWithCost NewPoolWithCost start")
	}

	go p.autoRelease()
	return p
}

func (p *PoolWithCost) PutConnectObjectToPool(o *Object) {
	if log.EnableDebug() {
		log.LogDebugf("PoolWithCost PutConnectObjectToPool start")
	}

	if o == nil || o.conn == nil {
		return
	}

	if p.conns.Len() >= p.maxcap {
		o.conn.Close()
		return
	}

	p.Lock()
	defer p.Unlock()

	for e := p.conns.Front(); e != nil; e = e.Next() {
		o1 := e.Value.(*Object)
		if o.cost < o1.cost {
			p.conns.InsertBefore(o, e)
			return
		}
	}

	p.conns.PushBack(o)
}

func (p *PoolWithCost) autoRelease() {
	if log.EnableDebug() {
		log.LogDebugf("PoolWithCost autoRelease start")
	}

	closeList := make([]*Object, 0)

	len := p.conns.Len()

	p.Lock()

	for i := 0; i < len; i++ {
		e := p.conns.Front()
		if e == nil {
			// p.conns.Remove(e)
			continue
		}

		o := e.Value.(*Object)
		if time.Now().UnixNano()-int64(o.idle) > p.timeout {
			p.conns.Remove(e)
			closeList = append(closeList, o)
		}
	}
	p.Unlock()

	for _, o := range closeList {
		o.conn.Close()
	}
}

func (p *PoolWithCost) ReleaseAll() {
	p.Lock()
	defer p.Unlock()

	len := p.conns.Len()

	for i := 0; i < len; i++ {
		e := p.conns.Front()
		if e == nil {
			continue
		}

		o := e.Value.(*Object)
		o.conn.Close()
		p.conns.Remove(e)
	}
}

func (p *PoolWithCost) GetConnectFromPool() (c *net.TCPConn, err error) {
	if log.EnableDebug() {
		log.LogDebugf("PoolWithCost GetConnectFromPool start")
	}

	p.Lock()
	len := p.conns.Len()

	for i := 0; i < len; i++ {
		e := p.conns.Front()
		if e == nil {
			continue
		}

		o := e.Value.(*Object)
		p.conns.Remove(e)

		if time.Now().UnixNano()-int64(o.idle) > p.timeout {
			o.conn.Close()
			continue
		}

		p.Unlock()
		return o.conn, nil
	}
	p.Unlock()

	return p.NewConnect(p.target)
}
