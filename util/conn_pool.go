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
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/tracing"
)

type Object struct {
	conn *net.TCPConn
	idle int64
}

const (
	ConnectIdleTime         = 30
	defaultConnectTimeoutMs = 1000
)

type ConnectPool struct {
	sync.RWMutex
	pools            map[string]*Pool
	mincap           int
	maxcap           int
	timeout          int64
	connectTimeoutNs int64
	closeCh          chan struct{}
	closeOnce        sync.Once
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:            make(map[string]*Pool),
		mincap:           5,
		maxcap:           80,
		timeout:          int64(time.Second * ConnectIdleTime),
		connectTimeoutNs: int64(defaultConnectTimeoutMs*time.Millisecond),
		closeCh:          make(chan struct{}),
	}
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeoutMs int64) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:            make(map[string]*Pool),
		mincap:           5,
		maxcap:           80,
		timeout:          int64(idleConnTimeout * time.Second),
		connectTimeoutNs: connectTimeoutMs * int64(time.Millisecond),
		closeCh:          make(chan struct{}),
	}
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeoutAndCap(min, max int, idleConnTimeout, connectTimeoutNs int64) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:            make(map[string]*Pool),
		mincap:           min,
		maxcap:           max,
		timeout:          idleConnTimeout * int64(time.Second),
		connectTimeoutNs: connectTimeoutNs,
		closeCh:          make(chan struct{}),
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
	var tracer = tracing.NewTracer("ConnectPool.GetConnect").SetTag("target", targetAddr)
	defer tracer.Finish()
	ctx := tracer.Context()

	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool, ok = cp.pools[targetAddr]
		if !ok {
			pool = NewPool(ctx, cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeoutNs, targetAddr)
			cp.pools[targetAddr] = pool
		}
		cp.Unlock()
	}

	return pool.GetConnectFromPool(ctx)
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

func (cp *ConnectPool) PutConnectWithErr(c *net.TCPConn, err error) {
	cp.PutConnect(c, err != nil)
	remoteAddr := "connect is nil"
	if c != nil {
		remoteAddr = c.RemoteAddr().String()
	}
	// If connect failed because of server restart, release all connection
	if err != nil {
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "broken pipe") || strings.Contains(errStr, "connection reset by peer") {
			cp.ClearConnectPool(remoteAddr)
		}
	}
}

func (cp *ConnectPool) UpdateTimeout(idleConnTimeout, connectTimeoutNs int64) {
	cp.Lock()
	cp.timeout = idleConnTimeout * int64(time.Second)
	cp.connectTimeoutNs = connectTimeoutNs
	for _, pool := range cp.pools {
		atomic.StoreInt64(&pool.timeout, idleConnTimeout * int64(time.Second))
		atomic.StoreInt64(&pool.connectTimeoutNs, connectTimeoutNs)
	}
	cp.Unlock()
}

func (cp *ConnectPool) ClearConnectPool(addr string) {
	cp.RLock()
	pool, ok := cp.pools[addr]
	cp.RUnlock()
	if !ok {
		return
	}
	pool.ReleaseAll()
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
	objects          chan *Object
	mincap           int
	maxcap           int
	target           string
	timeout          int64
	connectTimeoutNs int64
}

func NewPool(ctx context.Context, min, max int, timeout, connectTimeoutNs int64, target string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *Object, max)
	p.timeout = timeout
	p.connectTimeoutNs = connectTimeoutNs
	p.initAllConnect(ctx)
	return p
}

func (p *Pool) initAllConnect(ctx context.Context) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Pool.initAllConnect").SetTag("target", p.target)
	defer tracer.Finish()

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

func (p *Pool) NewConnect(ctx context.Context, target string) (c *net.TCPConn, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Pool.NewConnect").SetTag("target", target)
	defer tracer.Finish()

	var connect net.Conn
	connect, err = net.DialTimeout("tcp", p.target, time.Duration(p.connectTimeoutNs)*time.Nanosecond)
	if err == nil {
		conn := connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		c = conn
	}
	return
}

func (p *Pool) GetConnectFromPool(ctx context.Context) (c *net.TCPConn, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Pool.GetConnectFromPool").SetTag("target", p.target)
	defer tracer.Finish()
	ctx = tracer.Context()

	var (
		o *Object
	)
	for {
		select {
		case o = <-p.objects:
		default:
			return p.NewConnect(ctx, p.target)
		}
		if time.Now().UnixNano()-int64(o.idle) > p.timeout {
			_ = o.conn.Close()
			o = nil
			continue
		}
		return o.conn, nil
	}
}
