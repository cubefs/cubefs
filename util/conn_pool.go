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
	"container/list"
	"context"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/util/tracing"
)

type Object struct {
	conn *net.TCPConn
	idle int64
}

const (
	defaultConnectTimeoutMs = 1000
	ObjectPoolCnt = 64
)

var (
	ObjectPool [ObjectPoolCnt]*sync.Pool
	bytePool   = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 128)
		},
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index := 0; index < ObjectPoolCnt; index++ {
		ObjectPool[index] = &sync.Pool{
			New: func() interface{} {
				return new(Object)
			},
		}
	}
}

func GetObjectConnectFromPool() *Object {
	index := rand.Intn(ObjectPoolCnt)
	o := ObjectPool[index].Get().(*Object)
	o.conn = nil
	o.idle = time.Now().UnixNano()
	return o
}

func ReturnObjectConnectToPool(o *Object) {
	if o != nil {
		o.conn = nil
		ObjectPool[rand.Intn(ObjectPoolCnt)].Put(o)
	}
}

const (
	ConnectIdleTime       = 30
	defaultConnectTimeout = 1
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
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool, ok = cp.pools[targetAddr]
		if !ok {
			pool = NewPool(nil, cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
			cp.pools[targetAddr] = pool
		}
		cp.Unlock()
	}

	return pool.GetConnectFromPool(nil)
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
	o := GetObjectConnectFromPool()
	o.conn = c
	pool.PutConnectToPool(o)

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
		errStr := err.Error()
		if strings.Contains(errStr, syscall.ETIMEDOUT.Error()) || strings.Contains(errStr, syscall.ECONNREFUSED.Error()) {
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
	connectTimeoutNs int64
	objects        *list.List
	lock           sync.RWMutex
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
}

func NewPool(ctx context.Context, min, max int, timeout, connectTimeoutNs int64, target string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = list.New()
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
			o := GetObjectConnectFromPool()
			o.conn = conn
			p.PutConnectToPool(o)
		}
	}
}

func (p *Pool) PutConnectToPool(o *Object) {
	p.lock.Lock()
	p.objects.PushBack(o)
	p.lock.Unlock()
}

func (p *Pool) autoRelease() {
	needRemove := make([]*list.Element, 0)
	p.lock.RLock()
	connectLen := p.objects.Len()
	needRemoveCnt := connectLen - p.mincap
	for e := p.objects.Front(); e != nil; e = e.Next() {
		o := e.Value.(*Object)
		if time.Now().UnixNano()-int64(o.idle) > p.timeout {
			needRemove = append(needRemove, e)
		}
		if len(needRemove) >= needRemoveCnt {
			break
		}
	}
	p.lock.RUnlock()

	p.lock.Lock()
	for _, e := range needRemove {
		p.objects.Remove(e)
	}
	p.lock.Unlock()
	for _, e := range needRemove {
		o := e.Value.(*Object)
		o.conn.Close()
		ReturnObjectConnectToPool(o)
	}

}

func (p *Pool) ReleaseAll() {
	allE := make([]*list.Element, 0)
	p.lock.Lock()
	for e := p.objects.Front(); e != nil; e = e.Next() {
		allE = append(allE, e)
	}
	p.objects = list.New()
	p.lock.Unlock()
	for _, e := range allE {
		o := e.Value.(*Object)
		o.conn.Close()
		ReturnObjectConnectToPool(o)
	}
}

func (p *Pool) NewConnect(ctx context.Context, target string) (c *net.TCPConn, err error) {
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
	var (
		o *Object
	)

	p.lock.Lock()
	e := p.objects.Back()
	if e != nil {
		o = p.objects.Remove(e).(*Object)
	}
	p.lock.Unlock()
	if o != nil && time.Now().UnixNano()-o.idle < p.timeout {
		c = o.conn
		ReturnObjectConnectToPool(o)
		return
	} else {
		ReturnObjectConnectToPool(o)
		c, err = p.NewConnect(ctx, p.target)
	}
	return
}
