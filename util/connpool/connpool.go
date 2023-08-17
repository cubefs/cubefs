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

package connpool

import (
	"context"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type Object struct {
	conn *net.TCPConn
	idle int64
}

const (
	delfaultIdleConnTimeout = 30 * time.Second
	defaultConnectTimeout   = 1 * time.Second
	objectPoolCnt           = 64
)

var (
	ObjectPool [objectPoolCnt]*sync.Pool
	bytePool   = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 128)
		},
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index := 0; index < objectPoolCnt; index++ {
		ObjectPool[index] = &sync.Pool{
			New: func() interface{} {
				return new(Object)
			},
		}
	}
}

func GetObjectConnectFromPool() *Object {
	index := rand.Intn(objectPoolCnt)
	o := ObjectPool[index].Get().(*Object)
	o.conn = nil
	o.idle = time.Now().UnixNano()
	return o
}

func ReturnObjectConnectToPool(o *Object) {
	if o != nil {
		o.conn = nil
		ObjectPool[rand.Intn(objectPoolCnt)].Put(o)
	}
}

type ConnectPool struct {
	sync.RWMutex
	pools           map[string]*Pool
	mincap          int
	maxcap          int
	idleConnTimeout time.Duration
	connectTimeout  time.Duration
	closeCh         chan struct{}
	closeOnce       sync.Once
	wg              sync.WaitGroup
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:           make(map[string]*Pool),
		mincap:          5,
		maxcap:          80,
		idleConnTimeout: delfaultIdleConnTimeout,
		connectTimeout:  defaultConnectTimeout,
		closeCh:         make(chan struct{}),
	}
	cp.wg.Add(1)
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout time.Duration) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:           make(map[string]*Pool),
		mincap:          5,
		maxcap:          80,
		idleConnTimeout: idleConnTimeout,
		connectTimeout:  connectTimeout,
		closeCh:         make(chan struct{}),
	}
	cp.wg.Add(1)
	go cp.autoRelease()

	return cp
}

func NewConnectPoolWithTimeoutAndCap(min, max int, idleConnTimeout, connectTimeout time.Duration) (cp *ConnectPool) {
	cp = &ConnectPool{
		pools:           make(map[string]*Pool),
		mincap:          min,
		maxcap:          max,
		idleConnTimeout: idleConnTimeout,
		connectTimeout:  connectTimeout,
		closeCh:         make(chan struct{}),
	}
	cp.wg.Add(1)
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
			pool = NewPool(nil, cp.mincap, cp.maxcap, cp.idleConnTimeout, cp.connectTimeout, targetAddr)
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

func (cp *ConnectPool) UpdateTimeout(idleConnTimeout, connectTimeout time.Duration) {
	cp.Lock()
	cp.idleConnTimeout = idleConnTimeout
	cp.connectTimeout = connectTimeout
	for _, pool := range cp.pools {
		pool.idleConnTimeout = idleConnTimeout
		pool.connectTimeout = connectTimeout
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
	defer cp.wg.Done()
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
	cp.pools = make(map[string]*Pool)
	cp.RUnlock()
	for _, pool := range pools {
		pool.ReleaseAll()
	}
}

func (cp *ConnectPool) Close() {
	cp.closeOnce.Do(func() {
		close(cp.closeCh)
		cp.wg.Wait()
		cp.releaseAll()
	})
}

type Pool struct {
	lock            sync.RWMutex
	objects         chan *Object
	mincap          int
	maxcap          int
	target          string
	idleConnTimeout time.Duration
	connectTimeout  time.Duration
}

func NewPool(ctx context.Context, min, max int, idleConnTimeout, connectTimeout time.Duration, target string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *Object, max)
	p.idleConnTimeout = idleConnTimeout
	p.connectTimeout = connectTimeout
	p.initAllConnect(ctx)
	return p
}

func (p *Pool) initAllConnect(ctx context.Context) {

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
			if time.Now().UnixNano()-int64(o.idle) > int64(p.idleConnTimeout) {
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
	var connect net.Conn
	connect, err = net.DialTimeout("tcp", p.target, p.connectTimeout)
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
	for {
		select {
		case o = <-p.objects:
		default:
			return p.NewConnect(ctx, p.target)
		}
		if time.Now().UnixNano()-int64(o.idle) > int64(p.idleConnTimeout) {
			_ = o.conn.Close()
			o = nil
			continue
		}
		return o.conn, nil
	}
}
