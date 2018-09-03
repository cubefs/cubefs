// Copyright 2018 The ChuBao Authors.
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
	"net"
	"sync"
	"time"
)

type ConnTestFunc func(conn *net.TCPConn) bool

type ConnPool struct {
	sync.Mutex
	pools    map[string]Pool
	initCap  int
	maxCap   int
	idleTime time.Duration
	testFunc ConnTestFunc
}

func NewConnPool() (connP *ConnPool) {
	connP = &ConnPool{pools: make(map[string]Pool), initCap: 5, maxCap: 50, idleTime: time.Second * 20}
	go connP.autoRelease()

	return connP
}

func (connP *ConnPool) Get(targetAddr string) (c *net.TCPConn, err error) {
	var obj interface{}

	factoryFunc := func(addr interface{}) (interface{}, error) {
		var connect *net.TCPConn
		conn, err := net.DialTimeout("tcp", addr.(string), time.Second)
		if err == nil {
			connect, _ = conn.(*net.TCPConn)
			connect.SetKeepAlive(true)
			connect.SetNoDelay(true)
		}

		return connect, err
	}
	closeFunc := func(v interface{}) error { return v.(net.Conn).Close() }

	testFunc := func(item interface{}) bool {
		if connP != nil {
			return connP.testFunc(item.(*net.TCPConn))
		}
		return true
	}

	connP.Lock()
	pool, ok := connP.pools[targetAddr]
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  connP.initCap,
			MaxCap:      connP.maxCap,
			Factory:     factoryFunc,
			Close:       closeFunc,
			Test:        testFunc,
			IdleTimeout: connP.idleTime,
			Para:        targetAddr,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			connP.Unlock()
			conn, err := factoryFunc(targetAddr)
			return conn.(*net.TCPConn), err
		}
		connP.pools[targetAddr] = pool
	}
	connP.Unlock()

	if obj, err = pool.Get(); err != nil {
		conn, err := factoryFunc(targetAddr)
		return conn.(*net.TCPConn), err
	}
	c = obj.(*net.TCPConn)
	return
}

func (connP *ConnPool) Put(c *net.TCPConn, forceClose bool) {
	if c == nil {
		return
	}
	if forceClose {
		c.Close()
		return
	}
	addr := c.RemoteAddr().String()
	connP.Lock()
	pool, ok := connP.pools[addr]
	connP.Unlock()
	if !ok {
		c.Close()
		return
	}
	pool.Put(c)

	return
}

func (connP *ConnPool) autoRelease() {
	for {
		pools := make([]Pool, 0)
		connP.Lock()
		for _, pool := range connP.pools {
			pools = append(pools, pool)
		}
		connP.Unlock()
		for _, pool := range pools {
			pool.AutoRelease()
		}
		time.Sleep(time.Minute)
	}

}
