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
	"net"
	"strings"
	"sync"
	"time"
)

type ConnectObject struct {
	conn *net.TCPConn
}

type Pool struct {
	pool   chan *ConnectObject
	mincap int
	maxcap int
	target string
}

func NewPool(min,max int,target string)(p *Pool){
	p=new(Pool)
	p.mincap =min
	p.maxcap =max
	p.target=target
	p.pool=make(chan *ConnectObject,min)
	return p
}

func (p *Pool)initAllConnect(){
	for i:=0;i<p.mincap;i++{
		c,err:=net.Dial("tcp",p.target)
		if err==nil {
			conn:=c.(*net.TCPConn)
			conn.SetKeepAlive(true)
			conn.SetNoDelay(true)
			obj:=&ConnectObject{conn:conn}
			p.putconnect(obj)
		}
	}
}

func (p *Pool)putconnect(c *ConnectObject){
	select {
		case p.pool<-c:
			return
		default:
			return
	}
}

func (p *Pool)getconnect()(c *ConnectObject){
	select {
	case c=<-p.pool:
		return
	default:
		return
	}
}

func (p *Pool)AutoRelease(){
	for{
		select {
		case c:=<-p.pool:
			c.conn.Close()
			return
		default:
			return
		}
	}
}

func (p *Pool)Get()(c *net.TCPConn,err error){
	obj:=p.getconnect()
	if obj!=nil {
		return obj.conn,nil
	}
	var connect net.Conn
	connect,err=net.Dial("tcp",p.target)
	if err==nil {
		conn:=connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		c=conn
	}
	return
}

type ConnPool struct {
	sync.Mutex
	pools    map[string]*Pool
	mincap   int
	maxcap   int
	timeout int64
}

func NewConnPool() (connectPool *ConnPool) {
	connectPool = &ConnPool{pools: make(map[string]*Pool), mincap: 5, maxcap: 50, timeout: int64(time.Second * 20)}
	go connectPool.autoRelease()

	return connectPool
}

func (connectPool *ConnPool) Get(targetAddr string) (c *net.TCPConn, err error) {
	connectPool.Lock()
	pool, ok := connectPool.pools[targetAddr]
	if !ok {
		pool=NewPool(connectPool.mincap,connectPool.maxcap,targetAddr)
		connectPool.pools[targetAddr]=pool
	}
	connectPool.Unlock()

	return pool.Get()
}

func (connectPool *ConnPool) Put(c *net.TCPConn, forceClose bool) {
	if c == nil {
		return
	}
	if forceClose {
		c.Close()
		return
	}
	addr := c.RemoteAddr().String()
	connectPool.Lock()
	pool, ok := connectPool.pools[addr]
	connectPool.Unlock()
	if !ok {
		c.Close()
		return
	}
	object:=&ConnectObject{conn:c}
	pool.putconnect(object)

	return
}

func (connectPool *ConnPool) CheckErrorForPutConnect(c *net.TCPConn, target string, err error) {
	if c == nil {
		return
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		c.Close()
		connectPool.ReleaseAllConnect(target)
		return
	}
	if err != nil {
		c.Close()
		return
	}
	addr := c.RemoteAddr().String()
	connectPool.Lock()
	pool, ok := connectPool.pools[addr]
	connectPool.Unlock()
	if !ok {
		c.Close()
		return
	}
	object:=&ConnectObject{conn:c}
	pool.putconnect(object)
}

func (connectPool *ConnPool) ReleaseAllConnect(target string) {
	connectPool.Lock()
	pool := connectPool.pools[target]
	connectPool.Unlock()
	pool.AutoRelease()
}

func (connectPool *ConnPool) autoRelease() {
	for {
		pools := make([]*Pool, 0)
		connectPool.Lock()
		for _, pool := range connectPool.pools {
			pools = append(pools, pool)
		}
		connectPool.Unlock()
		for _, pool := range pools {
			pool.AutoRelease()
		}
		time.Sleep(time.Minute)
	}

}
