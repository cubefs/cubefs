// Modified work copyright (C) 2018 The CFS Authors.
//
// The MIT License (MIT)
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package zkconn

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"sync"
	"time"
)

const (
	defaultDialTimeout = 1
	poolExhausted      = "zookeeper pool exhausted"
)

var ErrZkPoolExhausted = errors.New("zookeeper pool exhausted")

type Object struct {
	conn *zk.Conn
	idle int64
}

type Pool struct {
	pool    chan *Object
	mincap  int
	maxcap  int
	target  string
	zkAddrs []string
	timeout int64
	active  int
	sync.RWMutex
}

func NewPool(min, max int, timeout int64, target string, zkAddrs []string) (p *Pool) {
	p = new(Pool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.zkAddrs = zkAddrs
	p.pool = make(chan *Object, max)
	p.timeout = timeout
	p.initAllConnect()
	return p
}

func (p *Pool) initAllConnect() {
	for i := 0; i < p.mincap; i++ {
		c, err := p.createConn()
		if err == nil {
			obj := &Object{conn: c, idle: time.Now().UnixNano()}
			p.putconnect(obj)
		}
	}
}

func (p *Pool) createConn() (c *zk.Conn, err error) {
	p.Lock()
	defer p.Unlock()
	if p.active >= p.maxcap {
		return nil, ErrZkPoolExhausted
	}
	c, _, err = zk.Connect(p.zkAddrs, time.Second*10, zk.WithLogInfo(false))
	if err != nil {
		return
	}
	p.active++
	return
}

func (p *Pool) destroyConn(conn *zk.Conn) {
	if conn == nil {
		return
	}
	conn.Close()
	p.Lock()
	p.active--
	p.Unlock()
}

func (p *Pool) putconnect(o *Object) {
	select {
	case p.pool <- o:
		return
	default:
		p.destroyConn(o.conn)
		return
	}
}

func (p *Pool) Get() (c *zk.Conn, err error) {
	obj, err := p.getconnect()
	if obj != nil {
		return obj.conn, nil
	}
	return
}

func (p *Pool) getconnect() (o *Object, err error) {
	select {
	case o = <-p.pool:
		return
	default:
		var c *zk.Conn
		c, err = p.createConn()
		if err != nil && strings.Contains(err.Error(), poolExhausted) {
			o = <-p.pool
		} else {
			o = &Object{conn: c, idle: time.Now().UnixNano()}
		}
		return o, err
	}
}

func (p *Pool) AutoRelease() {
	connectLen := len(p.pool)
	for i := 0; i < connectLen; i++ {
		select {
		case o := <-p.pool:
			if time.Now().UnixNano()-o.idle > p.timeout {
				p.destroyConn(o.conn)
			} else {
				p.putconnect(o)
			}
		default:
			return
		}
	}
}

type ConnectPool struct {
	sync.RWMutex
	pools   map[string]*Pool
	mincap  int
	maxcap  int
	timeout int64
}

func NewConnectPool() (cp *ConnectPool) {
	cp = &ConnectPool{pools: make(map[string]*Pool), mincap: 5, maxcap: 50, timeout: int64(3 * time.Minute)}
	go cp.autoRelease()

	return cp
}

func (cp *ConnectPool) Get(targetAddr string, zkAddrs []string) (c *zk.Conn, err error) {
	cp.RLock()
	pool, ok := cp.pools[targetAddr]
	cp.RUnlock()
	if !ok {
		cp.Lock()
		pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, targetAddr, zkAddrs)
		cp.pools[targetAddr] = pool
		cp.Unlock()
	}

	return pool.Get()
}

func (cp *ConnectPool) Put(target string, c *zk.Conn, forceClose bool) {
	if c == nil {
		return
	}

	cp.RLock()
	pool, ok := cp.pools[target]
	cp.RUnlock()
	if !ok {
		c.Close()
		return
	}
	if forceClose {
		pool.destroyConn(c)
		return
	}
	object := &Object{conn: c, idle: time.Now().UnixNano()}
	pool.putconnect(object)

	return
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
