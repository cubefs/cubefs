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

import "C"
import (
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/rdma"
	"strings"
	"sync"
	"time"
)

type RdmaClientObject struct {
	client *rdma.Client // client conn
	conn   *rdma.Connection
	idle   int64
}

const (
	RdmaConnectIdleTime       = 30
	defaultRdmaConnectTimeout = 1
)

var (
	RdmaEnvInit = false
	Config      = &rdma.RdmaPoolConfig{}
	//RdmaConnPool = NewRdmaConnectPool()
	//StreamRdmaConnPool = NewRdmaConnectPool()
)

func InitRdmaEnv() {
	if !RdmaEnvInit {
		//Config = &rdma.RdmaPoolConfig{}
		if err := rdma.InitPool(Config); err != nil {
			println("init rdma pool failed")
			return
		}
		RdmaEnvInit = true
	}
}

type RdmaConnectPool struct {
	sync.RWMutex
	clientPools    map[string]*ClientPool
	mincap         int
	maxcap         int
	timeout        int64
	connectTimeout int64
	closeCh        chan struct{}
	closeOnce      sync.Once
}

func NewRdmaConnectPool() (rcp *RdmaConnectPool) {
	InitRdmaEnv()
	rcp = &RdmaConnectPool{
		clientPools:    make(map[string]*ClientPool),
		mincap:         5,
		maxcap:         500,
		timeout:        int64(time.Second * RdmaConnectIdleTime),
		connectTimeout: defaultRdmaConnectTimeout,
		closeCh:        make(chan struct{}),
	}
	//go rcp.autoRelease()

	return rcp
}

func NewRdmaConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout int64) (rcp *RdmaConnectPool) {
	InitRdmaEnv()
	rcp = &RdmaConnectPool{
		clientPools:    make(map[string]*ClientPool),
		mincap:         5,
		maxcap:         80,
		timeout:        int64(idleConnTimeout * time.Second),
		connectTimeout: connectTimeout,
		closeCh:        make(chan struct{}),
	}
	//go rcp.autoRelease()

	return rcp
}

func (rcp *RdmaConnectPool) GetRdmaConn(targetAddr string) (conn *rdma.Connection, err error) {
	rcp.RLock()
	pool, ok := rcp.clientPools[targetAddr]
	rcp.RUnlock()
	if !ok {
		newPool := NewClientPool(rcp.mincap, rcp.maxcap, rcp.timeout, rcp.connectTimeout, targetAddr)
		rcp.Lock()
		pool, ok = rcp.clientPools[targetAddr]
		if !ok {
			//pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
			pool = newPool
			rcp.clientPools[targetAddr] = pool
		}
		rcp.Unlock()
	}

	return pool.GetRdmaConnFromPool()
}

func (rcp *RdmaConnectPool) PutRdmaConn(conn *rdma.Connection, forceClose bool) {
	if conn == nil {
		return
	}
	/*	if forceClose {
			_ = c.Close()
			return
		}
		select {
		case <-cp.closeCh:
			_ = c.Close()
			return
		default:
		}*/
	//addr := client.remoteIp + ":" + client.remotePort
	client, _ := (conn.Ctx).(*rdma.Client)
	if forceClose {
		conn.Close()
		client.Close()
		return
	}
	addr := client.RemoteIp + ":" + client.RemotePort
	rcp.RLock()
	pool, ok := rcp.clientPools[addr]
	rcp.RUnlock()
	if !ok {
		//c.Close()
		//*Client(conn.ctx).Close()
		conn.Close()
		client.Close()
		return
	}

	//object := &RdmaClientObject{client: client, idle: time.Now().UnixNano()}
	//pool.PutRdmaClientObjectToPool(object)
	pool.PutRdmaConnToPool(conn)
}

type ClientPool struct {
	sync.RWMutex
	objects        chan *RdmaClientObject
	inUse          map[*rdma.Connection]*RdmaClientObject
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
}

func NewClientPool(min, max int, timeout, connectTimeout int64, target string) (p *ClientPool) {
	p = new(ClientPool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *RdmaClientObject, max)
	p.inUse = make(map[*rdma.Connection]*RdmaClientObject)
	p.timeout = timeout
	p.connectTimeout = connectTimeout
	p.initAllClient()
	return p
}

func (clientPool *ClientPool) initAllClient() {
	str := strings.Split(clientPool.target, ":")
	targetIp := str[0]
	targetPort := str[1]
	for i := 0; i < clientPool.mincap; i++ {
		client, err := rdma.NewRdmaClient(targetIp, targetPort) //TODO
		if err == nil {
			o := &RdmaClientObject{client: client, conn: nil, idle: time.Now().UnixNano()}
			//clientPool.PutRdmaClientObjectToPool(o)
			clientPool.objects <- o
		}
	}
}

/*
func (clientPool *ClientPool) PutRdmaClientObjectToPool(o *RdmaClientObject) {
	select {
	case clientPool.objects <- o:
		return
	default:
		if o.client != nil {
			//o.conn.Close()
			o.client.Close()
		}
		return
	}
}
*/

func (clientPool *ClientPool) GetRdmaConnFromPool() (conn *rdma.Connection, err error) {
	var (
		o *RdmaClientObject = nil
	)
	for {
		select {
		case o = <-clientPool.objects:
		default:
			str := strings.Split(clientPool.target, ":")
			targetIp := str[0]
			targetPort := str[1]
			client, err := rdma.NewRdmaClient(targetIp, targetPort) //TODO
			if err == nil {
				o = &RdmaClientObject{client: client, conn: nil, idle: time.Now().UnixNano()}
			}
			log.LogDebugf("new rdma client: err(%v)", err)
		}
		if o != nil {
			if time.Now().UnixNano()-int64(o.idle) > clientPool.timeout {
				//_ = o.conn.Close()
				o.client.Close()
				o = nil
				continue
			}
			if o.conn == nil {
				conn = o.client.Dial()
				clientPool.Lock()
				clientPool.inUse[conn] = o
				clientPool.Unlock()
			} else {
				conn = o.conn
				conn.ReConnect()
				clientPool.Lock()
				clientPool.inUse[conn] = o
				clientPool.Unlock()
			}
			return conn, nil
		}
	}
}

func (clientPool *ClientPool) PutRdmaConnToPool(conn *rdma.Connection) {
	conn.Close()
	clientPool.Lock()
	o := clientPool.inUse[conn]
	delete(clientPool.inUse, conn)
	clientPool.Unlock()
	select {
	case clientPool.objects <- o:
		return
	default:
		if o.client != nil {
			//o.conn.Close()
			o.client.Close()
		}
		return
	}
}

/*
func (clientPool *ClientPool) GetClientFromPool() (client *Client, err error) {
	var (
		o *RdmaClientObject
	)
	for {
		select {
		case o = <-clientPool.objects:
		default:
			str := strings.Split(clientPool.target, ":")
			targetIp := str[0]
			targetPort := str[1]
			return NewRdmaClient(targetIp, targetPort,nil) //TODO
		}
		if time.Now().UnixNano()-int64(o.idle) > clientPool.timeout {
			//_ = o.conn.Close()
			o = nil
			continue
		}
		return o.client, nil
	}
}
*/

/*func (rcp *RdmaConnectPool) GetRdmaConn(targetAddr string) (c *Connection) {
	rcp.RLock()
	c, _ = rcp.connPools[targetAddr]
	rcp.RUnlock()

	return
}*/

/*func (rcp *RdmaConnectPool) PutRdmaConn(targetAddr string, c *Connection) {
	rcp.Lock()
	rcp.connPools[targetAddr] = c
	rcp.Unlock()
}*/

/*func (rcp *RdmaConnectPool) GetRdmaClientConn(targetAddr string) (c *ClientConnection) {
	rcp.RLock()
	c, _ = rcp.clientConnPools[targetAddr]
	rcp.RUnlock()

	return
}*/

/*func (rcp *RdmaConnectPool) PutRdmaClientConn(targetAddr string, c *ClientConnection) {
	rcp.Lock()
	rcp.clientConnPools[targetAddr] = c
	rcp.Unlock()
}*/
