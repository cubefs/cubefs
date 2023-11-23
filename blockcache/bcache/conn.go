// Copyright 2022 The CubeFS Authors.
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

package bcache

import (
	"net"
	"time"
)

const (
	DefaultTimeOut    = 1 * time.Second
	ConnectExpireTime = 20
)

type ConnObject struct {
	c          *net.Conn
	lastActive int64
}

type ConnPool struct {
	conns  chan *ConnObject
	mincap int
	maxcap int
	expire int64
	target string
}

func NewConnPool(target string, mincap, maxcap int, expire int64) *ConnPool {
	p := &ConnPool{
		conns:  make(chan *ConnObject, maxcap),
		mincap: mincap,
		maxcap: maxcap,
		expire: expire,
		target: target,
	}
	return p
}

func (connPool *ConnPool) Get() (c *net.Conn, err error) {
	var o *ConnObject
	for {
		select {
		case o = <-connPool.conns:
		default:
			return connPool.NewConnect(connPool.target)
		}
		if time.Now().UnixNano()-o.lastActive > connPool.expire {
			_ = (*o.c).Close()
			o = nil
			continue
		}
		return o.c, nil
	}
}

func (connPool *ConnPool) NewConnect(target string) (*net.Conn, error) {
	conn, err := net.DialTimeout("unix", target, DefaultTimeOut)

	return &conn, err
}

func (connPool *ConnPool) Put(c *net.Conn) {
	o := &ConnObject{
		c:          c,
		lastActive: time.Now().UnixNano(),
	}
	select {
	case connPool.conns <- o:
		return
	default:
		if o.c != nil {
			(*o.c).Close()
		}
		return
	}
}
