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
	"container/list"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/rdma"
)

type RdmaClientObject struct {
	client *rdma.Client // client conn
	conn   *rdma.Connection
	Addr   string
	idle   int64
}

const (
	RdmaConnectIdleTime       = 30
	defaultRdmaConnectTimeout = 30
)

var (
	RdmaEnvInit = false
	Config      = &rdma.RdmaPoolConfig{}
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
	connectTimeout int64
	closeCh        chan struct{}
	closeOnce      sync.Once
	NetLinks       list.List
}

func NewRdmaConnectPool() (rcp *RdmaConnectPool) {
	InitRdmaEnv()
	rcp = &RdmaConnectPool{
		connectTimeout: defaultRdmaConnectTimeout,
		closeCh:        make(chan struct{}),
		NetLinks:       *list.New(),
	}
	//go rcp.autoRelease()

	return rcp
}

func NewRdmaConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout int64) (rcp *RdmaConnectPool) {
	InitRdmaEnv()
	rcp = &RdmaConnectPool{
		connectTimeout: connectTimeout,
		closeCh:        make(chan struct{}),
		NetLinks:       *list.New(),
	}
	//go rcp.autoRelease()

	return rcp
}

func (rcp *RdmaConnectPool) GetRdmaConn(targetAddr string) (conn *rdma.Connection, err error) {
	rcp.Lock()
	defer rcp.Unlock()

	for item := rcp.NetLinks.Front(); item != nil; item = item.Next() {
		if item.Value == nil {
			continue
		}
		obj, ok := item.Value.(*RdmaClientObject)
		if !ok {
			continue
		}
		if obj.Addr == targetAddr {
			conn = obj.conn
			rcp.NetLinks.Remove(item)
			return conn, nil
		}
	}

	str := strings.Split(targetAddr, ":")
	targetIp := str[0]
	targetPort := str[1]
	client, err := rdma.NewRdmaClient(targetIp, targetPort) //TODO
	if err != nil {
		return nil, err
	}
	conn = client.Dial()

	return conn, nil
}

func (rcp *RdmaConnectPool) PutRdmaConn(conn *rdma.Connection, forceClose bool) {
	if conn == nil {
		return
	}

	client, _ := (conn.Ctx).(*rdma.Client)
	addr := client.RemoteIp + ":" + client.RemotePort

	if forceClose {
		conn.Close()
		client.Close()
		return
	}

	rcp.Lock()
	defer rcp.Unlock()

	obj := &RdmaClientObject{
		client: client,
		conn:   conn,
		Addr:   addr,
	}
	rcp.NetLinks.PushFront(obj)

	return
}
