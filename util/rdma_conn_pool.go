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
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/rdma"
)

type RdmaConnObject struct {
	conn *rdma.Connection
	Addr string
	idle int64
}

const (
	RdmaConnectIdleTime       = 30
	defaultRdmaConnectTimeout = 5
)

var (
	RdmaEnvInit = false
	Config      = &rdma.RdmaEnvConfig{}
)

func InitRdmaEnv() error {
	if !RdmaEnvInit {
		if err := rdma.InitEnv(Config); err != nil {
			return err
		}
		RdmaEnvInit = true
	}
	return nil
}

type RdmaConnectPool struct {
	sync.RWMutex
	timeout           int64
	connectTimeout    int64
	useExternalTxFlag bool
	closeCh           chan struct{}
	closeOnce         sync.Once
	NetLinks          list.List
}

func NewRdmaConnectPool(useExternalTxFlag bool) (rcp *RdmaConnectPool, err error) {
	err = InitRdmaEnv()
	if err != nil {
		return nil, err
	}
	rcp = &RdmaConnectPool{
		useExternalTxFlag: useExternalTxFlag,
		timeout:           int64(time.Second * RdmaConnectIdleTime),
		connectTimeout:    defaultRdmaConnectTimeout,
		closeCh:           make(chan struct{}),
		NetLinks:          *list.New(),
	}
	go rcp.autoRelease()

	return rcp, nil
}

func NewRdmaConnectPoolWithTimeout(useExternalTxFlag bool, idleConnTimeout time.Duration, connectTimeout int64) (rcp *RdmaConnectPool, err error) {
	err = InitRdmaEnv()
	if err != nil {
		return nil, err
	}
	rcp = &RdmaConnectPool{
		useExternalTxFlag: useExternalTxFlag,
		timeout:           int64(idleConnTimeout * time.Second),
		connectTimeout:    connectTimeout,
		closeCh:           make(chan struct{}),
		NetLinks:          *list.New(),
	}
	go rcp.autoRelease()

	return rcp, nil
}

func (rcp *RdmaConnectPool) Close() {
	rcp.closeCh <- struct{}{}

	rcp.Lock()
	defer rcp.Unlock()

	for item := rcp.NetLinks.Front(); item != nil; item = item.Next() {
		if item.Value == nil {
			continue
		}
		obj, ok := item.Value.(*RdmaConnObject)
		if !ok {
			continue
		}
		obj.conn.Close()
	}
}

func (rcp *RdmaConnectPool) CleanTimeOutLink() {
	rcp.Lock()
	defer rcp.Unlock()

	now := time.Now().UnixNano()

	for item := rcp.NetLinks.Front(); item != nil; item = item.Next() {
		if item.Value == nil {
			continue
		}
		obj, ok := item.Value.(*RdmaConnObject)
		if !ok {
			continue
		}
		if (now - obj.idle) > rcp.timeout {
			rcp.NetLinks.Remove(item)
			obj.conn.Close()
		}
	}
}

func (rcp *RdmaConnectPool) autoRelease() {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
			rcp.CleanTimeOutLink()
		case <-rcp.closeCh:
			return
		}
	}
}

func (rcp *RdmaConnectPool) GetRdmaConn(targetAddr string) (conn *rdma.Connection, err error) {
	rcp.Lock()
	defer rcp.Unlock()

	for item := rcp.NetLinks.Front(); item != nil; item = item.Next() {
		if item.Value == nil {
			continue
		}
		obj, ok := item.Value.(*RdmaConnObject)
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
	if len(str) != 2 {
		err = errors.New("rdma address error")
		return nil, err
	}
	targetIp := str[0]
	targetPort := str[1]
	conn = &rdma.Connection{}
	conn.TargetIp = targetIp
	conn.TargetPort = targetPort
	if err = conn.DialTimeout(targetIp, targetPort, rcp.useExternalTxFlag, time.Duration(rcp.connectTimeout)*time.Second); err != nil {
		return nil, err
	}

	return conn, nil
}

func (rcp *RdmaConnectPool) PutRdmaConn(conn *rdma.Connection, forceClose bool) {
	if conn == nil {
		return
	}

	addr := conn.TargetIp + ":" + conn.TargetPort

	if forceClose {
		conn.Close()
		return
	}

	rcp.Lock()
	defer rcp.Unlock()

	obj := &RdmaConnObject{
		conn: conn,
		Addr: addr,
		idle: time.Now().UnixNano(),
	}
	rcp.NetLinks.PushFront(obj)

	return
}
