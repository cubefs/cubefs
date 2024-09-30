// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"errors"
	"github.com/cubefs/cubefs/util/rdma"
	"net"
	"strings"
	"time"
)

type ConnTimeout struct {
	addr      string
	conn      net.Conn
	readTime  time.Duration
	writeTime time.Duration
	isRdma    bool
}

func DialTimeout(addr string, connTime time.Duration) (*ConnTimeout, error) {
	conn, err := net.DialTimeout("tcp", addr, connTime)
	if err != nil {
		return nil, err
	}

	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetLinger(0)
	conn.(*net.TCPConn).SetKeepAlive(true)
	return &ConnTimeout{conn: conn, addr: addr, isRdma: false}, nil
}

func DialRdmaTimeout(addr string, connTime time.Duration) (*ConnTimeout, error) {
	str := strings.Split(addr, ":")
	targetIp := str[0]
	targetPort := str[1]
	conn := &rdma.Connection{}
	conn.TargetIp = targetIp
	conn.TargetPort = targetPort
	err := conn.DialTimeout(targetIp, targetPort, connTime)
	if err != nil {
		return nil, err
	}
	conn.SetLoopExchange()
	return &ConnTimeout{conn: conn, addr: addr, isRdma: true}, nil
}

func NewConnTimeout(conn net.Conn) *ConnTimeout {
	if conn == nil {
		return nil
	}

	if c, ok := conn.(*rdma.Connection); ok {
		c.SetLoopExchange()
		return &ConnTimeout{conn: c, addr: c.RemoteAddr().String(), isRdma: true}
	} else {
		conn.(*net.TCPConn).SetNoDelay(true)
		conn.(*net.TCPConn).SetLinger(0)
		conn.(*net.TCPConn).SetKeepAlive(true)
		return &ConnTimeout{conn: conn, addr: conn.RemoteAddr().String(), isRdma: false}
	}

}

func (c *ConnTimeout) SetReadTimeout(timeout time.Duration) {
	c.readTime = timeout
}

func (c *ConnTimeout) SetWriteTimeout(timeout time.Duration) {
	c.writeTime = timeout
}

func (c *ConnTimeout) Read(p []byte) (n int, err error) {
	if c.readTime.Nanoseconds() > 0 {
		err = c.conn.SetReadDeadline(time.Now().Add(c.readTime))
		if err != nil {
			return
		}
	}

	n, err = c.conn.Read(p)
	return
}

func (c *ConnTimeout) ReadByRdma() (rdmaBuffer *rdma.RdmaBuffer, err error) {
	conn, _ := c.conn.(*rdma.Connection)

	return conn.GetRecvMsgBuffer()
}

func (c *ConnTimeout) ReleaseRxByRdma(rdmaBuffer *rdma.RdmaBuffer) (err error) {
	conn, ok := c.conn.(*rdma.Connection)
	if !ok {
		return errors.New("release rx data buffer failed: rdma conn type conversion error")
	}
	conn.ReleaseConnRxDataBuffer(rdmaBuffer)
	return
}

func (c *ConnTimeout) Write(p []byte) (n int, err error) {
	if c.writeTime.Nanoseconds() > 0 {
		err = c.conn.SetWriteDeadline(time.Now().Add(c.writeTime))
		if err != nil {
			return
		}
	}

	n, err = c.conn.Write(p)
	return
}

func (c *ConnTimeout) GetDataBuffer(len uint32) (*rdma.RdmaBuffer, error) {
	conn, ok := c.conn.(*rdma.Connection)
	if !ok {
		return nil, errors.New("get data buffer failed: rdma conn type conversion error")
	}
	return conn.GetConnTxDataBuffer(len)
}
func (c *ConnTimeout) AddWriteRequest(rdmaBuffer *rdma.RdmaBuffer) error {
	conn, ok := c.conn.(*rdma.Connection)
	if !ok {
		return errors.New("add write sge failed: rdma conn type conversion error")
	}
	return conn.AddWriteRequest(rdmaBuffer)
}

func (c *ConnTimeout) Flush() error {
	conn, ok := c.conn.(*rdma.Connection)
	if !ok {
		return errors.New("flush age failed: rdma conn type conversion error")
	}
	return conn.FlushWriteRequest()
}

func (c *ConnTimeout) RemoteAddr() string {
	return c.addr
}

func (c *ConnTimeout) GetRdmaConn() *rdma.Connection {
	if c.isRdma {
		return c.conn.(*rdma.Connection)
	}
	return nil
}

func (c *ConnTimeout) IsRdma() bool {
	return c.isRdma
}

func (c *ConnTimeout) Close() error {
	return c.conn.Close()
}
