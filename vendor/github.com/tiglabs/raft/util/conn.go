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
	"net"
	"time"
)

type ConnTimeout struct {
	addr      string
	conn      net.Conn
	readTime  time.Duration
	writeTime time.Duration
}

func DialTimeout(addr string, connTime time.Duration) (*ConnTimeout, error) {
	conn, err := net.DialTimeout("tcp", addr, connTime)
	if err != nil {
		return nil, err
	}

	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetLinger(0)
	conn.(*net.TCPConn).SetKeepAlive(true)
	return &ConnTimeout{conn: conn, addr: addr}, nil
}

func NewConnTimeout(conn net.Conn) *ConnTimeout {
	if conn == nil {
		return nil
	}

	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetLinger(0)
	conn.(*net.TCPConn).SetKeepAlive(true)
	return &ConnTimeout{conn: conn, addr: conn.RemoteAddr().String()}
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

func (c *ConnTimeout) RemoteAddr() string {
	return c.addr
}

func (c *ConnTimeout) Close() error {
	return c.conn.Close()
}
