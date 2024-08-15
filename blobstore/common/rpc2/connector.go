// Copyright 2024 The CubeFS Authors.
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

package rpc2

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

type Conn interface {
	net.Conn
	Allocator() transport.Allocator

	ReadOnce() (p []byte, err error) // rmda connection
}

type Dialer interface {
	Dial(ctx context.Context, addr string) (Conn, error)
}

type tcpConn struct{ net.Conn }

func (tcpConn) Allocator() transport.Allocator  { return nil }
func (tcpConn) ReadOnce() (p []byte, err error) { panic("rpc2: tcp connection cant read once") }

type tcpDialer struct{ timeout time.Duration }

func (t tcpDialer) Dial(ctx context.Context, addr string) (Conn, error) {
	var d net.Dialer
	d.Timeout = t.timeout
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return tcpConn{Conn: conn}, nil
}

type rdmaDialer struct{}

func (rdmaDialer) Dial(ctx context.Context, addr string) (Conn, error) {
	return nil, errors.New("rpc2: rdma not implements")
}

type Connector interface {
	Get(ctx context.Context, addr string) (*transport.Stream, error)
	Put(ctx context.Context, stream *transport.Stream) error
	Close() error
}

type connector struct {
	dialer Dialer
	config ConnectorConfig

	mu       sync.RWMutex
	sessions map[string]map[*transport.Session]struct{} // remote address
	streams  map[net.Addr]chan *transport.Stream        // local address
}

type ConnectorConfig struct {
	Transport *transport.Config

	// tcp or rdma
	Network     string
	Dialer      Dialer
	DialTimeout time.Duration

	MaxStreamPerSession int
}

func defaultConnector(config ConnectorConfig) Connector {
	defaulter.LessOrEqual(&config.MaxStreamPerSession, int(1024))
	dialer := config.Dialer
	if dialer == nil {
		switch config.Network {
		case "tcp":
			dialer = tcpDialer{}
		case "rdma":
			dialer = rdmaDialer{}
		default:
			panic("rpc2: connector network " + config.Network)
		}
	}
	if config.Transport == nil {
		config.Transport = transport.DefaultConfig()
	}
	return &connector{
		dialer:   dialer,
		config:   config,
		sessions: make(map[string]map[*transport.Session]struct{}),
		streams:  make(map[net.Addr]chan *transport.Stream),
	}
}

func (c *connector) Get(ctx context.Context, addr string) (*transport.Stream, error) {
	c.mu.RLock()
	ses, ok := c.sessions[addr]
	c.mu.RUnlock()
	if !ok || len(ses) == 0 {
		conn, err := c.dialer.Dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		conf := *c.config.Transport
		conf.Allocator = conn.Allocator()
		sess, err := transport.Client(conn, &conf)
		if err != nil {
			conn.Close()
			return nil, err
		}
		stream, err := sess.OpenStream()
		if err != nil {
			sess.Close()
			return nil, err
		}

		c.mu.Lock()
		if ses, ok = c.sessions[addr]; !ok {
			c.sessions[addr] = map[*transport.Session]struct{}{sess: {}}
		} else {
			ses[sess] = struct{}{}
		}
		c.streams[sess.LocalAddr()] = make(chan *transport.Stream, c.config.MaxStreamPerSession)
		c.mu.Unlock()
		return stream, nil
	}

	// try to get opened stream
	var stream *transport.Stream
	c.mu.RLock()
	sesCopy := make(map[*transport.Session]struct{}, len(ses))
	for sess := range ses {
		select {
		case stream = <-c.streams[sess.LocalAddr()]:
		default:
		}
		if stream != nil {
			break
		}
		sesCopy[sess] = struct{}{}
	}
	c.mu.RUnlock()
	if stream != nil {
		return stream, nil
	}

	// try to open new stream
	for sess := range sesCopy {
		newStream, err := sess.OpenStream()
		if err != nil {
			c.mu.Lock()
			delete(c.sessions[addr], sess)
			delete(c.streams, sess.LocalAddr())
			c.mu.Unlock()
			sess.Close()
			continue
		}
		return newStream, nil
	}

	return c.Get(ctx, addr)
}

func (c *connector) Put(ctx context.Context, stream *transport.Stream) error {
	c.mu.RLock()
	ch, ok := c.streams[stream.LocalAddr()]
	c.mu.RUnlock()
	if ok {
		select {
		case ch <- stream:
		default:
			stream.Close()
		}
	}
	return nil
}

func (c *connector) Close() (err error) {
	c.mu.Lock()
	for _, sesss := range c.sessions {
		for sess := range sesss {
			errx := sess.Close()
			if err == nil || err == io.ErrClosedPipe {
				err = errx
			}
		}
	}
	c.sessions = make(map[string]map[*transport.Session]struct{})
	c.streams = make(map[net.Addr]chan *transport.Stream)
	c.mu.Unlock()
	if err == io.ErrClosedPipe {
		err = nil
	}
	return
}
