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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

type Dialer interface {
	Dial(ctx context.Context, addr string) (transport.Conn, error)
}

type bufioConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufioConn) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

func newTcpConn(conn net.Conn, readSize int, writev bool) transport.Conn {
	if readSize > 0 {
		conn = &bufioConn{
			Conn:   conn,
			reader: bufio.NewReaderSize(conn, readSize),
		}
	}
	return transport.NetConn(conn, nil, writev)
}

type tcpDialer struct {
	timeout  time.Duration
	buffSize int
	writev   bool
}

func (t tcpDialer) Dial(ctx context.Context, addr string) (transport.Conn, error) {
	var d net.Dialer
	d.Timeout = t.timeout
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return newTcpConn(conn, t.buffSize, t.writev), nil
}

type rdmaDialer struct{}

func (rdmaDialer) Dial(ctx context.Context, addr string) (transport.Conn, error) {
	return nil, errors.New("rpc2: rdma not implements")
}

type Connector interface {
	Get(ctx context.Context, addr string) (*transport.Stream, error)
	Put(ctx context.Context, stream *transport.Stream, broken bool) error
	Stats() any
	Close() error
}

type limitStream struct {
	limit limit.Limiter
	ch    chan *transport.Stream
}

type connector struct {
	dialer Dialer
	config ConnectorConfig

	getn int64

	mu       sync.RWMutex
	sessions map[string]map[*transport.Session]struct{} // remote address
	streams  map[net.Addr]*limitStream                  // local address
}

type streamStats struct {
	Running  int `json:"running"`
	Buffered int `json:"buffered"`
}

type connectorStats struct {
	Config   ConnectorConfig        `json:"config"`
	Sessions map[string]int         `json:"sessions"`
	Streams  map[string]streamStats `json:"streams"`
}

type ConnectorConfig struct {
	Transport *TransportConfig `json:"transport,omitempty"`

	BufioReaderSize  int  `json:"bufio_reader_size"`
	ConnectionWriteV bool `json:"connection_writev"`

	// tcp or rdma
	Network     string        `json:"network"`
	Dialer      Dialer        `json:"-"`
	DialTimeout util.Duration `json:"dial_timeout"`

	MaxSessionPerAddress int `json:"max_session_per_address"`
	MaxStreamPerSession  int `json:"max_stream_per_session"`
}

func defaultConnector(config ConnectorConfig) Connector {
	defaulter.LessOrEqual(&config.MaxSessionPerAddress, int(4))
	defaulter.LessOrEqual(&config.MaxStreamPerSession, int(1024))
	dialer := config.Dialer
	if dialer == nil {
		switch config.Network {
		case "tcp":
			dialer = tcpDialer{
				timeout:  config.DialTimeout.Duration,
				buffSize: config.BufioReaderSize,
				writev:   config.ConnectionWriteV,
			}
		case "rdma":
			dialer = rdmaDialer{}
		default:
			panic("rpc2: connector network " + config.Network)
		}
	}
	if config.Transport == nil {
		config.Transport = DefaultTransportConfig()
	}
	return &connector{
		dialer:   dialer,
		config:   config,
		sessions: make(map[string]map[*transport.Session]struct{}),
		streams:  make(map[net.Addr]*limitStream),
	}
}

func (c *connector) Get(ctx context.Context, addr string) (*transport.Stream, error) {
	if atomic.AddInt64(&c.getn, 1)%(1<<10) == 3 {
		getSpan(ctx).Infof("stats: %+v", c.Stats())
	}
	return c.get(ctx, addr, false)
}

func (c *connector) get(ctx context.Context, addr string, newSession bool) (*transport.Stream, error) {
	span := getSpan(ctx).WithOperation("connector.get")
	c.mu.RLock()
	ses, ok := c.sessions[addr]
	sesLen := len(ses)
	c.mu.RUnlock()
	if !ok || sesLen == 0 || newSession {
		span.Debug("to new session for", addr)
		conn, err := c.dialer.Dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		sess, err := transport.Client(conn, c.config.Transport.Transport())
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
		defer c.mu.Unlock()
		if ses, ok = c.sessions[addr]; !ok {
			c.sessions[addr] = map[*transport.Session]struct{}{sess: {}}
		} else {
			if len(ses) >= c.config.MaxSessionPerAddress {
				sess.Close()
				return nil, ErrConnLimited
			} else {
				ses[sess] = struct{}{}
			}
		}
		c.streams[sess.LocalAddr()] = &limitStream{
			limit: count.New(c.config.MaxStreamPerSession),
			ch:    make(chan *transport.Stream, c.config.MaxStreamPerSession),
		}
		c.streams[sess.LocalAddr()].limit.Acquire()
		return stream, nil
	}

	// try to get opened stream
	span.Debug("to get opened stream for", addr)
	var stream *transport.Stream
	c.mu.RLock()
	sesCopy := make(map[*transport.Session]struct{}, sesLen)
	for sess := range c.sessions[addr] {
		ss := c.streams[sess.LocalAddr()]
		if err := ss.limit.Acquire(); err != nil {
			span.Infof("opened session(%v) limited(%d)", sess.LocalAddr(), ss.limit.Running())
			continue
		}
		select {
		case stream = <-ss.ch:
		default:
		}
		if stream != nil {
			break
		}
		ss.limit.Release()
		sesCopy[sess] = struct{}{}
	}
	c.mu.RUnlock()
	if stream != nil {
		return stream, nil
	}

	// try to open new stream
	span.Debug("to new stream for", addr)
	for sess := range sesCopy {
		newStream, err := sess.OpenStream()
		if err != nil {
			c.mu.Lock()
			delete(c.sessions[addr], sess)
			delete(c.streams, sess.LocalAddr())
			c.mu.Unlock()
			sess.Close()
			span.Warnf("close session(%d) -> %s", sess.LocalAddr(), err.Error())
			continue
		}

		c.mu.RLock()
		ss, hasStream := c.streams[sess.LocalAddr()]
		c.mu.RUnlock()
		if hasStream && ss.limit.Acquire() == nil {
			return newStream, nil
		}
		go func() { newStream.Close() }()
	}

	return c.get(ctx, addr, true)
}

func (c *connector) Put(ctx context.Context, stream *transport.Stream, broken bool) error {
	span := getSpan(ctx).WithOperation("connector.put")
	c.mu.RLock()
	ss, ok := c.streams[stream.LocalAddr()]
	c.mu.RUnlock()
	if ok {
		ss.limit.Release()
		if broken || stream.IsClosed() {
			span.Infof("close broken stream(%d %v)", stream.ID(), stream.LocalAddr())
			stream.Close()
			return nil
		}
		select {
		case ss.ch <- stream:
			span.Debugf("reuse the stream(%d %v)", stream.ID(), stream.LocalAddr())
		default:
			span.Infof("close full stream(%d %v)", stream.ID(), stream.LocalAddr())
			stream.Close()
		}
	}
	return nil
}

func (c *connector) Stats() any {
	st := connectorStats{
		Config:   c.config,
		Sessions: make(map[string]int),
		Streams:  make(map[string]streamStats),
	}
	c.mu.RLock()
	for addr := range c.sessions {
		st.Sessions[addr] = len(c.sessions[addr])
		for sess := range c.sessions[addr] {
			ssName := sess.LocalAddr().String() + "->" + sess.RemoteAddr().String()
			ss := c.streams[sess.LocalAddr()]
			st.Streams[ssName] = streamStats{
				Running:  ss.limit.Running(),
				Buffered: len(ss.ch),
			}
		}
	}
	c.mu.RUnlock()
	return st
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
	c.streams = make(map[net.Addr]*limitStream)
	c.mu.Unlock()
	if err == io.ErrClosedPipe {
		err = nil
	}
	return
}
