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
	"net"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

type Connector interface {
	Get(ctx context.Context, addr string) (*transport.Stream, error)
	Put(ctx context.Context, stream *transport.Stream) error
	Close()
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

	MaxStreamPerSession int
}

func DefaultConnector(dialer Dialer, config ConnectorConfig) Connector {
	defaulter.LessOrEqual(&config.MaxStreamPerSession, int(1024))
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
		sess, err := transport.Client(conn, c.config.Transport)
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
		c.sessions[addr] = map[*transport.Session]struct{}{sess: {}}
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
		}
	}
	return nil
}

func (c *connector) Close() {
	c.mu.Lock()
	for _, sesss := range c.sessions {
		for sess := range sesss {
			sess.Close()
		}
	}
	c.sessions = make(map[string]map[*transport.Session]struct{})
	c.streams = make(map[net.Addr]chan *transport.Stream)
	c.mu.Unlock()
}
