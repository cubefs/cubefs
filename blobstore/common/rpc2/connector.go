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
)

type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

type Connector interface {
	Get(ctx context.Context, addr string) (*transport.Stream, error)
	Put(ctx context.Context, stream *transport.Stream) error
}

type sessionStreams struct {
	session *transport.Session
	streamC chan *transport.Stream
}

type connector struct {
	dialer Dialer
	config *transport.Config

	mu       sync.RWMutex
	sessions map[string]sessionStreams
}

func DefaultConnector(dialer Dialer, config *transport.Config) Connector {
	return &connector{
		dialer:   dialer,
		config:   config,
		sessions: make(map[string]sessionStreams),
	}
}

func (c *connector) Get(ctx context.Context, addr string) (*transport.Stream, error) {
	c.mu.RLock()
	ss, ok := c.sessions[addr]
	c.mu.RUnlock()
	if ok {
		if ss.session.IsClosed() {
		}
	}
	return nil, nil
}

func (c *connector) Put(ctx context.Context, stream *transport.Stream) error {
	return nil
}
