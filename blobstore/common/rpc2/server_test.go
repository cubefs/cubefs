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
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/stretchr/testify/require"
)

func TestServerError(t *testing.T) {
	{
		addr := getAddress("tcp")
		server := Server{
			Addresses: []NetworkAddress{{Network: "network", Address: addr}},
			Handler:   defHandler.MakeHandler(),
		}
		err := server.Serve()
		require.Error(t, err)
	}
	{
		addr := getAddress("tcp")
		addr1 := getAddress("tcp")
		server := Server{
			Addresses: []NetworkAddress{
				{Network: "tcp", Address: addr},
				{Network: "tcp", Address: addr1},
			},
			Handler: defHandler.MakeHandler(),
		}
		go func() { server.Serve() }()
		server.WaitServe()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		server.Shutdown(ctx)
		cancel()
	}
	{
		trans := transport.DefaultConfig()
		trans.MaxFrameSize = 1 << 30
		addr := getAddress("tcp")
		server := Server{
			Transport: trans,
			Addresses: []NetworkAddress{{Network: "tcp", Address: addr}},
			Handler:   defHandler.MakeHandler(),
		}
		go func() { server.Serve() }()
		server.WaitServe()
		connector := defaultConnector(ConnectorConfig{Network: "tcp"})
		_, err := connector.Get(testCtx, addr)
		require.NoError(t, err)
	}
}

func handleServerTimeout(w ResponseWriter, req *Request) error {
	if req.ContentLength > 0 {
		buff := make([]byte, req.ContentLength)
		if _, err := io.ReadFull(req.Body, buff); err != nil {
			return err
		}
	}
	time.Sleep(300 * time.Millisecond)
	return w.WriteOK(nil)
}

func TestServerTimeout(t *testing.T) {
	handler := &Router{}
	handler.Register("/", handleServerTimeout)
	addr := getAddress("tcp")
	trans := transport.DefaultConfig()
	trans.MaxFrameSize = 32 << 10
	server := Server{
		Transport:       trans,
		BufioReaderSize: 1 << 20,
		Addresses:       []NetworkAddress{{Network: "tcp", Address: addr}},
		Handler:         handler.MakeHandler(),
		ReadTimeout:     200 * time.Millisecond,
		WriteTimeout:    200 * time.Millisecond,
	}
	go func() { server.Serve() }()
	server.WaitServe()

	cli := Client{
		ConnectorConfig: ConnectorConfig{
			Transport:   trans,
			Network:     "tcp",
			DialTimeout: 200 * time.Millisecond,
		},
		Retry: 1,
	}

	defer func() {
		cli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		server.Shutdown(ctx)
		cancel()
	}()

	req, err := NewRequest(testCtx, addr, "/", nil, bytes.NewReader(make([]byte, 32<<10)))
	require.NoError(t, err)
	require.Error(t, cli.DoWith(req, nil))
}
