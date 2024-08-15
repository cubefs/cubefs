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
			Handler:   defHandler,
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
			Handler: defHandler,
		}
		go func() { server.Serve() }()
		server.waitServe()
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
			Handler:   defHandler,
		}
		go func() { server.Serve() }()
		server.waitServe()
		connector := defaultConnector(ConnectorConfig{Network: "tcp"})
		_, err := connector.Get(testCtx, addr)
		require.NoError(t, err)
	}
}
