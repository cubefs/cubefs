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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	{
		var conn tcpConn
		require.Nil(t, conn.Allocator())
		require.Panics(t, func() { conn.ReadOnce() })
	}
	{
		var conf ConnectorConfig
		conf.Network = "xxx"
		require.Panics(t, func() { defaultConnector(conf) })
	}
	{
		var conf ConnectorConfig
		conf.Network = "rdma"
		c := defaultConnector(conf)
		_, err := c.Get(testCtx, "")
		require.Error(t, err)
	}
}
