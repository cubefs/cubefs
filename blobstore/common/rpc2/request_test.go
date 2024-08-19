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

func handleRequestTimeout(w ResponseWriter, req *Request) error {
	time.Sleep(10 * time.Second)
	return w.WriteOK(nil)
}

func TestRequestTimeout(t *testing.T) {
	var handler Router
	handler.Register("/", handleRequestTimeout)
	handler.Register("/none", handleNone)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	cli.RequestTimeout = 200 * time.Millisecond
	req, err := NewRequest(testCtx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	req = req.WithContext(context.Background())
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cli.RequestTimeout = 0

	ctx, cancel := context.WithDeadline(testCtx, time.Now().Add(100*time.Millisecond))
	req, err = NewRequest(ctx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cancel()

	cli.Timeout = 200 * time.Millisecond
	req, err = NewRequest(testCtx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cli.Timeout = 0

	buff := make([]byte, 8<<20)
	cli.ResponseTimeout = 200 * time.Millisecond
	req, err = NewRequest(testCtx, server.Name, "/none", nil, bytes.NewReader(buff))
	require.NoError(t, err)
	resp, err := cli.Do(req, nil)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
	_, err = io.ReadFull(resp.Body, buff)
	require.Error(t, err)
	cli.ResponseTimeout = 0

	cli.Timeout = time.Second
	ctx, cancel = context.WithDeadline(testCtx, time.Now().Add(100*time.Millisecond))
	req, err = NewRequest(ctx, server.Name, "/none", nil, bytes.NewReader(buff))
	require.NoError(t, err)
	resp, err = cli.Do(req, nil)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
	_, err = io.ReadFull(resp.Body, buff)
	require.Error(t, err)
	cancel()
}

func TestRequestErrors(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()

	cli.ConnectorConfig.Transport.MaxFrameSize = 32
	req, err := NewRequest(testCtx, addr, "/", nil, nil)
	require.Panics(t, func() {
		req.OptionChecksum(ChecksumBlock{
			Algorithm: ChecksumAlgorithm_Alg_None,
			Direction: ChecksumDirection_Dir_None,
			BlockSize: 1 << 10,
		})
	})
	req.OptionCrcDownload()
	req.OptionCrcDownload()
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, ErrFrameHeader, err)
}
