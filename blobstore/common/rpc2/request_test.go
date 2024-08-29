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
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/stretchr/testify/require"
)

func handleRequestTimeout(w ResponseWriter, req *Request) error {
	req.LocalAddrString()
	req.RemoteAddrString()
	req.GetReadableParameter()
	time.Sleep(10 * time.Second)
	return w.WriteOK(nil)
}

func TestRequestTimeout(t *testing.T) {
	var handler Router
	handler.Register("/", handleRequestTimeout)
	handler.Register("/none", handleNone)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	cli.RequestTimeout.Duration = 200 * time.Millisecond
	req, err := NewRequest(testCtx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	req = req.WithContext(context.Background())
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cli.RequestTimeout.Duration = 0

	ctx, cancel := context.WithTimeout(testCtx, 100*time.Millisecond)
	req, err = NewRequest(ctx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cancel()

	cli.Timeout.Duration = 200 * time.Millisecond
	req, err = NewRequest(testCtx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.ErrorIs(t, transport.ErrTimeout, err)
	cli.Timeout.Duration = 0

	buff := make([]byte, 8<<20)
	cli.ResponseTimeout.Duration = 200 * time.Millisecond
	req, err = NewRequest(testCtx, server.Name, "/none", nil, bytes.NewReader(buff))
	require.NoError(t, err)
	resp, err := cli.Do(req, nil)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
	_, err = io.ReadFull(resp.Body, buff)
	require.Error(t, err)
	cli.ResponseTimeout.Duration = 0

	cli.Timeout.Duration = time.Second
	ctx, cancel = context.WithTimeout(testCtx, 100*time.Millisecond)
	req, err = NewRequest(ctx, server.Name, "/none", nil, bytes.NewReader(buff))
	require.NoError(t, err)
	resp, err = cli.Do(req, nil)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
	_, err = io.ReadFull(resp.Body, buff)
	require.Error(t, err)
	cancel()
}

func TestRequestContextCancel(t *testing.T) {
	var handler Router
	handler.Register("/", handleRequestTimeout)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	ctx, cancel := context.WithTimeout(testCtx, 100*time.Millisecond)
	req, _ := NewRequest(ctx, server.Name, "/", nil, nil)
	require.Error(t, cli.DoWith(req, nil))
	cancel()

	ctx, cancel = context.WithCancel(testCtx)
	req, _ = NewRequest(ctx, server.Name, "/", nil, nil)
	cancel()
	require.Error(t, cli.DoWith(req, nil))
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

func handleBodyReadable(w ResponseWriter, req *Request) error {
	var args strMessage
	if len(req.Parameter) != 0 {
		return errors.New("not empty parameter")
	}
	if err := req.ParseParameter(&args); err != nil {
		return err
	}
	req.Body.Close()
	req.GetReadableParameter()
	if len(req.Parameter) == 0 {
		return errors.New("copy to parameter")
	}
	return w.WriteOK(nil)
}

func TestRequestBodyReadable(t *testing.T) {
	handler := &Router{}
	handler.Register("/", handleBodyReadable)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()

	args := &strMessage{str: "request data"}
	// request message in parameter & response message in body
	req, _ := NewRequest(testCtx, server.Name, "/", nil, Codec2Reader(args))
	req.OptionCrcUpload()
	req.ContentLength = int64(args.Size())
	require.NoError(t, cli.DoWith(req, nil))
}

func TestRequestRetryCrc(t *testing.T) {
	handler := &Router{}
	handler.Register("/", handleUpload)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()
	cli.Retry = 3
	cli.RequestTimeout.Duration = time.Second
	cli.RetryOn = func(err error) bool { return err != nil }

	args := &strMessage{str: "request retry crc"}
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	// request message in parameter & response message in body
	req, _ := NewRequest(testCtx, server.Name, "/", args, bytes.NewReader(buff))
	req.OptionCrcUpload()
	req.ContentLength = int64(len(buff)) + 1
	req.GetBody = func() (io.ReadCloser, error) {
		return nil, errors.New("get body")
	}
	require.Error(t, cli.DoWith(req, args))

	req.GetBody = func() (io.ReadCloser, error) {
		req.ContentLength = int64(len(buff))
		return io.NopCloser(bytes.NewReader(buff)), nil
	}
	require.NoError(t, cli.DoWith(req, args))
	hasher := req.checksum.Hasher()
	hasher.Write(buff)
	require.True(t, args.str == fmt.Sprint(req.checksum.Readable(hasher.Sum(nil))))
}
