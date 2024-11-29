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

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

func handleResponseDoubleOk(w ResponseWriter, req *Request) error {
	w.WriteHeader(200, nil)
	w.WriteHeader(200, nil)
	w.WriteOK(NoParameter)
	return w.WriteOK(nil)
}

func handleResponseDoubleStatus(w ResponseWriter, req *Request) error {
	size := int64(10)
	w.SetContentLength(size)
	w.Flush()
	resp := w.(*response)
	resp.Write(make([]byte, size-1))
	resp.Write(make([]byte, size))
	resp.Write(make([]byte, size))
	return nil
}

// response has wrote 200 OK
func handleResponseAfterError(w ResponseWriter, req *Request) error {
	w.AfterBody(func() error { return NewError(511, "", "after body") })
	return w.WriteOK(nil)
}

func handleResponseWriteBody(w ResponseWriter, req *Request) error {
	w.WriteHeader(200, nil)
	_, err := w.WriteBody(func(cb ChecksumBlock, conn *transport.Stream) (int64, error) {
		if cb != (ChecksumBlock{}) {
			return 0, NewError(400, "Checksum", "no checksum")
		}
		return 0, nil
	})
	return err
}

func handleResponseClosed(w ResponseWriter, req *Request) error {
	resp := w.(*response)
	resp.hdr.GoString()
	resp.conn.Close()
	return w.WriteOK(nil)
}

func TestResponseError(t *testing.T) {
	var handler Router
	handler.Register("/ok", handleResponseDoubleOk)
	handler.Register("/status", handleResponseDoubleStatus)
	handler.Register("/after", handleResponseAfterError)
	handler.Register("/writebody", handleResponseWriteBody)
	handler.Register("/closed", handleResponseClosed)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	req, err := NewRequest(testCtx, server.Name, "/ok", nil, nil)
	require.NoError(t, err)
	require.NoError(t, cli.DoWith(req, nil))
	req, err = NewRequest(testCtx, server.Name, "/status", nil, nil)
	require.NoError(t, err)
	require.NoError(t, cli.DoWith(req, nil))
	req, err = NewRequest(testCtx, server.Name, "/after", nil, nil)
	require.NoError(t, err)
	require.NoError(t, cli.DoWith(req, nil))
	req, err = NewRequest(testCtx, server.Name, "/writebody", nil, nil)
	req.OptionCrc()
	require.NoError(t, err)
	require.NoError(t, cli.DoWith(req, nil))
	req, err = NewRequest(testCtx, server.Name, "/closed", nil, nil)
	require.NoError(t, err)
	require.Error(t, cli.DoWith(req, nil))
}
