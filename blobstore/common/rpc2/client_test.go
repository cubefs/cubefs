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
	"fmt"
	"io"
	"strings"
	"testing"

	auth_proto "github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/stretchr/testify/require"
)

type errNoParameter struct{ Codec }

func (errNoParameter) Marshal() ([]byte, error) { return nil, fmt.Errorf("codec") }
func (errNoParameter) Unmarshal([]byte) error   { return fmt.Errorf("codec") }

func TestClientRetry(t *testing.T) {
	var emptyCli Client
	emptyCli.Close()

	addr, cli, shutdown := newTcpServer()
	defer shutdown()

	for _, r := range []io.Reader{
		bytes.NewBuffer([]byte("error")),
		bytes.NewReader([]byte("error")),
		strings.NewReader("error"),
	} {
		req, err := NewRequest(testCtx, addr, "/error", nil, r)
		require.NoError(t, err)
		require.Error(t, cli.DoWith(req, nil))

		req, err = NewRequest(testCtx, addr, "/error", nil, r)
		require.NoError(t, err)
		req.GetBody = func() (io.ReadCloser, error) { return nil, fmt.Errorf("error") }
		require.Error(t, cli.DoWith(req, nil))
	}
}

func TestClientCodec(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	_, err := NewRequest(testCtx, addr, "/", errNoParameter{NoParameter}, nil)
	require.Error(t, err)
	req, err := NewRequest(testCtx, addr, "/", nil, bytes.NewReader(make([]byte, 4)))
	require.NoError(t, err)
	require.Error(t, cli.DoWith(req, errNoParameter{NoParameter}))
}

func TestClientLbNoSelector(t *testing.T) {
	cli := &Client{}
	cli.ConnectorConfig.Network = "tcp"
	require.ErrorIs(t, cli.DoWith(&Request{}, nil), ErrConnNoAddress)

	_, cli, shutdown := newTcpServer()
	defer shutdown()
	cli.Retry = 1000
	cli.LbConfig.Hosts = []string{"127.0.0.1:ff", "127.0.0.1:ee"}
	req, err := NewRequest(testCtx, "", "/", nil, bytes.NewReader(nil))
	require.NoError(t, err)
	require.Error(t, cli.DoWith(req, nil), ErrConnNoAddress)
}

func TestClientLbSelector(t *testing.T) {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	cli.LbConfig.Hosts = []string{"127.0.0.1:ff", "127.0.0.1:ee", addr}
	for range [100]struct{}{} {
		req, err := NewRequest(testCtx, "", "/", nil, bytes.NewReader(nil))
		require.NoError(t, err)
		require.NoError(t, cli.DoWith(req, nil))
	}
	cli.Close()
}

func handleClientAuth(w ResponseWriter, req *Request) error {
	secret := req.Header.Get("token-secret")
	return auth_proto.Decode(req.Header.Get(auth_proto.TokenHeaderKey),
		[]byte(req.RemotePath), []byte(secret))
}

func TestClientAuth(t *testing.T) {
	var handler Router
	handler.Register("/", handleClientAuth)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	cli.Auth.EnableAuth = true
	cli.Auth.Secret = "secret"
	req, _ := NewRequest(testCtx, server.Name, "/", nil, nil)
	require.Error(t, cli.DoWith(req, nil))

	req, _ = NewRequest(testCtx, server.Name, "/", nil, nil)
	req.Header.Set("token-secret", cli.Auth.Secret)
	require.NoError(t, cli.DoWith(req, nil))
}

func TestClientCodecRetry(t *testing.T) {
	var handler Router
	handler.Register("/", handleError)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()

	args := &strMessage{AnyCodec[string]{Value: "retry request message"}}
	err := cli.Request(testCtx, server.Name, "/", args, nil)
	require.Error(t, err)
}
