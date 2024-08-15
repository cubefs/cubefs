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
