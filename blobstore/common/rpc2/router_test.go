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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func handleMiddlewareNil(w ResponseWriter, req *Request) error   { return nil }
func handleMiddlewareError(w ResponseWriter, req *Request) error { return &Error{Status: 555} }
func handleRouteError(w ResponseWriter, req *Request) error      { return &Error{Status: 666} }
func handleRoutePanic(w ResponseWriter, req *Request) error      { panic("route") }

func TestRpc2Router(t *testing.T) {
	{
		var handler Router
		handler.Middleware(handleMiddlewareError)
		handler.Register("/", handleRouteError)
		server, cli, shutdown := newServer("tcp", &handler)
		defer shutdown()
		req, err := NewRequest(testCtx, server.Name, "/", nil, nil)
		require.NoError(t, err)
		err = cli.DoWith(req, nil)
		require.Equal(t, 555, DetectStatusCode(err))
	}
	{
		var handler Router
		handler.Register("/", handleRouteError)
		server, cli, shutdown := newServer("tcp", &handler)
		defer shutdown()
		req, err := NewRequest(testCtx, server.Name, "/404", nil, nil)
		require.NoError(t, err, ErrorString(err))
		err = cli.DoWith(req, nil)
		require.Equal(t, 404, DetectStatusCode(err), ErrorString(err))

		req, err = NewRequest(testCtx, server.Name, "/", nil, nil)
		require.NoError(t, err)
		err = cli.DoWith(req, nil)
		require.Equal(t, 666, DetectStatusCode(err))
	}
	{
		var handler Router
		handler.Register("/", handleRoutePanic)
		server, cli, shutdown := newServer("tcp", &handler)
		defer shutdown()
		req, err := NewRequest(testCtx, server.Name, "/", nil, nil)
		require.NoError(t, err)
		err = cli.DoWith(req, nil)
		require.Equal(t, 597, DetectStatusCode(err))

		handler.PanicHandler = func(ResponseWriter, *Request, interface{}, []byte) error {
			return &Error{Status: 777}
		}
		req, err = NewRequest(testCtx, server.Name, "/", nil, nil)
		require.NoError(t, err)
		err = cli.DoWith(req, nil)
		require.Equal(t, 777, DetectStatusCode(err), ErrorString(fmt.Errorf("panic 777")))
	}
	{
		var handler Router
		handler.Register("/", handleRoutePanic)
		require.Panics(t, func() { handler.Register("/", handleRoutePanic) })
		require.Panics(t, func() {
			for range [1 << 10]struct{}{} {
				handler.Middleware(handleMiddlewareNil)
			}
			handler.Middleware(handleMiddlewareNil)
		})
		require.Panics(t, func() { handler.Interceptor(interceptor{"x"}) })
	}
}

type panicInterceptor struct{}

func (panicInterceptor) Handle(w ResponseWriter, req *Request, h Handle) error {
	panic("interceptor")
}

func TestRpc2RouterInterceptor(t *testing.T) {
	var handler Router
	handler.Interceptor(panicInterceptor{})
	handler.Register("/", handleNone)
	server, cli, shutdown := newServer("tcp", &handler)
	defer shutdown()
	req, err := NewRequest(testCtx, server.Name, "/", nil, nil)
	require.NoError(t, err)
	err = cli.DoWith(req, nil)
	require.Equal(t, DefaultStatusPanic, DetectStatusCode(err))
}
