// Copyright 2022 The CubeFS Authors.
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

package rpc

import (
	"errors"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerRecovery(t *testing.T) {
	{
		router := New()
		router.Router.PanicHandler = defaultRecovery
		router.Use(func(c *Context) {
			panic("in interceptor")
		})
		router.Handle(http.MethodGet, "/", func(c *Context) {})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		require.Panics(t, func() {
			router.ServeHTTP(w, req)
		})
	}
	{
		router := New()
		router.Router.PanicHandler = defaultRecovery
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) {
			panic("in app")
		})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		require.Panics(t, func() {
			router.ServeHTTP(w, req)
		})
	}
	{
		router := New()
		router.Router.PanicHandler = defaultRecovery
		router.Use(func(c *Context) {})
		router.Handle(http.MethodGet, "/", func(c *Context) {
			panic(&net.OpError{
				Err: &os.SyscallError{
					Syscall: "App",
					Err:     errors.New("broken pipe"),
				},
			})
		})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		require.NotPanics(t, func() {
			router.ServeHTTP(w, req)
		})
	}
	{
		router := New()
		router.Router.PanicHandler = defaultRecovery
		router.Use(func(c *Context) {
			panic(&net.OpError{
				Err: &os.SyscallError{
					Syscall: "Middleware",
					Err:     errors.New("connection reset by peer"),
				},
			})
		})
		router.Handle(http.MethodGet, "/", func(c *Context) {})

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		require.NotPanics(t, func() {
			router.ServeHTTP(w, req)
		})
	}
}
