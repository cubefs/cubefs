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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type (
	testMiddleware struct {
		Name   string
		RunIn  func() error
		RunOut func() error
		ProgressHandler
	}
)

func (mw testMiddleware) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	fmt.Println("In  Middleware", mw.Name)
	if err := mw.RunIn(); err != nil {
		return
	}
	f(w, req)
	if err := mw.RunOut(); err != nil {
		return
	}
	fmt.Println("Out Middleware", mw.Name)
}

func TestMiddlewareBase(t *testing.T) {
	{
		var routed string
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) { routed += "app" })
		handler := MiddlewareHandlerWith(router)

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "app", routed)
	}
	{
		var routed string
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) { routed += "app" })
		handler := MiddlewareHandlerFuncWith(router,
			testMiddleware{
				Name: "middleware1",
				RunIn: func() error {
					routed += "1-in"
					return nil
				},
				RunOut: func() error {
					routed += "1-out"
					return nil
				},
			},
		)

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "1-inapp1-out", routed)
	}
	{
		var routed string
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) { routed += "app" })
		handler := MiddlewareHandlerWith(router,
			testMiddleware{
				Name: "middleware1",
				RunIn: func() error {
					routed += "1-in"
					return errors.New("")
				},
				RunOut: func() error {
					routed += "1-out"
					return nil
				},
			},
		)

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "1-in", routed)
	}
	{
		var routed string
		router := New()
		router.Handle(http.MethodGet, "/", func(c *Context) { routed += "app" })

		handler := MiddlewareHandlerWith(router,
			testMiddleware{
				Name: "middleware1",
				RunIn: func() error {
					routed += "1-in"
					return nil
				},
				RunOut: func() error {
					routed += "1-out"
					return nil
				},
			},
			testMiddleware{
				Name: "middleware2",
				RunIn: func() error {
					routed += "2-in"
					return nil
				},
				RunOut: func() error {
					routed += "2-out"
					return errors.New("")
				},
			},
			testMiddleware{
				Name: "middleware3",
				RunIn: func() error {
					routed += "3-in"
					return errors.New("")
				},
				RunOut: func() error {
					routed += "3-out"
					return errors.New("")
				},
			},
			testMiddleware{
				Name: "middleware4",
				RunIn: func() error {
					routed += "4-in"
					return errors.New("")
				},
				RunOut: func() error {
					routed += "4-out"
					return nil
				},
			},
		)

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "1-in2-in3-in2-out1-out", routed)
	}
}

func TestMiddlewareDefault(t *testing.T) {
	defer initDefaultRouter()
	{
		var routed string
		GET("/", func(c *Context) { routed += "app" })
		handler := MiddlewareHandler()

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "app", routed)
	}
	{
		var routed string
		GET("/get", func(c *Context) { routed += "app" })
		handler := MiddlewareHandlerFunc(
			testMiddleware{
				Name: "middleware1",
				RunIn: func() error {
					routed += "1-in"
					return nil
				},
				RunOut: func() error {
					routed += "1-out"
					return nil
				},
			},
		)

		w := new(mockResponseWriter)
		req, _ := http.NewRequest(http.MethodGet, "/get", nil)
		handler.ServeHTTP(w, req)
		require.Equal(t, "1-inapp1-out", routed)
	}
}
