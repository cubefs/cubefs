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
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestServerRouterDefault(t *testing.T) {
	log.SetOutputLevel(log.Linfo)
	defer setLogLevel()
	defer initDefaultRouter()

	str := ""
	Use(func(c *Context) {
		str += "m1"
	})
	Use(func(c *Context) {
		c.Next()
		str += "m2"
	})

	HEAD("/head", func(c *Context) { str += "head" })
	GET("/get", func(c *Context) { str += "get" })
	POST("/post", func(c *Context) { str += "post" })
	PUT("/put", func(c *Context) { str += "put" })
	DELETE("/delete", func(c *Context) { str += "delete" })
	OPTIONS("/options", func(c *Context) { str += "options" })
	PATCH("/patch", func(c *Context) { str += "patch" })
	Handle(http.MethodTrace, "/trace", func(c *Context) { str += "trace" })

	for _, method := range []string{
		"head", "get", "post", "put",
		"delete", "options", "patch", "trace",
	} {
		str = ""
		w := new(mockResponseWriter)
		req, _ := http.NewRequest(strings.ToUpper(method), "/"+method, nil)
		DefaultRouter.Router.ServeHTTP(w, req)

		require.Equal(t, "m1"+method+"m2", str)
	}
}

func TestServerRouterMaxHandlers(t *testing.T) {
	{
		router := New()
		router.Use(make([]HandlerFunc, abortIndex-2)...)
		router.Handle(http.MethodGet, "/", func(c *Context) {})
	}
	{
		router := New()
		router.Use(make([]HandlerFunc, abortIndex-1)...)
		require.Panics(t, func() {
			router.Handle(http.MethodGet, "/", func(c *Context) {})
		})
	}
	{
		router := New()
		require.Panics(t, func() {
			router.Use(make([]HandlerFunc, abortIndex)...)
		})
	}
}
