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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerCrcDecode(t *testing.T) {
	router := New()
	router.Handle(http.MethodPut, "/put/:size", func(c *Context) {
		size, _ := strconv.Atoi(c.Param.ByName("size"))

		n, err := io.Copy(ioutil.Discard, c.Request.Body)
		if err != nil && err != io.EOF {
			c.RespondError(err)
			return
		}

		if int(n) != size {
			c.RespondError(NewError(500, "size", fmt.Errorf("%d != %d", n, size)))
			return
		}
		c.Respond()
	})

	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1024", body)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.status)
	}
	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1024", body)
		WithCrcEncode()(req)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.status)
	}
	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1025", body)
		WithCrcEncode()(req)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusInternalServerError, w.status)
	}
}

func TestServerCrcDecodeWithMiddleware(t *testing.T) {
	router := New()
	router.Handle(http.MethodPut, "/put/:size", func(c *Context) {
		size, _ := strconv.Atoi(c.Param.ByName("size"))

		n, err := io.Copy(ioutil.Discard, c.Request.Body)
		if err != nil && err != io.EOF {
			c.RespondError(err)
			return
		}

		if int(n) != size {
			c.RespondError(NewError(500, "size", fmt.Errorf("%d != %d", n, size)))
			return
		}
		c.Respond()
	})

	handler := MiddlewareHandlerFuncWith(router)
	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1024", body)
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.status)
	}
	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1024", body)
		WithCrcEncode()(req)
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.status)
	}
	{
		w := new(mockResponseWriter)
		body := bytes.NewBuffer(make([]byte, 1024))
		req, _ := http.NewRequest(http.MethodPut, "/put/1025", body)
		WithCrcEncode()(req)
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusInternalServerError, w.status)
	}
}
