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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	addr = "127.0.0.1:9999"
)

var cli = http.Client{Transport: &http.Transport{}}

func setLogLevel() {
	log.SetOutputLevel(log.Lerror)
}

func init() {
	mux := New()
	mux.Handle(http.MethodGet, "/", func(c *Context) {
		remoteIP, ok := c.RemoteIP()
		if ok {
			b, _ := remoteIP.MarshalText()
			c.RespondWith(200, "", b)
		} else {
			c.RespondError(NewError(400, "", fmt.Errorf("no remote ip")))
		}
	})
	mux.Handle(http.MethodGet, "/hijack", func(c *Context) {
		conn, _, err := c.Hijack()
		if err != nil {
			c.RespondError(err)
			return
		}
		conn.Close()
	})
	mux.Handle(http.MethodGet, "/flush", func(c *Context) {
		c.RespondWithReader(200, 1<<10, "", bytes.NewBuffer(make([]byte, 1<<10)), map[string]string{
			"x-rpc-name": "flush",
		})
		c.Flush()
	})
	mux.Handle(http.MethodGet, "/stream", func(c *Context) {
		n := 0
		clientGone := c.Stream(func(w io.Writer) bool {
			if n < 1<<20 {
				n += 1 << 10
				w.Write(make([]byte, 1<<10))
				time.Sleep(10 * time.Microsecond)
				return true
			}
			return false
		})

		if clientGone {
			log.Infof("client gone")
		}
	})

	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux.Router,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server exits:", err)
		}
	}()
	time.Sleep(time.Second)

	setLogLevel()
}

func TestServerResponse(t *testing.T) {
	{
		url := fmt.Sprintf("http://%s/", addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		buf, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, "127.0.0.1", string(buf))
	}
	{
		url := fmt.Sprintf("http://%s/hijack", addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		require.Error(t, io.EOF, err)
		if resp != nil {
			resp.Body.Close()
		}
	}
	{
		url := fmt.Sprintf("http://%s/flush", addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, "flush", resp.Header.Get("x-rpc-name"))
		buf := make([]byte, 1<<20)
		n, err := resp.Body.Read(buf)
		require.ErrorIs(t, io.EOF, err)
		require.Equal(t, 1024, n)
	}
	{
		url := fmt.Sprintf("http://%s/stream", addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		resp.Body.Close()

		require.Equal(t, 200, resp.StatusCode)
		buf := make([]byte, 1<<22)
		n, _ := resp.Body.Read(buf)
		require.Equal(t, 0, n)
	}
	{
		url := fmt.Sprintf("http://%s/stream", addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, 200, resp.StatusCode)
		buf := make([]byte, 1<<18)
		_, err = io.ReadFull(resp.Body, buf)
		require.NoError(t, err)
	}
}
