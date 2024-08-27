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

package auditlog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

type strMessage struct{ str string }

func (s *strMessage) Size() int                       { return len(s.str) }
func (s *strMessage) Marshal() ([]byte, error)        { return []byte(s.str), nil }
func (s *strMessage) MarshalTo(b []byte) (int, error) { return copy(b, []byte(s.str)), nil }
func (s *strMessage) Unmarshal(b []byte) error        { s.str = string(b); return nil }
func (s *strMessage) Readable() bool                  { return true }

var testCtx = context.Background()

type noCopyReadWriter struct{}

func (noCopyReadWriter) Read(p []byte) (int, error)  { return len(p), nil }
func (noCopyReadWriter) Write(p []byte) (int, error) { return len(p), nil }

func handleNone(w rpc2.ResponseWriter, req *rpc2.Request) error {
	span := req.Span()
	span.AppendTrackLogWithFunc("trace-before", func() error {
		time.Sleep(time.Millisecond)
		return nil
	})
	span.SetTag("tag-before", "tag-xxx")

	var para strMessage
	req.ParseParameter(&para)
	if req.ContentLength == 0 {
		req.Body.Close()
		w.WriteOK(&para)
		return w.WriteOK(nil)
	}

	if req.ContentLength > 1<<10 {
		w.SetContentLength(req.ContentLength)
		w.ReadFrom(noCopyReadWriter{})
		return nil
	}

	req.Body.WriteTo(rpc2.LimitWriter(noCopyReadWriter{}, req.ContentLength))
	w.SetContentLength(req.ContentLength)
	w.WriteHeader(200, &para)
	w.WriteHeader(200, &para)
	_, err := w.ReadFrom(noCopyReadWriter{})
	w.ReadFrom(noCopyReadWriter{})

	span.AppendTrackLogWithFunc("trace-after", func() error {
		time.Sleep(time.Millisecond)
		return nil
	})
	span.SetTag("tag-after", "tag-yyy")

	extraHeader := ExtraHeader(w)
	extraHeader.Set("header-extra", "extra")
	return err
}

func newTcpServer(cfg Config) (string, *rpc2.Client, func()) {
	defHandler := &rpc2.Router{}
	defHandler.Register("/", handleNone)
	defHandler.Register("/filter", handleNone)
	server, client, f := newServer(cfg, "tcp", defHandler)
	return server.Name, client, f
}

func newServer(cfg Config, network string, router *rpc2.Router) (*rpc2.Server, *rpc2.Client, func()) {
	tmpDir := fmt.Sprintf("%s/auditlog-%s%s", os.TempDir(),
		strconv.FormatInt(time.Now().Unix(), 10), strconv.Itoa(rand.Intn(100000)))
	os.Mkdir(tmpDir, 0o755)
	cfg.LogDir = tmpDir
	var it rpc2.Interceptor
	it, lc, err := Open(cfg.MetricConfig.Idc, &cfg)
	if err != nil {
		panic(err)
	}
	router.Interceptor(it)

	addr := getAddress(network)
	server := rpc2.Server{
		Name:      addr,
		Addresses: []rpc2.NetworkAddress{{Network: network, Address: addr}},
		Handler:   router.MakeHandler(),
	}
	go func() {
		if err := server.Serve(); err != nil && err != rpc2.ErrServerClosed {
			panic(err)
		}
	}()
	server.WaitServe()
	client := rpc2.Client{
		ConnectorConfig: rpc2.ConnectorConfig{Network: network},
	}
	return &server, &client, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		server.Shutdown(ctx)
		client.Close()
		lc.Close()
		os.RemoveAll(tmpDir)
	}
}

func getAddress(network string) (addr string) {
	if err := retry.Timed(10, 1).On(func() error {
		ln, err := net.Listen(network, "127.0.0.1:0")
		if err != nil {
			return err
		}
		if err = ln.Close(); err != nil {
			return err
		}
		addr = ln.Addr().String()
		return nil
	}); err != nil {
		panic(err)
	}
	return
}

func TestRpc2Open(t *testing.T) {
	var cfg Config
	cfg.Filters = []FilterConfig{{Must: Conditions{"term": {"method": "SYN"}}}}
	cfg.MetricConfig.Idc = "TestRpc2Open"

	addr, cli, shutdown := newTcpServer(cfg)
	defer shutdown()

	para := &strMessage{"'test rpc2 open'"}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, nil)
		require.NoError(t, cli.DoWith(req, para))
	}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, nil)
		require.NoError(t, cli.DoWith(req, para))
	}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/", nil, rpc2.Codec2Reader(para))
		require.NoError(t, cli.DoWith(req, para))
	}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/", nil, rpc2.Codec2Reader(para))
		require.NoError(t, cli.DoWith(req, para))
	}
	{
		buff := make([]byte, 1<<10)
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, bytes.NewReader(buff))
		resp, err := cli.Do(req, para)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	{
		buff := make([]byte, 1<<10)
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, bytes.NewReader(buff))
		resp, err := cli.Do(req, para)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	{
		buff := make([]byte, 2<<10)
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, bytes.NewReader(buff))
		require.NoError(t, cli.DoWith(req, nil))
	}
}

func TestRpc2Filter(t *testing.T) {
	var cfg Config
	cfg.Filters = []FilterConfig{{Must: Conditions{"term": {"path": "/filter"}}}}
	cfg.MetricConfig.Idc = "TestRpc2Filter"
	cfg.LogFormat = LogFormatJSON

	addr, cli, shutdown := newTcpServer(cfg)
	defer shutdown()

	para := &strMessage{"'test rpc2 filter'"}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/", para, nil)
		require.NoError(t, cli.DoWith(req, para))
	}
	{
		req, _ := rpc2.NewRequest(testCtx, addr, "/filter", nil, rpc2.Codec2Reader(para))
		require.NoError(t, cli.DoWith(req, para))
	}
}
