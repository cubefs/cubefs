// Copyright 2023 The CubeFS Authors.
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

package flashnode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/require"
)

var (
	masterAddr     string
	masterServer   *httptest.Server
	flashPort      int
	flashServer    *FlashNode
	flashHTTP      int
	httpServer     *http.Server
	extentListener net.Listener
	closeCh        = make(chan struct{})

	_nodeID     uint64
	_apiGetIP   uint32
	_apiAddNode uint32
)

func init() {
	proto.InitBufferPool(32768)

	masterServer = httptest.NewServer(http.HandlerFunc(handleMaster))
	masterAddr = masterServer.URL[len("http://"):]

	flashPort = getFreePort()
	flashHTTP = getFreePort()

	initExtentListener()
}

func getFreePort() int {
	server := httptest.NewServer(http.HandlerFunc(nil))
	defer server.Close()
	port, err := strconv.Atoi(strings.SplitN(server.URL[len("http://"):], ":", 2)[1])
	if err != nil {
		panic(err)
	}
	return port
}

func handleMaster(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case proto.AdminGetIP:
		switch atomic.AddUint32(&_apiGetIP, 1) {
		case 1:
			w.Write([]byte{'{', ']'})
		case 2:
			b, _ := json.Marshal(proto.HTTPReply{Data: proto.ClusterInfo{Cluster: "test"}})
			w.Write(b)
		default:
			b, _ := json.Marshal(proto.HTTPReply{Data: proto.ClusterInfo{Cluster: "test", Ip: "127.0.0.1"}})
			w.Write(b)
		}
	case proto.FlashNodeAdd:
		switch atomic.AddUint32(&_apiAddNode, 1) {
		case 1:
			w.Write([]byte{'{', ']'})
		default:
			b, _ := json.Marshal(proto.HTTPReply{Data: atomic.AddUint64(&_nodeID, 10)})
			w.Write(b)
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func newFlashnode() *FlashNode {
	return &FlashNode{mc: master.NewMasterClient([]string{masterAddr}, false)}
}

func TestFlashNode(t *testing.T) {
	t.Run("New", testNew)
	t.Run("Config", testConfig)
	t.Run("TCP", testTCP)
	t.Run("HTTP", testHTTP)
	t.Run("Shotdown", testShutdown)
}

func testNew(t *testing.T) {
	f := newFlashnode()
	f.register()
}

func testConfig(t *testing.T) {
	flashServer = newFlashnode()
	httpServer = &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", flashHTTP),
		Handler: http.DefaultServeMux,
	}

	var cfg *config.Config
	require.Error(t, flashServer.parseConfig(cfg))
	confStr := "{}"
	for _, lines := range [][]string{
		{fmt.Sprintf("\"listen\":\"%d\"", flashPort), `"listen":""`, `"listen":" "`},
		{`"zoneName":"zone",`, `"zoneName":"",`},
		{`"readRps":0,`, `"readRps":-1,"memPercent":0.001,`},
		{``, `"memPercent":0.9,`, `"memPercent":0.001,`},
		{`"memPercent":0.2,`, `"memTotal":1024,`},
		{fmt.Sprintf("\"masterAddr\":[\"%s\"],", masterAddr), `"masterAddr":[],`},
	} {
		for _, line := range lines[1:] {
			errStr := confStr[:1] + line + confStr[1:]
			cfg = config.LoadConfigString(errStr)
			require.Error(t, flashServer.parseConfig(cfg))
		}
		confStr = confStr[:1] + lines[0] + confStr[1:]
	}
	cfg = config.LoadConfigString(confStr)
	require.NoError(t, flashServer.parseConfig(cfg))
	require.NoError(t, flashServer.start(cfg))
	t.Log("listen tcp    on:", flashServer.localAddr)
	t.Log("listen extent on:", extentListener.Addr().String())

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.LogFatal("http server exits:", err)
		}
	}()
	t.Log("listen http   on:", httpServer.Addr)
	time.Sleep(time.Second)
}

func testShutdown(t *testing.T) {
	masterServer.Close()
	flashServer.shutdown()
	httpServer.Shutdown(context.Background())
	extentListener.Close()
	close(closeCh)
}
