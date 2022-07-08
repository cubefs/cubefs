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
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type testRespData struct {
	Result string `json:"result"`
}

func TestOpen(t *testing.T) {
	moduleName := "TESTMOULE"
	tracer := trace.NewTracer(moduleName)
	trace.SetGlobalTracer(tracer)

	tmpDir := os.TempDir() + "/testauditog" + strconv.FormatInt(time.Now().Unix(), 10) + strconv.Itoa(rand.Intn(100000))
	err := os.Mkdir(tmpDir, 0o755)
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	ah, lc, err := Open(moduleName, &Config{LogDir: tmpDir, ChunkBits: 29})
	assert.NoError(t, err)
	assert.NotNil(t, ah)
	assert.NotNil(t, lc)
	defer lc.Close()
	assert.NoError(t, err)

	bussinessHandler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("testh1", "testh1value")
		w.Header().Set("Content-Type", rpc.MIMEJSON)
		w.WriteHeader(http.StatusOK)
		data, err := json.Marshal(testRespData{Result: "success"})
		assert.NoError(t, err)
		w.Write(data)
	}
	entryHandler := func(w http.ResponseWriter, req *http.Request) {
		ah.Handler(w, req, bussinessHandler)
	}

	server := httptest.NewServer(http.HandlerFunc(entryHandler))
	defer server.Close()

	url := server.URL
	client := http.DefaultClient

	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete, http.MethodPut} {
		req, err := http.NewRequest(method, url, nil)
		assert.NoError(t, err)
		resp, err := client.Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		b, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err)
		respData := &testRespData{}
		err = json.Unmarshal(b, respData)
		assert.NoError(t, err)
		assert.Equal(t, "success", respData.Result)
	}
}
