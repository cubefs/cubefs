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

package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testServer *httptest.Server

var (
	testSecret = "testSecret"
	testName   = "test"
)

type ret struct {
	Name string `json:"name"`
}

func init() {
	authHandler := NewAuthHandler(&Config{EnableAuth: true, Secret: testSecret})
	http.HandleFunc("/get/name", func(w http.ResponseWriter, r *http.Request) {
		authHandler.Handler(w, r, func(w http.ResponseWriter, r *http.Request) {
			data, _ := json.Marshal(&ret{Name: testName})
			w.Write(data)
		})
	})
	testServer = httptest.NewServer(http.DefaultServeMux)
}

func TestAuth(t *testing.T) {
	// invalid secret
	tc := NewAuthTransport(&http.Transport{}, &Config{EnableAuth: true, Secret: "wrongSecret"})
	client := http.Client{
		Transport: tc,
	}
	req, err := http.NewRequest("POST", testServer.URL+"/get/name?id="+strconv.Itoa(101), nil)
	assert.NoError(t, err)
	response, err := client.Do(req)
	assert.NoError(t, err)
	defer response.Body.Close()
	assert.Equal(t, http.StatusForbidden, response.StatusCode)

	// valid secret
	tc = NewAuthTransport(&http.Transport{}, &Config{EnableAuth: true, Secret: testSecret})
	client = http.Client{
		Transport: tc,
	}
	result := &ret{}
	req, err = http.NewRequest("POST", testServer.URL+"/get/name?id="+strconv.Itoa(101), nil)
	assert.NoError(t, err)
	response, err = client.Do(req)
	assert.NoError(t, err)
	defer response.Body.Close()
	assert.Equal(t, http.StatusOK, response.StatusCode)
	err = json.NewDecoder(response.Body).Decode(result)
	assert.NoError(t, err)
	assert.Equal(t, testName, result.Name)

	// test empty token
	c := http.Client{}
	req, err = http.NewRequest("POST", testServer.URL+"/get/name?id="+strconv.Itoa(101), nil)
	assert.NoError(t, err)
	response, err = c.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()

	// test token failed
	req.Header.Set(TokenHeaderKey, "#$@%DF#$@#$")
	response, err = c.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()
}
