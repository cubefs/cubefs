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

package lib

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

func DoHttpWithResp(url, method string, params, reslut interface{}, header map[string]string) (err error) {
	var b io.Reader = http.NoBody
	if params != nil {
		msg, err := json.Marshal(params)
		if err != nil {
			log.LogErrorf("params is not legal, param %v, err %s", params, err.Error())
		}
		b = bytes.NewReader(msg)
	}

	req, err := http.NewRequest(method, url, b)
	if err != nil {
		log.LogErrorf("new req failed, url %s,  params %v, err %s", url, params, err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	if len(header) > 0 {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	client := http.DefaultClient
	client.Timeout = 10 * time.Second
	var resp *http.Response

	for idx := 0; idx < 3; idx++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}

		log.LogWarnf("do http req err, retry, url %s, param %v, err %s", url, params, err.Error())
		time.Sleep(time.Second)
	}

	if err != nil {
		log.LogErrorf("do http req err, retry, url %s, param %v, err %s", url, params, err.Error())
		return err
	}

	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(reslut)
	if err != nil {
		log.LogErrorf("decode reply error, url %s, err %s", url, err.Error())
		return
	}

	return
}

func InnerDoHttp(url, method string, params, result interface{}, header map[string]string) (err error) {
	var b io.Reader = http.NoBody
	if params != nil {
		msg, err := json.Marshal(params)
		if err != nil {
			log.LogErrorf("params is not legal, param %v, err %s", params, err.Error())
		}
		b = bytes.NewReader(msg)
	}

	req, err := http.NewRequest(method, url, b)
	if err != nil {
		log.LogErrorf("new req failed, url %s,  params %v, err %s", url, params, err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	if len(header) > 0 {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	client := http.DefaultClient
	client.Timeout = 10 * time.Second
	var resp *http.Response

	for idx := 0; idx < 3; idx++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}

		log.LogWarnf("do http req err, retry, url %s, param %v, err %s", url, params, err.Error())
		time.Sleep(time.Second)
	}

	if err != nil {
		log.LogErrorf("do http req err, retry, url %s, param %v, err %s", url, params, err.Error())
		return err
	}

	reply := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}

	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(reply)
	if err != nil {
		log.LogErrorf("decode reply error, url %s, err %s", url, err.Error())
		return
	}

	if reply.Code != 0 {
		log.LogErrorf("req %s failed, reply %v", url, reply)
		return newError(resp.StatusCode, int(reply.Code), reply.Msg)
	}

	if result == nil {
		return
	}

	err = json.Unmarshal([]byte(reply.Data), result)
	if err != nil {
		log.LogErrorf("unmarshal resp failed, url %s, reply %v, err %s", url, reply, err.Error())
		return
	}

	log.LogDebugf("get result data after unmarshal, data %v, data %s", result, string(reply.Data))

	return
}
