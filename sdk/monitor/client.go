// Copyright 2018 The CubeFS Authors.
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

package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/chubaofs/chubaofs/util/log"
)

type MonitorClient struct {
	sync.Mutex

	monitorNodes string
	enableHTTPS  bool

	monitorAPI *MonitorAPI
}

func (mc *MonitorClient) MonitorAPI() *MonitorAPI {
	return mc.monitorAPI
}

func NewMonitorClient(monitors string, enableHTTPS bool) *MonitorClient {
	var mc = &MonitorClient{
		monitorNodes: monitors,
		enableHTTPS:  enableHTTPS,
	}

	mc.monitorAPI = &MonitorAPI{mc: mc}
	return mc
}

//resp, err = c.httpRequest(r.method, url, r.params, r.header, r.body)
func (mc *MonitorClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	var req *http.Request
	fullUrl := mc.mergeRequestUrl(url, param)
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	return
}

func (mc *MonitorClient) mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		return buff.String()
	}
	return url
}

// TODO---.io 和 address[]string
func (mc *MonitorClient) serveMonitorRequest(r *request) (repsData []byte, err error) {
	var (
		resp     *http.Response
		urlProto string
		url      string
	)
	// maybe for
	requestAddr := mc.monitorNodes
	if mc.enableHTTPS {
		urlProto = "https"
	} else {
		urlProto = "http"
	}

	url = fmt.Sprintf("%s://%s%s", urlProto, requestAddr, r.path)
	resp, err = mc.httpRequest(r.method, url, r.params, r.header, r.body)
	if err != nil {
		log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	stateCode := resp.StatusCode
	repsData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		log.LogErrorf("serveRequest: read http response body fail: err(%v)", err)
		//continue
		return
	}
	if stateCode == http.StatusOK {
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}
		if err := json.Unmarshal(repsData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)
		}
		return body.Data, nil
	}
	err = fmt.Errorf("send request err: req addr[%v], req path[%v], err[%v]", requestAddr, r.path, err)
	return
}
