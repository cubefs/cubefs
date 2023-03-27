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

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	requestTimeout = 30 * time.Second
)

type MetaHttpClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewMasterHelper returns a new MasterClient instance.
func NewMetaHttpClient(host string, useSSL bool) *MetaHttpClient {
	mc := &MetaHttpClient{host: host, useSSL: useSSL}
	var err error
	_, err = log.InitLog("/tmp/cfs", "cli", log.DebugLevel, nil)
	if err != nil {
		fmt.Printf("init cli log err[%v]", err)
	}
	return mc
}

type request struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func (r *request) addParam(key, value string) {
	r.params[key] = value
}

func (r *request) addHeader(key, value string) {
	r.header[key] = value
}

func (r *request) addBody(body []byte) {
	r.body = body
}

func newAPIRequest(method string, path string) *request {
	return &request{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

type RespBody struct {
}

func (c *MetaHttpClient) serveRequest(r *request) (respData []byte, err error) {
	var resp *http.Response
	var schema string
	if c.useSSL {
		schema = "https"
	} else {
		schema = "http"
	}
	var url = fmt.Sprintf("%s://%s%s", schema, c.host,
		r.path)
	resp, err = c.httpRequest(r.method, url, r.params, r.header, r.body)
	log.LogInfof("resp %v,err %v", resp, err)
	if err != nil {
		log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	stateCode := resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	log.LogInfof("resp data:%s", string(respData))
	_ = resp.Body.Close()
	if err != nil {
		log.LogErrorf("serveRequest: read http response body fail: err(%v)", err)
		return
	}
	switch stateCode {
	case http.StatusOK:
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}

		if err := json.Unmarshal(respData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)

		}
		// o represent proto.ErrCodeSuccess
		if body.Code != 200 {
			return nil, proto.ParseErrorCode(body.Code)
		}
		return []byte(body.Data), nil
	default:
		log.LogErrorf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
			resp.Request.URL.String(), c.host, stateCode, strings.Replace(string(respData), "\n", "", -1))
	}
	return
}

func (c *MetaHttpClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	client.Timeout = requestTimeout
	var req *http.Request
	fullUrl := c.mergeRequestUrl(url, param)
	log.LogDebugf("httpRequest: merge request url: method(%v) url(%v) bodyLength[%v].", method, fullUrl, len(reqData))
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

func (c *MetaHttpClient) mergeRequestUrl(url string, params map[string]string) string {
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

func (mc *MetaHttpClient) GetMetaPartition(pid uint64) (cursor uint64, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[GetMetaPartition],pid:%v,err:%v", pid, err)
		}
		log.LogFlush()
	}()
	request := newAPIRequest(http.MethodGet, "/getPartitionById")
	request.params["pid"] = fmt.Sprintf("%v", pid)
	respData, err := mc.serveRequest(request)
	log.LogInfof("err:%v,respData:%v\n", err, string(respData))
	if err != nil {
		return
	}
	type RstData struct {
		Cursor uint64
	}
	body := &RstData{}
	if err = json.Unmarshal(respData, body); err != nil {
		return
	}
	return body.Cursor, nil
}

func (mc *MetaHttpClient) GetAllDentry(pid uint64) (dentryMap map[string]*metanode.Dentry, err error, ) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[GetAllDentry],pid:%v,err:%v", pid, err)
		}
		log.LogFlush()
	}()
	dentryMap = make(map[string]*metanode.Dentry, 0)
	request := newAPIRequest(http.MethodGet, "/getAllDentry")
	request.params["pid"] = fmt.Sprintf("%v", pid)
	respData, err := mc.serveRequest(request)
	log.LogInfof("err:%v,respData:%v\n", err, string(respData))
	if err != nil {
		return
	}

	dec := json.NewDecoder(bytes.NewBuffer(respData))
	dec.UseNumber()

	// It's the "items". We expect it to be an array
	if err = parseToken(dec, '['); err != nil {
		return
	}
	// Read items (large objects)
	for dec.More() {
		// Read next item (large object)
		lo := &metanode.Dentry{}
		if err = dec.Decode(lo); err != nil {
			return
		}
		dentryMap[fmt.Sprintf("%v_%v", lo.ParentId, lo.Name)] = lo
	}
	// Array closing delimiter
	if err = parseToken(dec, ']'); err != nil {
		return
	}
	return
}

func parseToken(dec *json.Decoder, expectToken rune) (err error) {
	t, err := dec.Token()
	if err != nil {
		return
	}
	if delim, ok := t.(json.Delim); !ok || delim != json.Delim(expectToken) {
		err = fmt.Errorf("expected token[%v],delim[%v],ok[%v]", string(expectToken), delim, ok)
		return
	}
	return
}

func (mc *MetaHttpClient) GetAllInodes(pid uint64) (rstMap map[uint64]*metanode.Inode, err error, ) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[GetAllInodes],pid:%v,err:%v", pid, err)
		}
		log.LogFlush()
	}()

	inodeMap := make(map[uint64]*metanode.Inode, 0)

	request := newAPIRequest(http.MethodGet, "/getAllInodes")
	request.params["pid"] = fmt.Sprintf("%v", pid)
	respData, err := mc.serveRequest(request)
	log.LogInfof("err:%v,respData:%v\n", err, string(respData))
	if err != nil {
		return
	}

	dec := json.NewDecoder(bytes.NewBuffer(respData))
	dec.UseNumber()

	// It's the "items". We expect it to be an array
	if err = parseToken(dec, '['); err != nil {
		return
	}
	// Read items (large objects)
	for dec.More() {
		// Read next item (large object)
		in := &metanode.Inode{}
		if err = dec.Decode(in); err != nil {
			return
		}
		inodeMap[in.Inode] = in
	}
	// Array closing delimiter
	if err = parseToken(dec, ']'); err != nil {
		return
	}

	return inodeMap, nil
}

func (mc *MetaHttpClient) GetMetaDataCrcSum(pid uint64) (result *proto.MetaDataCRCSumInfo, err error){
	req := newAPIRequest(http.MethodGet, "/getMetaDataCrcSum")
	req.params["pid"] = fmt.Sprintf("%v", pid)
	var data []byte
	data, err = mc.serveRequest(req)
	if err != nil {
		log.LogInfof("err:%v,respData:%v\n", err, string(data))
		return
	}
	result = new(proto.MetaDataCRCSumInfo)
	if err = json.Unmarshal(data, result); err != nil {
		err = fmt.Errorf("unmarshal failed:%v", err)
		return
	}
	return
}

func (mc *MetaHttpClient) GetInodesCrcSum(pid uint64) (result *proto.InodesCRCSumInfo, err error) {
	req := newAPIRequest(http.MethodGet, "/getInodesCrcSum")
	req.params["pid"] = fmt.Sprintf("%v", pid)
	var respData []byte
	respData, err = mc.serveRequest(req)
	if err != nil {
		log.LogErrorf("err:%v,respData:%v\n", err, string(respData))
		return
	}

	dec := json.NewDecoder(bytes.NewBuffer(respData))
	result = &proto.InodesCRCSumInfo{}
	if err = dec.Decode(result); err != nil {
		log.LogErrorf("decode failed:%v", err)
		return
	}
	return
}

func (mc *MetaHttpClient) GetDelInodesCrcSum(pid uint64) (result *proto.InodesCRCSumInfo, err error) {
	req := newAPIRequest(http.MethodGet, "/getDelInodesCrcSum")
	req.params["pid"] = fmt.Sprintf("%v", pid)
	var respData []byte
	respData, err = mc.serveRequest(req)
	if err != nil {
		log.LogErrorf("err:%v,respData:%v\n", err, string(respData))
		return
	}

	dec := json.NewDecoder(bytes.NewBuffer(respData))
	result = &proto.InodesCRCSumInfo{}
	if err = dec.Decode(result); err != nil {
		log.LogErrorf("decode failed:%v", err)
		return
	}
	return
}