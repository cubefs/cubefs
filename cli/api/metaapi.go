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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
)

const (
	requestTimeout = 30 * time.Second
)

var getSpan = proto.SpanFromContext

type Inode struct {
	sync.RWMutex
	Inode      uint64 // Inode ID
	Type       uint32
	Uid        uint32
	Gid        uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte // SymLink target name
	NLink      uint32 // NodeLink counts
	Flag       int32
	Reserved   uint64 // reserved space
	Extents    []proto.ExtentKey
}

// String returns the string format of the inode.
func (i *Inode) String() string {
	i.RLock()
	defer i.RUnlock()
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString("Inode{")
	buff.WriteString(fmt.Sprintf("Inode[%d]", i.Inode))
	buff.WriteString(fmt.Sprintf("Type[%d]", i.Type))
	buff.WriteString(fmt.Sprintf("Uid[%d]", i.Uid))
	buff.WriteString(fmt.Sprintf("Gid[%d]", i.Gid))
	buff.WriteString(fmt.Sprintf("Size[%d]", i.Size))
	buff.WriteString(fmt.Sprintf("Gen[%d]", i.Generation))
	buff.WriteString(fmt.Sprintf("CT[%d]", i.CreateTime))
	buff.WriteString(fmt.Sprintf("AT[%d]", i.AccessTime))
	buff.WriteString(fmt.Sprintf("MT[%d]", i.ModifyTime))
	buff.WriteString(fmt.Sprintf("LinkT[%s]", i.LinkTarget))
	buff.WriteString(fmt.Sprintf("NLink[%d]", i.NLink))
	buff.WriteString(fmt.Sprintf("Flag[%d]", i.Flag))
	buff.WriteString(fmt.Sprintf("Reserved[%d]", i.Reserved))
	buff.WriteString(fmt.Sprintf("Extents[%s]", i.Extents))
	buff.WriteString("}")
	return buff.String()
}

type MetaHttpClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewMasterHelper returns a new MasterClient instance.
func NewMetaHttpClient(host string, useSSL bool) *MetaHttpClient {
	mc := &MetaHttpClient{host: host, useSSL: useSSL}
	return mc
}

type request struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func newAPIRequest(method string, path string) *request {
	return &request{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

type RespBody struct{}

func (c *MetaHttpClient) serveRequest(ctx context.Context, r *request) (respData []byte, err error) {
	span := getSpan(ctx)
	var resp *http.Response
	var schema string
	if c.useSSL {
		schema = "https"
	} else {
		schema = "http"
	}
	url := fmt.Sprintf("%s://%s%s", schema, c.host,
		r.path)
	resp, err = c.httpRequest(ctx, r.method, url, r.params, r.header, r.body)
	span.Infof("resp %v,err %v", resp, err)
	if err != nil {
		span.Errorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	stateCode := resp.StatusCode
	respData, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		span.Errorf("serveRequest: read http response body fail: err(%v)", err)
		return
	}
	switch stateCode {
	case http.StatusOK:
		body := new(proto.HTTPReplyRaw)
		if err := body.Unmarshal(respData); err != nil {
			return nil, err
		}
		// o represent proto.ErrCodeSuccess, TODO: 200 ???
		if body.Code != 200 {
			return nil, proto.ParseErrorCode(body.Code)
		}
		return body.Bytes(), nil
	default:
		span.Errorf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
			resp.Request.URL.String(), c.host, stateCode, strings.Replace(string(respData), "\n", "", -1))
	}
	return
}

func (c *MetaHttpClient) httpRequest(ctx context.Context,
	method, url string, param, header map[string]string, reqData []byte,
) (resp *http.Response, err error) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	client.Timeout = requestTimeout
	var req *http.Request
	fullUrl := c.mergeRequestUrl(url, param)
	getSpan(ctx).Debugf("httpRequest: merge request url: method(%v) url(%v) bodyLength[%v].", method, fullUrl, len(reqData))
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
	if len(params) > 0 {
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

func (mc *MetaHttpClient) GetMetaPartition(ctx context.Context, pid uint64) (cursor uint64, err error) {
	span := getSpan(ctx)
	defer func() {
		if err != nil {
			span.Errorf("action[GetMetaPartition],pid:%v,err:%v", pid, err)
		}
	}()
	request := newAPIRequest(http.MethodGet, "/getPartitionById")
	request.params["pid"] = fmt.Sprintf("%v", pid)
	respData, err := mc.serveRequest(ctx, request)
	span.Infof("err:%v,respData:%v\n", err, string(respData))
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

func (mc *MetaHttpClient) GetAllDentry(ctx context.Context, pid uint64) (dentryMap map[string]*metanode.Dentry, err error) {
	span := getSpan(ctx)
	defer func() {
		if err != nil {
			span.Errorf("action[GetAllDentry],pid:%v,err:%v", pid, err)
		}
	}()
	dentryMap = make(map[string]*metanode.Dentry)
	request := newAPIRequest(http.MethodGet, "/getAllDentry")
	request.params["pid"] = fmt.Sprintf("%v", pid)
	respData, err := mc.serveRequest(ctx, request)
	span.Infof("err:%v,respData:%v\n", err, string(respData))
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

func (mc *MetaHttpClient) GetAllInodes(ctx context.Context, pid uint64) (rstMap map[uint64]*Inode, err error) {
	span := getSpan(ctx)
	defer func() {
		if err != nil {
			span.Errorf("action[GetAllInodes],pid:%v,err:%v", pid, err)
		}
	}()
	reqURL := fmt.Sprintf("http://%v%v?pid=%v", mc.host, "/getAllInodes", pid)
	span.Debugf("reqURL=%v", reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	return unmarshalInodes(ctx, resp)
}

func unmarshalInodes(ctx context.Context, resp *http.Response) (rstMap map[uint64]*Inode, err error) {
	span := getSpan(ctx)
	bufReader := bufio.NewReader(resp.Body)
	rstMap = make(map[uint64]*Inode)
	var buf []byte
	for {
		buf, err = bufReader.ReadBytes('\n')
		span.Infof("buf[%v],err[%v]", string(buf), err)
		if err != nil && err != io.EOF {
			return
		}
		inode := &Inode{}
		if err1 := json.Unmarshal(buf, inode); err1 != nil {
			err = err1
			return
		}
		rstMap[inode.Inode] = inode
		span.Infof("after unmarshal current inode[%v]", inode)
		if err == io.EOF {
			err = nil
			return
		}
	}
}
