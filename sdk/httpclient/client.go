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

package httpclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/log"
)

const (
	get  = http.MethodGet
	post = http.MethodPost
)

var ErrNoAddress = errors.New("no address")

type Client struct {
	http.Client
	schema string
	addr   []string
	header map[string]string
}

func New() *Client {
	return &Client{schema: "http"}
}

func (c *Client) Addr(addresses ...string) *Client {
	c.addr = addresses[:]
	return c
}

func (c *Client) Timeout(timeout time.Duration) *Client {
	c.Client.Timeout = timeout
	return c
}

func (c *Client) SSL(ssl bool) *Client {
	c.schema = "http"
	if ssl {
		c.schema = "https"
	}
	return c
}

// WithHeader return a new Client.
func (c *Client) WithHeader(headers map[string]string, added ...string) *Client {
	if len(added)%2 == 1 {
		added = added[:len(added)-1]
	}
	nc := &Client{
		Client: c.Client,
		schema: c.schema,
		addr:   c.addr[:],
		header: make(map[string]string, len(c.header)),
	}
	for k, v := range c.header {
		nc.header[k] = v
	}
	for k, v := range headers {
		nc.header[k] = v
	}
	for idx := 0; idx < len(added); idx += 2 {
		nc.header[added[idx]] = added[idx+1]
	}
	return nc
}

func (c *Client) EncodingGzip() *Client {
	return c.WithHeader(nil, "x-cfs-Accept-Encoding", compressor.EncodingGzip)
}

func (c *Client) serve(r *request) (data []byte, err error) {
	err = ErrNoAddress
	for _, addr := range c.addr {
		url := fmt.Sprintf("%s://%s%s", c.schema, addr, r.url())

		var req *http.Request
		if req, err = http.NewRequest(r.method, url, bytes.NewReader(r.body)); err != nil {
			log.LogErrorf("serve: new request %s %s %v", r.method, url, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Connection", "close")
		for k, v := range c.header {
			req.Header.Set(k, v)
		}
		for k, v := range r.header {
			req.Header.Set(k, v)
		}

		var resp *http.Response
		if resp, err = c.Client.Do(req); err != nil {
			log.LogErrorf("serve: send request %s %s %v", r.method, url, err)
			continue
		}
		var respData []byte
		respData, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.LogErrorf("serve: read response body %v", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("serve: url(%s) status(%d) body(%s)",
				resp.Request.URL.String(), resp.StatusCode, string(respData))
			log.LogError(err)
			continue
		}

		respData, err = compressor.New(resp.Header.Get("x-cfs-Content-Encoding")).Decompress(respData)
		if err != nil {
			log.LogErrorf("decompress response body %v", err)
			continue
		}
		reply := new(proto.HTTPReplyRaw)
		if err = reply.Unmarshal(respData); err != nil {
			log.LogError(err)
			continue
		}
		if err = reply.Success(); err != nil {
			log.LogError(err)
			continue
		}
		return reply.Bytes(), nil
	}
	return
}

func (c *Client) serveWith(rst interface{}, r *request) error {
	buf, err := c.serve(r)
	if err != nil {
		return err
	}
	if rst == nil {
		return nil
	}
	return json.Unmarshal(buf, rst)
}
