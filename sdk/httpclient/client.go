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
	"github.com/cubefs/cubefs/util/log"
)

const (
	requestTimeout = 30 * time.Second
)

var ErrNoAddress = errors.New("no address")

type Client struct {
	http.Client
	schema string
	addr   []string
}

func New() *Client {
	return &Client{schema: "http"}
}

func (c *Client) WithAddr(addresses ...string) *Client {
	c.addr = addresses[:]
	return c
}

func (c *Client) WithTimeout(timeout time.Duration) *Client {
	c.Client.Timeout = timeout
	return c
}

func (c *Client) WithSSL(ssl bool) *Client {
	if ssl {
		c.schema = "https"
	} else {
		c.schema = "http"
	}
	return c
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

		switch resp.StatusCode {
		case http.StatusOK:
			body := &struct {
				Code int32           `json:"code"`
				Msg  string          `json:"msg"`
				Data json.RawMessage `json:"data"`
			}{}
			if err = json.Unmarshal(respData, body); err != nil {
				log.LogErrorf("unmarshal response body %v", err)
				return nil, fmt.Errorf("unmarshal response body %v", err)
			}
			if body.Code != proto.ErrCodeSuccess {
				log.LogWarnf("serve: body code[%d] msg[%s] data[%v] ", body.Code, body.Msg, body.Data)
				if body.Code == proto.ErrCodeInternalError && len(body.Msg) > 0 {
					return nil, errors.New(body.Msg)
				}
				return nil, proto.ParseErrorCode(body.Code)
			}
			return []byte(body.Data), nil
		default:
			err = fmt.Errorf("serve: url(%s) status(%d) body(%s)",
				resp.Request.URL.String(), resp.StatusCode, string(respData))
			log.LogError(err)
			continue
		}
	}
	return
}

func (c *Client) serveWith(r *request, rst interface{}) error {
	buf, err := c.serve(r)
	if err != nil {
		return err
	}
	if rst == nil {
		return nil
	}
	return json.Unmarshal(buf, rst)
}
