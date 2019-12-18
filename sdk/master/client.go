// Copyright 2018 The Chubao Authors.
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

package master

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	requestTimeout = 30 * time.Second
)

var (
	ErrNoValidMaster = errors.New("no valid master")
)

type MasterClient struct {
	sync.RWMutex
	masters    []string
	useSSL     bool
	leaderAddr string
}

// AddNode add the given address as the master address.
func (c *MasterClient) AddNode(address string) {
	c.Lock()
	c.updateMaster(address)
	c.Unlock()
}

// Leader returns the current leader address.
func (c *MasterClient) Leader() (addr string) {
	c.RLock()
	addr = c.leaderAddr
	c.RUnlock()
	return
}

func (c *MasterClient) AdminAPI() *AdminAPI {
	return &AdminAPI{
		mc: c,
	}
}

func (c *MasterClient) ClientAPI() *ClientAPI {
	return &ClientAPI{
		mc: c,
	}
}

func (c *MasterClient) NodeAPI() *NodeAPI {
	return &NodeAPI{
		mc: c,
	}
}

// Change the leader address.
func (c *MasterClient) setLeader(addr string) {
	c.Lock()
	c.leaderAddr = addr
	c.Unlock()
}

func (c *MasterClient) serveRequest(r *request) (repsData []byte, err error) {
	leaderAddr, nodes := c.prepareRequest()
	host := leaderAddr
	for i := -1; i < len(nodes); i++ {
		if i == -1 {
			if host == "" {
				continue
			}
		} else {
			host = nodes[i]
		}
		var resp *http.Response
		var schema string
		if c.useSSL {
			schema = "https"
		} else {
			schema = "http"
		}
		var url = fmt.Sprintf("%s://%s%s", schema, host,
			r.path)
		resp, err = c.httpRequest(r.method, url, r.params, r.header, r.body)
		if err != nil {
			log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
			continue
		}
		stateCode := resp.StatusCode
		repsData, err = ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			log.LogErrorf("serveRequest: read http response body fail: err(%v)", err)
			continue
		}
		switch stateCode {
		case http.StatusForbidden:
			curMasterAddr := strings.TrimSpace(string(repsData))
			curMasterAddr = strings.Replace(curMasterAddr, "\n", "", -1)
			if len(curMasterAddr) == 0 {
				log.LogWarnf("serveRequest: server response status 403: request(%s) status"+
					"(403), body is empty", host)
				err = ErrNoValidMaster
				return
			}
			repsData, err = c.serveRequest(r)
			return
		case http.StatusOK:
			if leaderAddr != host {
				c.setLeader(host)
			}
			var body = &struct {
				Code int32           `json:"code"`
				Msg  string          `json:"msg"`
				Data json.RawMessage `json:"data"`
			}{}
			if err := json.Unmarshal(repsData, body); err != nil {
				return nil, fmt.Errorf("unmarshal response body err:%v", err)

			}
			// o represent proto.ErrCodeSuccess
			if body.Code != 0 {
				if body.Code == proto.ErrCodeInvalidTicket {
					return nil, proto.ErrInvalidTicket
				} else if body.Code == proto.ErrCodeExpiredTicket {
					return nil, proto.ErrExpiredTicket
				} else {
					return nil, fmt.Errorf("request error, code[%d], msg[%s]", body.Code, body.Msg)
				}
			}
			return []byte(body.Data), nil
		default:
			log.LogErrorf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%v).",
				resp.Request.URL.String(), host, stateCode, string(repsData))
			continue
		}
	}
	err = ErrNoValidMaster
	return
}

// Nodes returns all master addresses.
func (c *MasterClient) Nodes() (nodes []string) {
	c.RLock()
	nodes = c.masters
	c.RUnlock()
	return
}

// prepareRequest returns the leader address and all master addresses.
func (c *MasterClient) prepareRequest() (addr string, nodes []string) {
	c.RLock()
	addr = c.leaderAddr
	nodes = c.masters
	c.RUnlock()
	return
}

func (c *MasterClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := &http.Client{}
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

func (c *MasterClient) updateMaster(address string) {
	contains := false
	for _, master := range c.masters {
		if master == address {
			contains = true
			break
		}
	}
	if !contains {
		c.masters = append(c.masters, address)
	}
	c.leaderAddr = address
}

func (c *MasterClient) mergeRequestUrl(url string, params map[string]string) string {
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

// NewMasterHelper returns a new MasterHelper instance.
func NewMasterClient(masters []string, useSSL bool) *MasterClient {
	return &MasterClient{masters: masters, useSSL: useSSL}
}
