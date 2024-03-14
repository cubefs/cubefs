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

package master

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/log"
)

// TODO: re-use response body.

const (
	requestTimeout = 30 * time.Second

	encodingGzip          = compressor.EncodingGzip
	headerAcceptEncoding  = proto.HeaderAcceptEncoding
	headerContentEncoding = proto.HeaderContentEncoding

	get  = http.MethodGet
	post = http.MethodPost
)

var ErrNoValidMaster = errors.New("no valid master")

type MasterCLientWithResolver struct {
	MasterClient
	resolver       *NameResolver
	updateInverval int
	stopC          chan struct{}
}

type MasterClient struct {
	sync.RWMutex
	masters     []string
	useSSL      bool
	leaderAddr  string
	timeout     time.Duration
	clientIDKey string

	adminAPI  *AdminAPI
	clientAPI *ClientAPI
	nodeAPI   *NodeAPI
	userAPI   *UserAPI
}

func (c *MasterClient) ReplaceMasterAddresses(addrs []string) {
	c.Lock()
	defer c.Unlock()
	c.masters = addrs
	c.leaderAddr = ""
}

func (c *MasterClient) GetMasterAddresses() (addrs []string) {
	c.Lock()
	defer c.Unlock()
	addrs = make([]string, len(c.masters))
	copy(addrs, c.masters)
	return
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

func (c *MasterClient) ClientIDKey() string {
	return c.clientIDKey
}

func (c *MasterClient) AdminAPI() *AdminAPI {
	return c.adminAPI
}

func (c *MasterClient) ClientAPI() *ClientAPI {
	return c.clientAPI
}

func (c *MasterClient) NodeAPI() *NodeAPI {
	return c.nodeAPI
}

func (c *MasterClient) UserAPI() *UserAPI {
	return c.userAPI
}

// Change the leader address.
func (c *MasterClient) SetLeader(addr string) {
	c.Lock()
	c.leaderAddr = addr
	c.Unlock()
}

// Change the request timeout
func (c *MasterClient) SetTimeout(timeout uint16) {
	c.Lock()
	c.timeout = time.Duration(timeout) * time.Second
	c.Unlock()
}

func (c *MasterClient) SetClientIDKey(clientIDKey string) {
	c.Lock()
	c.clientIDKey = clientIDKey
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
		schema := "http"
		if c.useSSL {
			schema = "https"
		}
		url := fmt.Sprintf("%s://%s%s", schema, host, r.path)
		resp, err = c.httpRequest(r.method, url, r)
		if err != nil {
			log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
			continue
		}
		stateCode := resp.StatusCode
		repsData, err = io.ReadAll(resp.Body)
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
				log.LogDebugf("server Request resp new master[%v] old [%v]", host, leaderAddr)
				c.SetLeader(host)
			}
			repsData, err = compressor.New(resp.Header.Get(headerContentEncoding)).Decompress(repsData)
			if err != nil {
				log.LogErrorf("serveRequest: decompress response body fail: err(%v)", err)
				return nil, fmt.Errorf("decompress response body err:%v", err)
			}
			body := new(proto.HTTPReplyRaw)
			if err := body.Unmarshal(repsData); err != nil {
				log.LogErrorf("unmarshal response body err:%v", err)
				return nil, fmt.Errorf("unmarshal response body err:%v", err)

			}
			if body.Code != proto.ErrCodeSuccess {
				log.LogWarnf("serveRequest: code[%v], msg[%v], data[%v] ", body.Code, body.Msg, body.Data)
				return []byte(body.Data), errors.New(body.Msg)
			}
			return body.Bytes(), nil
		default:
			msg := fmt.Sprintf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
				resp.Request.URL.String(), host, stateCode, strings.Replace(string(repsData), "\n", "", -1))
			err = errors.New(msg)
			log.LogErrorf(msg)
			continue
		}
	}
	return
}

func (c *MasterClient) requestWith(rst interface{}, r *request) error {
	if r.err != nil {
		return r.err
	}
	buf, err := c.serveRequest(r)
	if err != nil {
		return err
	}
	if rst == nil {
		return nil
	}
	return json.Unmarshal(buf, rst)
}

// result is nil
func (c *MasterClient) request(r *request) error {
	return c.requestWith(nil, r)
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

func (c *MasterClient) httpRequest(method, url string, r *request) (resp *http.Response, err error) {
	client := http.DefaultClient
	if !r.noTimeout {
		client.Timeout = c.timeout
	}
	reader := bytes.NewReader(r.body)
	var req *http.Request
	fullUrl := c.mergeRequestUrl(url, r.params)
	log.LogDebugf("httpRequest: method(%v) url(%v) bodyLength[%v].", method, fullUrl, len(r.body))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range r.header {
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

func NewMasterCLientWithResolver(masters []string, useSSL bool, updateInverval int) *MasterCLientWithResolver {
	mc := &MasterCLientWithResolver{
		MasterClient:   MasterClient{masters: masters, useSSL: useSSL, timeout: requestTimeout},
		updateInverval: updateInverval,
		stopC:          make(chan struct{}),
	}
	mc.adminAPI = &AdminAPI{mc: &mc.MasterClient}
	mc.clientAPI = &ClientAPI{mc: &mc.MasterClient}
	mc.nodeAPI = &NodeAPI{mc: &mc.MasterClient}
	mc.userAPI = &UserAPI{mc: &mc.MasterClient}
	resolver, err := NewNameResolver(masters)
	if err != nil {
		return nil
	} else {
		mc.resolver = resolver
	}
	return mc
}

func (mc *MasterCLientWithResolver) Start() (err error) {
	failed := true
	for i := 0; i < 3; i++ {
		var changed bool
		changed, err = mc.resolver.Resolve()
		if changed && err == nil {
			var addrs []string
			addrs, err = mc.resolver.GetAllAddresses()
			if err == nil {
				mc.ReplaceMasterAddresses(addrs)
				failed = false
				break
			} else {
				log.LogWarnf("MasterCLientWithResolver: Resolve failed: %v, retry %v", err, i)
			}
		}
	}

	if failed {
		err = errors.New("MasterCLientWithResolver: Resolve failed")
		log.LogErrorf("MasterCLientWithResolver: Resolve failed")
		return
	}

	if len(mc.resolver.domains) == 0 {
		log.LogDebugf("MasterCLientWithResolver: No domains found, skipping resolving timely")
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(mc.updateInverval) * time.Minute)
		// timer := time.NewTimer(0)
		defer ticker.Stop()
		for {
			select {
			case <-mc.stopC:
				log.LogInfo("MasterCLientWithResolver goroutine stopped")
				return
			case <-ticker.C:
				changed, err := mc.resolver.Resolve()
				if changed && err == nil {
					addrs, err := mc.resolver.GetAllAddresses()
					if err == nil {
						mc.ReplaceMasterAddresses(addrs)
					}

				}
				// timer.Reset(time.Duration(mc.updateInverval) * time.Minute)
			}
		}
	}()
	return nil
}

func (mc *MasterCLientWithResolver) Stop() {
	select {
	case mc.stopC <- struct{}{}:
		log.LogDebugf("stop resolver, notified!")
	default:
		log.LogDebugf("stop resolver, skipping notify!")
	}
}

// NewMasterHelper returns a new MasterClient instance.
func NewMasterClient(masters []string, useSSL bool) *MasterClient {
	mc := &MasterClient{masters: masters, useSSL: useSSL, timeout: requestTimeout}
	mc.adminAPI = &AdminAPI{mc: mc}
	mc.clientAPI = &ClientAPI{mc: mc}
	mc.nodeAPI = &NodeAPI{mc: mc}
	mc.userAPI = &UserAPI{mc: mc}
	return mc
}

// NewMasterClientFromString parse raw master address configuration
// string and returns a new MasterClient instance.
// Notes that a valid format raw string must match: "{HOST}:{PORT},{HOST}:{PORT}"
func NewMasterClientFromString(masterAddr string, useSSL bool) *MasterClient {
	masters := make([]string, 0)
	for _, master := range strings.Split(masterAddr, ",") {
		master = strings.TrimSpace(master)
		if master != "" {
			masters = append(masters, master)
		}
	}
	return NewMasterClient(masters, useSSL)
}
