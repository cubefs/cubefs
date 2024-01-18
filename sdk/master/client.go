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
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"io/ioutil"
	"net/http"
	netUrl "net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"

	pb "github.com/gogo/protobuf/proto"
)

const (
	requestTimeout = 30 * time.Second
)

var (
	ErrNoValidMaster = errors.New("no valid master")
	UmpKeyErrNoSuchHost = "ErrNoSuchHost"
	UmpErrNoSuchHostAlarmMsg = "request to master with 'no such host'"
)

type ClientType int

const (
	MASTER ClientType = iota
	DATANODE
	METANODE
	ECNODE
)

type MasterClient struct {
	sync.RWMutex
	masters                []string
	useSSL                 bool
	timeout                time.Duration
	leaderAddr             string
	nodeAddr               string
	ClientType             ClientType
	DataNodeProfPort       uint16
	MetaNodeProfPort       uint16
	EcNodeProfPort         uint16
	FlashNodeProfPort      uint16
	masterDomain           string
	isMasterLBDomainClient bool

	adminAPI  *AdminAPI
	clientAPI *ClientAPI
	nodeAPI   *NodeAPI
	userAPI   *UserAPI
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
func (c *MasterClient) setLeader(addr string) {
	c.Lock()
	c.leaderAddr = addr
	c.Unlock()
}

func (c *MasterClient) parseJsonResp(r *request, respData []byte) (data []byte, code int32, msg string, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(respData, body); err != nil {
		err = fmt.Errorf("unmarshal json response body err:%v", err)
		return
	}
	return []byte(body.Data), body.Code, body.Msg, nil
}

func (c *MasterClient) parseProtobufResp(r *request, respData []byte) (data []byte, code int32, msg string, err error) {
	var body = &proto.HTTPReplyPb{}
	if err = pb.Unmarshal(respData, body); err != nil {
		err = fmt.Errorf("unmarshal protobuf response body err:%v", err)
		return
	}
	return body.Data, body.Code, body.Msg, nil
}

func (c *MasterClient) serveRequest(r *request) (respData []byte, contentType string, err error) {
	requestAddr, nodes := c.prepareRequest()
	host := requestAddr
	for i := -1; i < len(nodes); i++ {
		if i == -1 {
			if host == "" {
				continue
			}
		} else {
			host = nodes[i]
		}
		var (
			resp    *http.Response
			timeout bool
			schema  string
		)
		if c.useSSL {
			schema = "https"
		} else {
			schema = "http"
		}
		var url = fmt.Sprintf("%s://%s%s", schema, host,
			r.path)
		resp, err, timeout = c.httpRequest(r.method, url, r.params, r.header, r.body)
		if timeout {
			log.LogWarnf("serveRequest: send http request timeout: method(%v) url(%v) err(%v)", r.method, url, err)
			c.updateMasterAddr()
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), "no such host") {
				exporter.WarningAppendKey(UmpKeyErrNoSuchHost, UmpErrNoSuchHostAlarmMsg)
			}
			log.LogWarnf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
			c.updateMasterAddr()
			continue
		}
		stateCode := resp.StatusCode
		respData, err = ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			log.LogWarnf("serveRequest: read http response body fail: err(%v)", err)
			continue
		}
		switch stateCode {
		case http.StatusForbidden:
			curMasterAddr := strings.TrimSpace(string(respData))
			curMasterAddr = strings.Replace(curMasterAddr, "\n", "", -1)
			if len(curMasterAddr) == 0 {
				log.LogWarnf("serveRequest: server response status 403: request(%s) status"+
					"(403), body is empty", host)
				err = ErrNoValidMaster
				return
			}
			respData, contentType, err = c.serveRequest(r)
			return
		case http.StatusOK:
			if requestAddr != host {
				c.setLeader(host)
			}
			contentType = resp.Header.Get("content-type")
			if proto.IsDbBack && r.path != proto.AdminSetNodeInfo && r.path != proto.AdminGetLimitInfo {
				return respData, contentType, nil
			}

			var (
				code int32
				msg  string
			)
			if contentType == proto.ProtobufType {
				respData, code, msg, err = c.parseProtobufResp(r, respData)
			} else {
				respData, code, msg, err = c.parseJsonResp(r, respData)
			}

			switch c.ClientType {
			case MASTER:
				// 0 represent proto.ErrCodeSuccess
				if code != 0 {
					if r.path != proto.AdminGetVolMutex {
						log.LogErrorf("action failed, Code:%v, Msg:%v", code, msg)
					}
					if err = proto.ParseErrorCode(code); err == proto.ErrInternalError {
						err = fmt.Errorf("errcode:%v, msg:%v\n", err.Error(), msg)
					}
					return
				}
			case DATANODE, METANODE, ECNODE:
				// 200 represent proto.ErrCodeSuccess
				if code != 200 {
					log.LogErrorf("action failed, Code:%v, Msg:%v", code, msg)
					err = proto.ParseErrorCode(code)
					return
				}
			}
			return

		default:
			if proto.IsDbBack && stateCode == http.StatusBadRequest {
				return nil, "", fmt.Errorf(string(respData))
			}
			log.LogWarnf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
				resp.Request.URL.String(), host, stateCode, strings.Replace(string(respData), "\n", "", -1))
			err = fmt.Errorf("serveRequest: unknown status(%v) host(%v) uri(%v) body(%s), errMsg(%s)", stateCode, host, resp.Request.URL.String(),
				 strings.Replace(string(respData), "\n", "", -1), http.StatusText(stateCode))
			continue
		}
	}
	err = fmt.Errorf("send request err: clientType[%v], req addr[%v], req path[%v], err[%v]", c.ClientType, nodes, r.path, err)
	return
}

// Nodes returns all master addresses.
func (c *MasterClient) Nodes() (nodes []string) {
	c.RLock()
	nodes = c.masters
	c.RUnlock()
	return
}

func (c *MasterClient) IsOnline() bool {
    for _, master := range c.masters {
        if strings.Contains(master, ".jd.") {
            return true
        }
    }
    return false
}

func (c *MasterClient) SetMasterDomain(domain string) {
	c.Lock()
	defer c.Unlock()

	c.masterDomain = domain
}

func (c *MasterClient) updateMasterAddr() {
	if c.isMasterLBDomainClient || c.masterDomain == "" {
		return
	}
	addrs, err := iputil.LookupHost(c.masterDomain)
	if err != nil {
		log.LogErrorf("master direct domain resolution failed: %v", err)
		return
	}
	log.LogWarnf("old master addr: %v, new master addr: %v", c.masters, addrs)

	c.Lock()
	c.masters = addrs
	c.Unlock()
}

// prepareRequest returns the leader address and all master addresses.
func (c *MasterClient) prepareRequest() (addr string, nodes []string) {
	c.Lock()
	switch c.ClientType {
	case MASTER:
		addr = c.leaderAddr
		nodes = c.masters
	case DATANODE, METANODE, ECNODE:
		addr = c.nodeAddr
		nodes = []string{addr}
	}
	c.Unlock()
	return
}

func (c *MasterClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error, timeout bool) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	if header["isTimeOut"] != "" {
		var isTimeOut bool
		if isTimeOut, err = strconv.ParseBool(header["isTimeOut"]); err != nil {
			return
		}
		if isTimeOut {
			client.Timeout = c.timeout
		} else {
			client.Timeout = 0
		}
	} else {
		client.Timeout = c.timeout
	}
	var req *http.Request
	fullUrl := c.mergeRequestUrl(url, param)
	log.LogDebugf("httpRequest: merge request url: method(%v) url(%v) timeout(%v) header(%v) bodyLength[%v].", method, fullUrl, client.Timeout, header, len(reqData))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	if err != nil {
		timeout = err.(*netUrl.Error).Timeout()
	}
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

// NewMasterClient returns a new MasterClient instance.
func NewMasterClient(masters []string, useSSL bool) *MasterClient {
	var mc = &MasterClient{masters: masters, useSSL: useSSL, timeout: requestTimeout}
	mc.ClientType = MASTER
	mc.adminAPI = &AdminAPI{mc: mc}
	mc.clientAPI = &ClientAPI{mc: mc}
	mc.nodeAPI = &NodeAPI{mc: mc}
	mc.userAPI = &UserAPI{mc: mc}
	return mc
}

// NewMasterClientWithoutTimeout returns a new MasterClient instance without timeout.
func NewMasterClientWithoutTimeout(masters []string, useSSL bool) *MasterClient {
	mc := NewMasterClient(masters, useSSL)
	mc.timeout = time.Duration(0)
	return mc
}

// NewNodeClient returns a new MasterClient instance.
func NewNodeClient(node string, useSSL bool, clientType ClientType) *MasterClient {
	var mc = &MasterClient{nodeAddr: node, useSSL: useSSL, timeout: requestTimeout}
	mc.ClientType = clientType
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
	var masters = make([]string, 0)
	for _, master := range strings.Split(masterAddr, ",") {
		master = strings.TrimSpace(master)
		if master != "" {
			masters = append(masters, master)
		}
	}
	return NewMasterClient(masters, useSSL)
}

func NewMasterClientWithDomain(directDomain string, masters []string, useSSL bool) *MasterClient {
	var mc = &MasterClient{masters: masters, useSSL: useSSL, timeout: requestTimeout}
	mc.ClientType = MASTER
	mc.masterDomain = directDomain
	mc.adminAPI = &AdminAPI{mc: mc}
	mc.clientAPI = &ClientAPI{mc: mc}
	mc.nodeAPI = &NodeAPI{mc: mc}
	mc.userAPI = &UserAPI{mc: mc}
	return mc
}

func NewMasterClientWithLBDomain(domainName string, useSSL bool) *MasterClient {
	var masterAddrs []string
	if domainName != "" {
		masterAddrs = []string{domainName}
	}
	var mc = &MasterClient{masters: masterAddrs, useSSL: useSSL, timeout: requestTimeout}
	mc.ClientType = MASTER
	mc.isMasterLBDomainClient = true
	mc.adminAPI = &AdminAPI{mc: mc}
	mc.clientAPI = &ClientAPI{mc: mc}
	mc.nodeAPI = &NodeAPI{mc: mc}
	mc.userAPI = &UserAPI{mc: mc}
	return mc
}

func (mc *MasterClient) innerRegNodeWithNewInterface(authKeyPath, ip string, regInfo *RegNodeInfoReq) (rsp *proto.RegNodeRsp, err error) {
	var req          *request
	var data         []byte
	var localAuthKey string

	rsp = &proto.RegNodeRsp{}
	authFilePath := path.Join(authKeyPath, AuthFileName + regInfo.Role)
	if _, stErr := os.Stat(authFilePath); stErr != nil {
		//first start
		os.MkdirAll(authKeyPath, 0666)
		req, err = mc.NodeAPI().buildNewRegReq(regInfo, "", ip)
	} else {
		authKeyBuf, _ := os.ReadFile(authFilePath)
		localAuthKey = string(authKeyBuf)
		req, err = mc.NodeAPI().buildNewRegReq(regInfo, localAuthKey, ip)
	}

	if err != nil || req == nil {
		return
	}

	if data, _, err = mc.serveRequest(req); err != nil {
		return
	}


	err = json.Unmarshal(data, rsp)

	if rsp.AuthKey != "" && localAuthKey != "" && rsp.AuthKey != localAuthKey {
		err = fmt.Errorf("auth key check failed, cluster:%s, local:%s", rsp.AuthKey, localAuthKey)
		return nil, err
	}

	if err == nil && rsp.AuthKey != "" {
		_ = os.WriteFile(authFilePath, []byte(rsp.AuthKey), 0666)
	}
	return
}

func (mc *MasterClient) innerRegNodeWithOldInterface(authKeyPath, ip string, regInfo *RegNodeInfoReq) (rsp *proto.RegNodeRsp, err error) {
	var req          *request
	var data         []byte
	var localAuthKey string

	rsp = &proto.RegNodeRsp{}
	req, err = mc.NodeAPI().buildOldRegReq(regInfo, localAuthKey, ip)

	if err != nil || req == nil {
		return
	}

	if data, _, err = mc.serveRequest(req); err != nil {
		return
	}

	rsp.Id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (mc *MasterClient) RegNodeInfoWithAddr(authKeyPath, addr string, regInfo *RegNodeInfoReq)(rsp *proto.RegNodeRsp, err error) {
	var clusterInfo  *proto.ClusterInfo
	rsp = &proto.RegNodeRsp{}

	defer func() {
		if rsp != nil && rsp.Cluster == "" && clusterInfo != nil {
			rsp.Cluster = clusterInfo.Cluster
		}
	}()

	if regInfo == nil || regInfo.Role == "" || authKeyPath == ""{
		err = fmt.Errorf("invalid para, role or auth key path is nil")
		return
	}

	clusterInfo, err = mc.adminAPI.GetClusterInfo()
	if err != nil {
		log.LogErrorf("[RegNodeInfo] %s", err.Error())
		return
	}

	rsp, err = mc.innerRegNodeWithNewInterface(authKeyPath, addr, regInfo)
	if err != nil {
		if strings.Contains(err.Error(), http.StatusText(http.StatusNotFound)) {
			if rsp, err = mc.innerRegNodeWithOldInterface(authKeyPath, addr, regInfo); err == nil {
				//rsp.Cluster = clusterInfo.Cluster
				rsp.Addr = addr + ":" + regInfo.SrvPort
			}
			return
		}
	}
	return
}

func (mc *MasterClient) RegNodeInfo(authKeyPath string, regInfo *RegNodeInfoReq)(rsp *proto.RegNodeRsp, err error) {
	var clusterInfo  *proto.ClusterInfo
	rsp = &proto.RegNodeRsp{}

	defer func() {
		if rsp != nil && rsp.Cluster == "" && clusterInfo != nil {
			rsp.Cluster = clusterInfo.Cluster
		}
	}()

	if regInfo == nil || regInfo.Role == "" || authKeyPath == ""{
		err = fmt.Errorf("invalid para, role or auth key path is nil")
		return
	}

	clusterInfo, err = mc.adminAPI.GetClusterInfo()
	if err != nil {
		log.LogErrorf("[RegNodeInfo] %s", err.Error())
		return
	}

	rsp, err = mc.innerRegNodeWithNewInterface(authKeyPath, clusterInfo.Ip, regInfo)
	if err != nil {
		if strings.Contains(err.Error(), http.StatusText(http.StatusNotFound)) {
			if rsp, err = mc.innerRegNodeWithOldInterface(authKeyPath, clusterInfo.Ip, regInfo); err == nil {
				rsp.Cluster = clusterInfo.Cluster
				rsp.Addr = clusterInfo.Ip + ":" + regInfo.SrvPort
			}
			return
		}
	}
	return
}
