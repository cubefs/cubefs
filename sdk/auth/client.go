// Copyright 2018 The Cubefs Authors.
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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/keystore"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	requestTimeout       = 30 * time.Second
	RequestMaxRetry      = 5
	RequestSleepInterval = 100 * time.Millisecond
)

type AuthClient struct {
	sync.RWMutex
	authnodes   []string
	enableHTTPS bool
	certFile    string
	ticket      *auth.Ticket
	leaderAddr  string
}

func (c *AuthClient) API() *API {
	return &API{
		ac: c,
	}
}

func NewAuthClient(authNodes []string, enableHTTPS bool, certFile string) *AuthClient {
	return &AuthClient{authnodes: authNodes, enableHTTPS: enableHTTPS, certFile: certFile}
}

func (c *AuthClient) request(clientID, clientKey string, key []byte, data interface{}, path, serviceID string) (respData []byte, err error) {
	var (
		body     []byte
		urlProto string
		url      string
		client   *http.Client
		certFile []byte
	)
	if c.enableHTTPS {
		urlProto = "https://"
		if certFile, err = loadCertfile(c.certFile); err != nil {
			log.LogWarnf("load cert file failed: %v", err)
			return
		}
		client, err = cryptoutil.CreateClientX(&certFile)
		if err != nil {
			return
		}
	} else {
		urlProto = "http://"
		client = &http.Client{}
	}
	//TODO don't retry if the param is wrong
	for i := 0; i < RequestMaxRetry; i++ {
		for _, ip := range c.authnodes {
			url = urlProto + ip + path
			body, err = proto.SendData(client, url, data)
			if err != nil {
				continue
			}
			var jobj *proto.HTTPAuthReply
			if err = json.Unmarshal(body, &jobj); err != nil {
				return nil, fmt.Errorf("unmarshal response body err:%v", err)
			}
			if jobj.Code != 0 {
				if jobj.Code == proto.ErrCodeExpiredTicket {
					c.ticket, err = c.API().GetTicket(clientID, clientKey, serviceID)
					if err == nil {
						c.request(clientID, clientKey, key, data, path, serviceID)
					}
				}
				err = fmt.Errorf(jobj.Msg)
				return nil, fmt.Errorf("request error, code[%d], msg[%s]", jobj.Code, err)
			}
			data := fmt.Sprint(jobj.Data)
			if respData, err = cryptoutil.DecodeMessage(data, key); err != nil {
				return nil, fmt.Errorf("decode message error: %v", err)
			}
			return
		}
		log.LogWarnf("Request authnode: getReply error and will RETRY, url(%v) err(%v)", url, err)
		time.Sleep(RequestSleepInterval)
	}
	log.LogWarnf("Request authnode exit: send to addr(%v) err(%v)", url, err)
	return nil, fmt.Errorf("Request authnode: getReply error, url(%v) err(%v)", url, err)
}

func (c *AuthClient) serveOSSRequest(id, key string, ticket *auth.Ticket, akCaps *keystore.AccessKeyCaps, reqType proto.MsgType, reqPath string) (caps *keystore.AccessKeyCaps, err error) {
	var (
		sessionKey []byte
		ts         int64
		resp       proto.AuthOSAccessKeyResp
		respData   []byte
	)
	apiReq := &proto.APIAccessReq{
		Type:      reqType,
		ClientID:  id,
		ServiceID: proto.AuthServiceID,
		Ticket:    ticket.Ticket,
	}
	if sessionKey, err = cryptoutil.Base64Decode(ticket.SessionKey); err != nil {
		return nil, err
	}
	if apiReq.Verifier, ts, err = cryptoutil.GenVerifier(sessionKey); err != nil {
		return nil, err
	}
	message := &proto.AuthOSAccessKeyReq{
		APIReq: *apiReq,
		AKCaps: *akCaps,
	}
	if respData, err = c.request(id, key, sessionKey, message, reqPath, proto.AuthServiceID); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &resp); err != nil {
		return
	}
	if err = proto.VerifyAPIRespComm(&resp.APIResp, reqType, id, proto.AuthServiceID, ts); err != nil {
		return
	}
	return &resp.AKCaps, err
}

func (c *AuthClient) serveAdminRequest(id, key string, ticket *auth.Ticket, keyInfo *keystore.KeyInfo, reqType proto.MsgType, reqPath string) (res *keystore.KeyInfo, err error) {
	var (
		sessionKey []byte
		ts         int64
		resp       proto.AuthAPIAccessResp
		respData   []byte
	)
	apiReq := &proto.APIAccessReq{
		Type:      reqType,
		ClientID:  id,
		ServiceID: proto.AuthServiceID,
		Ticket:    ticket.Ticket,
	}
	if sessionKey, err = cryptoutil.Base64Decode(ticket.SessionKey); err != nil {
		return nil, err
	}
	if apiReq.Verifier, ts, err = cryptoutil.GenVerifier(sessionKey); err != nil {
		return nil, err
	}
	message := &proto.AuthAPIAccessReq{
		APIReq:  *apiReq,
		KeyInfo: *keyInfo,
	}
	if respData, err = c.request(id, key, sessionKey, message, reqPath, proto.AuthServiceID); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &resp); err != nil {
		return
	}
	if err = proto.VerifyAPIRespComm(&resp.APIResp, reqType, id, proto.AuthServiceID, ts); err != nil {
		return
	}
	return &resp.KeyInfo, err
}

func loadCertfile(path string) (caCert []byte, err error) {
	caCert, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}
	return
}
