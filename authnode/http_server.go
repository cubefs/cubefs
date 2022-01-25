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

package authnode

import (
	"crypto/tls"
	"net/http"
	"net/http/httputil"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/log"
)

func (m *Server) startHTTPService() {
	go func() {
		m.handleFunctions()
		if m.cluster.PKIKey.EnableHTTPS {
			// not use PKI to verify client certificate
			// Instead, we use client secret key for authentication
			cfg := &tls.Config{
				//ClientAuth: tls.RequireAndVerifyClientCert,
				//ClientCAs:  caCertPool,
			}
			srv := &http.Server{
				Addr:      colonSplit + m.port,
				TLSConfig: cfg,
			}
			if err := srv.ListenAndServeTLS("/app/server.crt", "/app/server.key"); err != nil {
				log.LogErrorf("action[startHTTPService] failed,err[%v]", err)
				panic(err)
			}
		} else {
			if err := http.ListenAndServe(colonSplit+m.port, nil); err != nil {
				log.LogErrorf("action[startHTTPService] failed,err[%v]", err)
				panic(err)
			}
		}
	}()
	return
}

func (m *Server) newAuthProxy() *AuthProxy {
	var (
		authProxy AuthProxy
		err       error
	)
	if m.cluster.PKIKey.EnableHTTPS {
		if authProxy.client, err = cryptoutil.CreateClientX(&m.cluster.PKIKey.AuthRootPublicKey); err != nil {
			panic(err)
		}
	} else {
		authProxy.reverseProxy = &httputil.ReverseProxy{
			Director: func(request *http.Request) {
				request.URL.Scheme = "http"
				request.URL.Host = m.leaderInfo.addr
			}}
	}
	return &authProxy
}

func (m *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("URL[%v],remoteAddr[%v]", r.URL, r.RemoteAddr)
	switch r.URL.Path {
	case proto.ClientGetTicket:
		m.getTicket(w, r)
	case proto.AdminCreateKey:
		fallthrough
	case proto.AdminGetKey:
		fallthrough
	case proto.AdminDeleteKey:
		fallthrough
	case proto.AdminAddCaps:
		fallthrough
	case proto.AdminDeleteCaps:
		fallthrough
	case proto.AdminGetCaps:
		m.apiAccessEntry(w, r)
	case proto.AdminAddRaftNode:
		fallthrough
	case proto.AdminRemoveRaftNode:
		m.raftNodeOp(w, r)
	case proto.OSAddCaps:
		fallthrough
	case proto.OSDeleteCaps:
		fallthrough
	case proto.OSGetCaps:
		m.osCapsOp(w, r)
	default:
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "Invalid requst URL"})
	}
}

func (m *Server) handleFunctions() {
	http.HandleFunc(proto.ClientGetTicket, m.getTicket)
	http.Handle(proto.AdminCreateKey, m.handlerWithInterceptor())
	http.Handle(proto.AdminGetKey, m.handlerWithInterceptor())
	http.Handle(proto.AdminDeleteKey, m.handlerWithInterceptor())
	http.Handle(proto.AdminAddCaps, m.handlerWithInterceptor())
	http.Handle(proto.AdminDeleteCaps, m.handlerWithInterceptor())
	http.Handle(proto.AdminGetCaps, m.handlerWithInterceptor())
	http.Handle(proto.AdminAddRaftNode, m.handlerWithInterceptor())
	http.Handle(proto.AdminRemoveRaftNode, m.handlerWithInterceptor())
	http.Handle(proto.OSAddCaps, m.handlerWithInterceptor())
	http.Handle(proto.OSDeleteCaps, m.handlerWithInterceptor())
	http.Handle(proto.OSGetCaps, m.handlerWithInterceptor())
	return
}

func (m *Server) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.partition.IsRaftLeader() {
				if m.metaReady {
					m.ServeHTTP(w, r)
					return
				}
				log.LogWarnf("action[handlerWithInterceptor] leader meta has not ready")
				http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
				return
			}
			if m.leaderInfo.addr == "" {
				log.LogErrorf("action[handlerWithInterceptor] no leader,request[%v]", r.URL)
				http.Error(w, "no leader", http.StatusBadRequest)
				return
			}
			m.proxy(w, r)
		})
}

func (m *Server) proxy(w http.ResponseWriter, r *http.Request) {
	if m.cluster.PKIKey.EnableHTTPS {
		var (
			plaintext []byte
			err       error
			res       []byte
			jobj      proto.HTTPAuthReply
		)
		target := "https://" + m.leaderInfo.addr + r.URL.Path
		if plaintext, err = m.extractClientReqInfo(r); err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		res, err = proto.SendBytes(m.authProxy.client, target, plaintext)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthReqRedirectError, Msg: "[proxy] failed: " + err.Error()})
			return
		}
		if jobj, err = proto.ParseAuthReply(res); err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthReqRedirectError, Msg: "Target Server failed: " + err.Error()})
			return
		}
		sendOkReply(w, r, newSuccessHTTPAuthReply(jobj.Data))
	} else {
		m.authProxy.reverseProxy.ServeHTTP(w, r)
	}
}
