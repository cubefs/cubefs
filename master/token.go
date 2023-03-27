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
	"encoding/base64"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type TokenValue struct {
	VolName   string
	Value     string
	TokenType int8
}

func newTokenValue(token *proto.Token) (tv *TokenValue) {
	tv = &TokenValue{
		TokenType: token.TokenType,
		Value:     token.Value,
		VolName:   token.VolName,
	}
	return
}

func createToken(volName string, tokenType int8) (token *proto.Token, err error) {
	str := fmt.Sprintf("%v_%v_%v", volName, tokenType, time.Now().UnixNano())
	encodeStr := base64.StdEncoding.EncodeToString([]byte(str))
	token = &proto.Token{
		TokenType: tokenType,
		VolName:   volName,
		Value:     encodeStr,
	}
	return

}

func (c *Cluster) createToken(vol *Vol, tokenType int8) (err error) {
	token, err := createToken(vol.Name, tokenType)
	if err != nil {
		return
	}
	if err = c.syncAddToken(token); err != nil {
		return
	}
	vol.putToken(token)
	return
}

func (c *Cluster) deleteToken(vol *Vol, token, authKey string) (err error) {
	var serverAuthKey string
	if vol.Owner != "" {
		serverAuthKey = vol.Owner
	} else {
		serverAuthKey = vol.Name
	}
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}
	var tokenObj *proto.Token
	if tokenObj, err = vol.getToken(token); err != nil {
		return
	}
	if err = c.syncDeleteToken(tokenObj); err != nil {
		return
	}
	vol.deleteToken(token)
	return
}

func (c *Cluster) addToken(vol *Vol, tokenType int8, authKey string) (err error) {
	var serverAuthKey string
	if vol.Owner != "" {
		serverAuthKey = vol.Owner
	} else {
		serverAuthKey = vol.Name
	}
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}
	return c.createToken(vol, tokenType)
}

func (c *Cluster) updateToken(vol *Vol, tokenType int8, token, authKey string) (err error) {
	var serverAuthKey string
	if vol.Owner != "" {
		serverAuthKey = vol.Owner
	} else {
		serverAuthKey = vol.Name
	}
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}
	var tokenObj *proto.Token
	if tokenObj, err = vol.getToken(token); err != nil {
		return
	}
	oldTokenType := tokenObj.TokenType
	tokenObj.TokenType = tokenType
	if err = c.syncUpdateToken(tokenObj); err != nil {
		tokenObj.TokenType = oldTokenType
		return
	}
	vol.putToken(tokenObj)
	return
}

func (m *Server) addToken(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		name      string
		tokenType int8
		vol       *Vol
		msg       string
		authKey   string
	)
	metrics := exporter.NewTPCnt(proto.TokenAddURIUmpKey)
	defer func() { metrics.Set(err) }()
	if name, tokenType, authKey, err = parseAddTokenPara(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if err = m.cluster.addToken(vol, tokenType, authKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("add tokenType[%v] of vol [%v] successed,from[%v]", tokenType, name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
	return
}

func (m *Server) updateToken(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		name      string
		tokenType int8
		vol       *Vol
		msg       string
		authKey   string
		token     string
	)
	metrics := exporter.NewTPCnt(proto.TokenUpdateURIUmpKey)
	defer func() { metrics.Set(err) }()
	if name, tokenType, token, authKey, err = parseUpdateTokenPara(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if err = m.cluster.updateToken(vol, tokenType, token, authKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update tokenType[%v] of vol [%v] successed,from[%v]", tokenType, name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
	return
}

func (m *Server) deleteToken(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		name    string
		token   string
		vol     *Vol
		msg     string
		authKey string
	)
	metrics := exporter.NewTPCnt(proto.TokenDelURIUmpKey)
	defer func() { metrics.Set(err) }()
	if name, token, authKey, err = parseDeleteTokenPara(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if err = m.cluster.deleteToken(vol, token, authKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("delete token[%v] of vol [%v] successed,from[%v]", token, name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
	return
}

func (m *Server) getToken(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		name     string
		token    string
		tokenObj *proto.Token
		vol      *Vol
	)
	metrics := exporter.NewTPCnt(proto.TokenGetURIUmpKey)
	defer func() { metrics.Set(err) }()
	if name, token, err = parseGetTokenPara(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if tokenObj, err = vol.getToken(token); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(tokenObj))
	return
}

func parseAddTokenPara(r *http.Request) (name string, tokenType int8, authKey string, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
		return
	}
	if tokenType, err = extractTokenType(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return
}

func parseUpdateTokenPara(r *http.Request) (name string, tokenType int8, token, authKey string, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
		return
	}
	if tokenType, err = extractTokenType(r); err != nil {
		return
	}
	if token, err = extractTokenValue(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return
}

func parseDeleteTokenPara(r *http.Request) (name string, token, authKey string, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
		return
	}
	if token, err = extractTokenValue(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return
}

func extractTokenType(r *http.Request) (tokenType int8, err error) {
	var (
		tokenTypeStr string
		iTokenType   int
	)
	if tokenTypeStr = r.FormValue(tokenTypeKey); tokenTypeStr == "" {
		err = keyNotFound(tokenTypeKey)
		return
	}
	if iTokenType, err = strconv.Atoi(tokenTypeStr); err != nil {
		return
	}

	if !(iTokenType == proto.ReadWriteToken || iTokenType == proto.ReadOnlyToken) {
		err = fmt.Errorf("token type must be 1(readOnly) or 2(readWrite)")
		return
	}
	tokenType = int8(iTokenType)
	return
}

func parseGetTokenPara(r *http.Request) (name string, token string, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
		return
	}
	if token, err = extractTokenValue(r); err != nil {
		return
	}
	return
}

func extractTokenValue(r *http.Request) (token string, err error) {
	if token = r.FormValue(tokenKey); token == "" {
		err = keyNotFound(tokenKey)
		return
	}
	token, err = url.QueryUnescape(token)
	return
}
