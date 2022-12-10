// Copyright 2022 The CubeFS Authors.
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

package proxy

import (
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

type authApi interface {
	auth(ctx *Context) bool
}

type thirdAuth struct {
	addr      string
	accessKey string
}

func (t *thirdAuth) auth(ctx *Context) bool {
	authReq := map[string]string{}
	authReq["uid"] = ctx.UserId()
	authReq["token"] = ctx.Token()

	reply := &struct {
		Code int32  `json:"code"`
		Msg  string `json:"msg"`
	}{}

	header := map[string]string{}
	header[thirdAuthAccessKey] = t.accessKey
	header["X-ClientTag"] = "CFA"

	log.LogDebugf("start auth request, req %v, header %v", authReq, header)

	err := lib.DoHttpWithResp(t.addr, http.MethodPost, authReq, reply, header)
	if err != nil {
		ctx.ResponseErr(lib.AUTH_FAILED.With(err))
		log.LogErrorf("third auth by http req failed, addr %s, req %v, err %s", t.addr, authReq, err.Error())
		return false
	}

	if reply.Code != 0 {
		ctx.ResponseErr(lib.AUTH_FAILED.WithStr(fmt.Sprintf("code:%d,msg:%s", reply.Code, reply.Msg)))
		log.LogWarnf("third auth not allowed, addr %s, req %v, reply %v", t.addr, authReq, reply)
		return false
	}

	return true
}
