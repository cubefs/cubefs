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

package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/util/log"
)

type Context struct {
	Writer http.ResponseWriter
	Req    *http.Request
}

func (ctx *Context) Token() string {
	return ctx.Req.Header.Get(TokenKey)
}

func (ctx *Context) UserId() string {
	return ctx.Req.Header.Get(UserIdKey)
}

func (ctx *Context) ReqId() string {
	return ctx.Req.Header.Get(ReqIdKey)
}

func (ctx *Context) AppId() uint32 {
	appId, err := strconv.Atoi(ctx.Req.Header.Get(AppIdKey))
	if err != nil {
		return 0
	}

	return uint32(appId)
}

func (ctx *Context) Url() string {
	return ctx.Req.URL.Path
}

type baseReq interface {
	IsValid() bool
}

func (ctx *Context) ParseReq(args baseReq) (err error) {
	defer func() {
		if err != nil {
			ctx.ResponseErr(ARGS_ERR.With(err))
		}
	}()

	if args == nil {
		return nil
	}

	r := ctx.Req
	err = json.NewDecoder(r.Body).Decode(args)
	if err != nil {
		return err
	}

	if !args.IsValid() {
		err = fmt.Errorf("param is not illegal, param %v", args)
		return
	}

	return nil
}

func (ctx *Context) ResponseErr(err error) {
	httpErr, ok := err.(*Error)
	if !ok {
		reply := HttpReply{
			Code: UNKNOWN_ERR.StatusCode(),
			Msg:  err.Error(),
		}
		ctx.ResponseWithReply(reply, http.StatusInternalServerError)
		return
	}

	reply := HttpReply{
		Code: httpErr.ErrorCode(),
		Msg:  httpErr.Error(),
	}
	ctx.ResponseWithReply(reply, httpErr.StatusCode())
}

func (ctx *Context) ResponseOkWith(data interface{}) {
	reply := HttpReply{
		Code: 0,
		Data: data,
		Msg:  "success",
	}

	ctx.ResponseWithReply(reply, http.StatusOK)
}

func (ctx *Context) ResponseOk() {
	ctx.ResponseOkWith(nil)
}

func (ctx *Context) ResponseWithReply(reply HttpReply, status int) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("response failed, err %s", err.Error())
		}
	}()

	ctx.Writer.Header().Set("Content-Type", "application/json")
	ctx.Writer.WriteHeader(status)
	body, err := json.Marshal(reply)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	_, err = ctx.Writer.Write(body)
}
