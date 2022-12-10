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
	"strconv"

	"github.com/cubefs/blobstore/util/log"
)

type PushI interface {
	Push(uid, appid string, idDir bool, dir string) error
}

func (p *ProxyNode) sendMsg(ctx *Context, dir string, isDir bool) {
	appId := strconv.Itoa(int(ctx.AppId()))
	api := p.sender[ctx.getTag()]
	if api != nil {
		api.Push(ctx.UserId(), appId, isDir, dir)
	}
}

type mockSendApi struct {
}

func (m *mockSendApi) Push(uid, appid string, isDir bool, dir string) error {
	log.Infof("send msg to uid %s, appId %s, isDir %v, dir %s", uid, appid, isDir, dir)
	return nil
}
