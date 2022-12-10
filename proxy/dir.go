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
	"github.com/cubefs/cubefs/util/log"
)

type PathReq struct {
	Path string `json:"path"`
}

func (p *PathReq) IsValid() bool {
	return true
}

type MkdirReq struct {
	PathReq
	Mode uint32 `json:"mode"`
}

func (p *ProxyNode) mkdir(ctx *Context) {
	req := new(MkdirReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	log.LogDebugf("mkdir recive request info %v", req)

	fullPath := ctx.absPath(req.Path)
	err = ctx.vol().mkdir(fullPath, req.Mode)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, true)
	ctx.ResponseOk()
}

type ReadDirReq struct {
	PathReq
	From  string `json:"from"`
	Limit int    `json:"limit"`
}

func (p *ProxyNode) readDirLimit(ctx *Context) {
	req := new(ReadDirReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	log.LogDebugf("readDirLimit recive request info %v", req)
	dir := ctx.absPath(req.Path)
	infos, err := ctx.vol().readDirLimit(dir, req.From, req.Limit)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(infos)
}

func (p *ProxyNode) rmdir(ctx *Context) {
	req := new(PathReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	log.LogDebugf("rmdir recive request info %v", req)
	err = ctx.vol().rmdir(ctx.absPath(req.Path))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, true)
	ctx.ResponseOk()
}
