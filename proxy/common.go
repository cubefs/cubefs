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

type SymLinkReq struct {
	PathReq
	NewPath string `json:"newpath"`
}

func (p *ProxyNode) symLink(ctx *Context) {
	req := new(LinkReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("symbol link recive request info %v", req)
	err = ctx.vol().symlink(req.Path, ctx.absPath(req.NewPath))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}

type SetXattr struct {
	PathReq
	Flag uint8  `json:"flag"`
	Key  string `json:"name"`
	Data []byte `json:"data"`
}

func (s *SetXattr) IsValid() bool {
	return true
}

func (p *ProxyNode) setXattr(ctx *Context) {
	req := new(SetXattr)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("setXattr recive request info %v", req)
	err = ctx.vol().setXattr(ctx.absPath(req.Path), req.Key, req.Data)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}

type GetXattr struct {
	PathReq
	Name string `json:"name"`
}

func (p *ProxyNode) getXattr(ctx *Context) {
	req := new(GetXattr)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("getXattr recive request info %v", req)
	data, err := ctx.vol().getXattr(ctx.absPath(req.Path), req.Name)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("getXattr data info %v, val %s", data, string(data))
	ctx.ResponseOkWith(data)
}

type RemoveXattr struct {
	PathReq
	Name string `json:"name"`
}

func (p *ProxyNode) removeXattr(ctx *Context) {
	req := new(GetXattr)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("removeXattr recive request info %v", req)
	err = ctx.vol().removeXattr(ctx.absPath(req.Path), req.Name)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOk()
}

func (p *ProxyNode) listXattr(ctx *Context) {
	req := new(PathReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("listXattr xAttr req %v", req)
	keys, err := ctx.vol().listXattr(ctx.absPath(req.Path))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(keys)
}

type attrReq struct {
	PathReq
	Flag  uint8  `json:"flag"`
	Uid   uint32 `json:"uid"`
	Gid   uint32 `json:"gid"`
	Atime uint64 `json:"atime"`
	Mtime uint64 `json:"mtime"`
	Mode  uint32 `json:"mode"`
}

// TODO check whether time legal
func (a *attrReq) IsValid() bool {
	if a.Atime > 13569264000 || a.Mtime > 13569264000 {
		return false
	}
	return true
}

func (p *ProxyNode) setattr(ctx *Context) {
	req := new(attrReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("setattr recive request info %v", req)
	err = ctx.vol().setAttr(ctx.absPath(req.Path), req.Flag, req.Mode, req.Uid, req.Gid, int64(req.Atime), int64(req.Mtime))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}

type renameReq struct {
	PathReq
	Dest string `json:"destPath"`
}

func (r *renameReq) IsValid() bool {
	return true
}

func (p *ProxyNode) rename(ctx *Context) {
	req := new(renameReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("rename recive request info %v", req)
	err = ctx.vol().rename(ctx.absPath(req.Path), ctx.absPath(req.Dest))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	p.sendMsg(ctx, req.Dest, false)
	ctx.ResponseOk()
}

func (p *ProxyNode) stat(ctx *Context) {
	req := new(PathReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("stat recive request info %v", req)
	info, err := ctx.vol().stat(ctx.absPath(req.Path))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(info)
}

func (p *ProxyNode) statDir(ctx *Context) {
	req := new(PathReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("statDir recive request info %v", req)
	info, err := ctx.vol().statDir(ctx.absPath(req.Path))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(info)
}
