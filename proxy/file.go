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

type OpenReq struct {
	PathReq
	Flag uint8  `json:"openflag"`
	Mode uint32 `json:"mode"`
}

func (p *ProxyNode) open(ctx *Context) {
	req := new(OpenReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("open recive request info %v", req)
	ino, err := ctx.vol().Open(ctx.absPath(req.Path), req.Mode, req.Flag)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, true)
	ctx.ResponseOkWith(ino)
}

type WriteReq struct {
	PathReq
	Ino    uint64 `json:"id"`
	Offset int    `json:"off"`
	Data   []byte `json:"data"`
}

func (w *WriteReq) IsValid() bool {
	return w.Ino != 0
}

func (p *ProxyNode) writeFile(ctx *Context) {
	req := new(WriteReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("writeFile recive request info, path %s, ino %d, offset %d, size %d", req.Path, req.Ino, req.Offset, len(req.Data))
	size, err := ctx.vol().writeFile(req.Ino, req.Offset, req.Data)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(size)
}

type ReadReq struct {
	PathReq
	Ino    uint64 `json:"id"`
	Offset int    `json:"off"`
	Len    int    `json:"len"`
}

func (p *ProxyNode) readFile(ctx *Context) {
	req := new(ReadReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("read recive request info %v", req)
	data, err := ctx.vol().readFile(req.Ino, req.Offset, req.Len)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOkWith(data)
}

type SyncReq struct {
	PathReq
	Ino uint64 `json:"id"`
}

func (p *ProxyNode) fsync(ctx *Context) {
	req := new(SyncReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("read recive request info %v", req)
	err = ctx.vol().fsync(req.Ino)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOk()
}

type TruncReq struct {
	PathReq
	Off int `json:"offset"`
}

func (p *ProxyNode) truncate(ctx *Context) {
	req := new(TruncReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("truncate recive request info %v", req)
	err = ctx.vol().truncate(ctx.absPath(req.Path), req.Off)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}

func (p *ProxyNode) unlink(ctx *Context) {
	req := new(PathReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("unlink recive request info %v", req)
	err = ctx.vol().unlink(ctx.absPath(req.Path))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}

type LinkReq struct {
	PathReq
	NewPath string `json:"newpath"`
}

func (p *ProxyNode) link(ctx *Context) {
	req := new(LinkReq)
	err := ctx.ParseReq(req)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("unlink recive request info %v", req)
	err = ctx.vol().link(ctx.absPath(req.Path), ctx.absPath(req.NewPath))
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	p.sendMsg(ctx, req.Path, false)
	ctx.ResponseOk()
}
