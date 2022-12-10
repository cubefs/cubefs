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
	"path"

	"github.com/cubefs/cubefs/util/lib"
)

type Context struct {
	lib.Context

	Req *http.Request

	Vol      *Volume
	basePath string
}

func (ctx *Context) absPath(dir string) string {
	return path.Clean(fmt.Sprintf("%s/%s", ctx.basePath, dir))
}

func (ctx *Context) vol() *Volume {
	return ctx.Vol
}

func (ctx *Context) getTag() string {
	return ctx.Req.Header.Get(clientTag)
}

func (ctx *Context) UserId() string {
	return ctx.Context.UserId()
}

func (ctx *Context) DevId() string {
	return ctx.Req.Header.Get(deviceId)
}

func (ctx *Context) getUrl() string {
	return ctx.Req.URL.Path
}
