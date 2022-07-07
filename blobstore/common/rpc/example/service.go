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

package example

import (
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type metaValue struct {
	val int
}

func interceptorWholeRequest(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	c.Set("var", &metaValue{0})
	start := time.Now()
	c.Next()
	metaVal, _ := c.Get("var")
	val := metaVal.(*metaValue)
	span.Infof("Request %s: done Val=%d, spent %s", c.Request.URL.Path, val.val, time.Since(start).String())
}

func interceptorNothing1(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	metaVal, _ := c.Get("var")
	val := metaVal.(*metaValue)
	val.val += 10
	span.Infof("Request %s: interceptor nothing-1 Val=%d", c.Request.URL.Path, val.val)
}

func interceptorNothing2(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	metaVal, _ := c.Get("var")
	val := metaVal.(*metaValue)
	val.val++
	span.Debugf("Request %s: interceptor nothing-2 Val=%d", c.Request.URL.Path, val.val)
}

// NewAppHandler returns app server handler, use rpc.DefaultRouter
func NewAppHandler(cfg AppConfig) *rpc.Router {
	// must be register args with tag
	rpc.RegisterArgsParser(&ArgsUpload{}, "flag")
	rpc.RegisterArgsParser(&ArgsDelete{})
	rpc.RegisterArgsParser(&ArgsDownload{}, "form", "formx")
	rpc.RegisterArgsParser(&ArgsExist{})
	rpc.RegisterArgsParser(&ArgsURIOptional{}, "json")

	app := NewApp(cfg)

	rpc.Use(interceptorWholeRequest)                  // first interceptor
	rpc.Use(interceptorNothing1, interceptorNothing2) // second, then third

	// args key/val in c.Param
	rpc.PUT("/upload/:name/:size/:mode/:desc", app.Upload, rpc.OptArgsURI())
	// parse args in handler by c.ArgsBody
	rpc.POST("/update", app.Update)
	rpc.DELETE("/delete", app.Delete, rpc.OptArgsQuery())
	rpc.POST("/download", app.Download, rpc.OptArgsForm())
	rpc.GET("/stream", app.Stream)
	// param priority is uri > query > form > postfrom by default
	// but you can define you own rpc.Parser on args struct
	rpc.HEAD("/exist/:name", app.Exist, rpc.OptArgsURI(), rpc.OptArgsQuery())
	rpc.GET("/stat/:name", app.Stat, rpc.OptArgsURI(), rpc.OptArgsQuery())
	// initialized 8 room for key/val map of c.Meta
	rpc.GET("/list", app.List, rpc.OptMetaCapacity(8))

	// argument in uri with option
	rpc.GET("/args/:require/:option", app.OptionalArgs, rpc.OptArgsURI())
	// argument in uri without option
	rpc.GET("/args/:require", app.OptionalArgs, rpc.OptArgsURI())

	return rpc.DefaultRouter
}

// NewFileHandler returns file server handler
func NewFileHandler() *rpc.Router {
	rpc.RegisterArgsParser(&ArgsWrite{}, "flag")

	router := rpc.New()
	filer := NewFile()
	router.Handle(http.MethodPut, "/write/:size", filer.Write, rpc.OptArgsURI())
	router.Handle(http.MethodGet, "/read/:size", filer.Read, rpc.OptArgsURI())
	return router
}

// NewMetaHandler returns meta server handler
func NewMetaHandler() *rpc.Router {
	router := rpc.New()
	filer := NewFile()
	router.Handle(http.MethodPut, "/write/:size", filer.Write, rpc.OptArgsURI())
	router.Handle(http.MethodGet, "/read/:size", filer.Read, rpc.OptArgsURI())
	return router
}
