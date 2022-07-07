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

package rpc

import (
	"net/http"
	"reflect"
	"runtime"

	"github.com/julienschmidt/httprouter"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

type (
	// Router router with interceptors
	// Interceptor is middleware for http serve but named `interceptor`.
	// Middleware within this Router called `interceptor`.
	//
	// headMiddlewares is middlewares run firstly.
	// Running order is:
	//     headMiddlewares --> middlewares --> interceptors --> handler
	//
	// example:
	// router := New()
	// router.Use(interceptor1, interceptor2)
	// router.Handle(http.MethodGet, "/get/:name", handlerGet)
	// router.Handle(http.MethodPut, "/put/:name", handlerPut)
	Router struct {
		Router          *httprouter.Router // router
		hasMiddleware   bool               // true if the router with Middleware*
		headMiddlewares []ProgressHandler  // middlewares run firstly of all
		headHandler     http.HandlerFunc   // run thie handler if has no middlewares
		interceptors    []HandlerFunc      // interceptors after middlewares
	}
)

// ServeHTTP makes the router implement the http.Handler interface.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !r.hasMiddleware {
		r.headHandler(w, req)
		return
	}
	r.Router.ServeHTTP(w, req)
}

// DefaultRouter default router for server
var DefaultRouter *Router

func init() {
	initDefaultRouter()
}

func initDefaultRouter() {
	DefaultRouter = New()
	DefaultRouter.Router.PanicHandler = defaultRecovery
}

// New alias of httprouter.New
// Return a Router, control by yourself
func New() *Router {
	r := &Router{
		Router:          httprouter.New(),
		hasMiddleware:   false,
		headMiddlewares: []ProgressHandler{&crcDecoder{}},
	}
	r.headHandler = buildHTTPHandler(r.Router.ServeHTTP, r.headMiddlewares...)
	return r
}

// Use attaches a global interceptor to the router.
// You should Use interceptor before register handler.
// It is sorted by registered order.
func (r *Router) Use(interceptors ...HandlerFunc) {
	if len(r.interceptors)+len(interceptors) >= int(abortIndex) {
		panic("too many regiter handlers")
	}
	r.interceptors = append(r.interceptors, interceptors...)
}

// Handle registers a new request handle with the given path and method.
//
// For HEAD, GET, POST, PUT, PATCH and DELETE requests the respective shortcut
// functions can be used.
func (r *Router) Handle(method, path string, handler HandlerFunc, opts ...ServerOption) {
	// Notice: in golang, sentence [ sliceA := append(sliceA, item) ]
	// the pointer of sliceA is the pointer of sliceB, if sliceB has enough capacity.
	// so we need make a new slice.
	handlers := make([]HandlerFunc, 0, len(r.interceptors)+1)
	handlers = append(handlers, r.interceptors...)
	handlers = append(handlers, handler)
	if len(handlers) >= int(abortIndex) {
		panic("too many regiter handlers")
	}
	r.Router.Handle(method, path, makeHandler(handlers, opts...))

	opt := new(serverOptions)
	for _, o := range opts {
		o.apply(opt)
	}
	icnames := make([]string, 0, len(r.interceptors))
	for _, ic := range r.interceptors {
		icnames = append(icnames, runtime.FuncForPC(reflect.ValueOf(ic).Pointer()).Name())
	}
	name := runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	log.Infof("register handler method:%s, path:%s, interceptors:%s, handler:%s, opts:%+v",
		method, path, icnames, name, opt)
}

// Use attaches a global interceptor to the default router.
// You should Use interceptor before register handler.
// It is sorted by registered order.
func Use(interceptors ...HandlerFunc) {
	DefaultRouter.interceptors = append(DefaultRouter.interceptors, interceptors...)
}

// HEAD is a shortcut for Handle(http.MethodHead, path, handle)
func HEAD(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodHead, path, handler, opts...)
}

// GET is a shortcut for Handle(http.MethodGet, path, handle)
func GET(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodGet, path, handler, opts...)
}

// POST is a shortcut for Handle(http.MethodPost, path, handle)
func POST(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodPost, path, handler, opts...)
}

// PUT is a shortcut for Handle(http.MethodPut, path, handle)
func PUT(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodPut, path, handler, opts...)
}

// DELETE is a shortcut for Handle(http.MethodDelete, path, handle)
func DELETE(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodDelete, path, handler, opts...)
}

// OPTIONS is a shortcut for Handle(http.MethodOptions, path, handle)
func OPTIONS(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodOptions, path, handler, opts...)
}

// PATCH is a shortcut for Handle(http.MethodPatch, path, handle)
func PATCH(path string, handler HandlerFunc, opts ...ServerOption) {
	Handle(http.MethodPatch, path, handler, opts...)
}

// Handle registers a new request handle with the given path and method.
//
// For HEAD, GET, POST, PUT, PATCH and DELETE requests the respective shortcut
// functions can be used.
func Handle(method, path string, handler HandlerFunc, opts ...ServerOption) {
	DefaultRouter.Handle(method, path, handler, opts...)
}
