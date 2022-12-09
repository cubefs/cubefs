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

package route

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

type RouteNode struct {
	port    string
	mgoAddr string

	routeOp UserRouterApi
	volOp   VolOpApi
	wg      sync.WaitGroup

	lock sync.RWMutex

	hMap map[string]func(ctx *Context)
}

func NewServer() *RouteNode {
	return &RouteNode{}
}

func (p *RouteNode) Start(cfg *config.Config) (err error) {
	p.port = cfg.GetString("port")
	p.mgoAddr = cfg.GetString("mgoAddr")

	p.routeOp = &UserRouteImp{}
	p.volOp = &VolItemImp{}

	err = initDB(p.mgoAddr)
	if err != nil {
		log.LogErrorf("init mgo failed, addr %s, err %s", p.mgoAddr, err.Error())
		return
	}

	p.registerHandler()

	http.HandleFunc("/", p.serveHttp)

	var server = &http.Server{
		Addr: fmt.Sprintf(":%s", p.port),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogFatalf("start http failed, port (%d), err (%s)", p.port, err.Error())
		}
	}()

	p.wg.Add(1)

	return
}

func (p *RouteNode) registerHandler() {
	p.hMap = make(map[string]func(ctx *Context))
	// dir
	p.hMap["/addroute"] = p.addRoute
	p.hMap["/allocroute"] = p.allocRoute
	p.hMap["/getroute"] = p.getRoute

	p.hMap["/lockroute"] = p.lockRoute
	p.hMap["/unlockroute"] = p.unlockRoute

	p.hMap["/addvol"] = p.addVol
	p.hMap["/listVols"] = p.listVols
}

func (p *RouteNode) Shutdown() {
	p.wg.Done()
}

// Sync will block invoker goroutine until this ProxyNode shutdown.
func (p *RouteNode) Sync() {
	p.wg.Wait()
}

func (p *RouteNode) serveHttp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	prefix := "/router/api/v1"

	ctx := &Context{}
	ctx.Req = r
	ctx.Writer = w

	url := r.URL.Path
	urlKey := strings.TrimPrefix(url, prefix)

	if !strings.Contains(url, "/addvol") && !strings.Contains(url, "/listVols") {
		if ctx.UserId() == "" {
			ctx.ResponseErr(lib.ARGS_ERR.WithStr("user id can't be empty"))
			return
		}
	}

	log.LogDebugf("serveHttp url [%s], key[%s]", url, urlKey)
	if fn, hit := p.hMap[urlKey]; hit {
		fn(ctx)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}
