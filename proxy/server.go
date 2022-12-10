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
	"strconv"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/route"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

type ProxyNode struct {
	port string

	routeKey string

	masterAddr string

	mc *master.MasterClient

	volRW  sync.RWMutex
	volMap map[string]*Volume

	wg sync.WaitGroup

	hMap map[string]func(ctx *Context)

	sender map[string]PushI
	ru     routerApi
	auth   authApi
}

func NewServer() *ProxyNode {
	return &ProxyNode{}
}

func (p *ProxyNode) Start(cfg *config.Config) (err error) {
	p.port = cfg.GetString(cfgListenPort)
	p.masterAddr = cfg.GetString(cfgMasterAddr)

	p.mc = master.NewMasterClientFromString(p.masterAddr, false)
	p.volMap = make(map[string]*Volume)

	p.sender = make(map[string]PushI)
	p.sender[MockTag] = &mockSendApi{}

	routerAddr := cfg.GetString(cfgRouteAddr)
	if routerAddr != "" {
		p.ru = &routerImp{addr: cfg.GetString(cfgRouteAddr)}
	}

	authAddr := cfg.GetString(cfgAuthAddr)
	volName := cfg.GetString(cfgVolName)
	if volName != "" {
		vid, err := parseVidFromName(volName)
		if err != nil {
			return err
		}

		p.ru = &singleVolImp{Vid: vid}
	} else if authAddr != "" {
		accessKey := cfg.GetString(cfgAuthAccessKey)
		p.auth = &thirdAuth{
			addr:      authAddr,
			accessKey: accessKey,
		}
	}

	if volName == "" && routerAddr == "" {
		return fmt.Errorf("%s and %s can't both be empty", cfgRouteAddr, cfgVolName)
	}

	if authAddr != "" {
		p.auth = &thirdAuth{addr: authAddr}
	}

	p.registerHandler()

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%s", p.port), nil); err != nil {
			log.LogFatalf("start http failed, port (%s), err (%s)", p.port, err.Error())
		}
	}()

	p.wg.Add(1)

	return
}

func (p *ProxyNode) registerHandler() {
	p.hMap = make(map[string]func(ctx *Context))
	// dir
	p.hMap["/api/v1/mkdir"] = p.mkdir
	p.hMap["/api/v1/readdirex"] = p.readDirLimit
	p.hMap["/api/v1/rmdir"] = p.rmdir

	// file
	p.hMap["/api/v1/open"] = p.open
	p.hMap["/api/v1/write"] = p.writeFile
	p.hMap["/api/v1/read"] = p.readFile
	p.hMap["/api/v1/fsync"] = p.fsync
	p.hMap["/api/v1/truncate"] = p.truncate
	p.hMap["/api/v1/unlink"] = p.unlink
	p.hMap["/api/v1/link"] = p.link

	// common
	p.hMap["/api/v1/symbol_link"] = p.symLink
	p.hMap["/api/v1/setXattr"] = p.setXattr
	p.hMap["/api/v1/getXattr"] = p.getXattr
	p.hMap["/api/v1/removeXattr"] = p.removeXattr
	p.hMap["/api/v1/listXattr"] = p.listXattr
	p.hMap["/api/v1/setattr"] = p.setattr
	p.hMap["/api/v1/rename"] = p.rename
	p.hMap["/api/v1/stat"] = p.stat
	p.hMap["/api/v1/statDir"] = p.statDir

	// create user
	p.hMap["/api/v1/createUser"] = p.createUser

	http.HandleFunc("/", p.serveHttp)
}

func (p *ProxyNode) Shutdown() {
	p.wg.Done()
}

// Sync will block invoker goroutine until this ProxyNode shutdown.
func (p *ProxyNode) Sync() {
	p.wg.Wait()
}

func (p *ProxyNode) serveHttp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := &Context{
		Req: r,
	}
	ctx.Context.Req = r
	ctx.Context.Writer = w

	if ctx.UserId() == "" {
		ctx.ResponseErr(lib.ARGS_ERR.WithStr("user id is empty"))
		return
	}

	if p.auth != nil && !p.auth.auth(ctx) {
		return
	}

	err := p.setRouterInfo(ctx)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	url := ctx.getUrl()
	log.LogDebugf("serveHttp url [%s], basePath [%s]", url, ctx.basePath)
	if fn, hit := p.hMap[url]; hit {
		fn(ctx)
		return
	}

	w.WriteHeader(http.StatusNotFound)

}

func (p *ProxyNode) setRouterInfo(ctx *Context) (err error) {
	if strings.Contains(ctx.getUrl(), "/createUser") {
		return
	}

	router, err := p.getRouterInfo(ctx)
	if err != nil {
		return
	}

	if router.Status != lib.RouteNormalStatus {
		return lib.ERR_ROUTE_LOCKED.WithStr("try later")
	}
	ctx.basePath = buildBasePath(router.HashId, ctx.AppId(), ctx.UserId())

	ctx.Vol, err = p.getVol(router.VolumeId)
	if err != nil {
		log.LogErrorf("get vol failed, vid [%d], err %s", router.VolumeId, err.Error())
		return
	}

	return
}

func (p *ProxyNode) getRouterInfo(ctx *Context) (*route.RouteInfoDetail, error) {
	resp, err := p.ru.getRoute(ctx.UserId(), ctx.AppId())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func parseVidFromName(vName string) (id uint32, err error) {
	arr := strings.Split(vName, "_")
	if len(arr) != 2 {
		return 0, fmt.Errorf("cfg volName is not legal, shoule like xxx_1, xxx_2")
	}

	vid, err := strconv.Atoi(arr[1])
	if err != nil {
		return 0, fmt.Errorf("cfg volName is not legal, shoule like xxx_1, xxx_2, err %s", err.Error())
	}

	return uint32(vid), nil
}
