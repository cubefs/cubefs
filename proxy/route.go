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
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/route"
	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

type routerApi interface {
	allocRoute(uid string, appId uint32) (resp *route.AllocRouteResp, err error)
	addRoute(uid string, appId uint32, req *route.AllocRouteResp) (err error)
	getRoute(uid string, appId uint32) (resp *route.RouteInfoDetail, err error)
}

type singleVolImp struct {
	Vid uint32
}

// try add router after alloc router
func (r *singleVolImp) allocRoute(uid string, appId uint32) (resp *route.AllocRouteResp, err error) {

	resp = &route.AllocRouteResp{
		VolId:   r.Vid,
		GroupId: 0,
		HashId:  0,
	}

	return resp, nil
}

func (r *singleVolImp) addRoute(uid string, appId uint32, req *route.AllocRouteResp) (err error) {
	return
}

func (r *singleVolImp) getRoute(uid string, appId uint32) (resp *route.RouteInfoDetail, err error) {
	resp = &route.RouteInfoDetail{
		GroupId:  0,
		VolumeId: r.Vid,
		HashId:   0,
		Status:   lib.RouteNormalStatus,
	}
	return resp, nil
}

type routerImp struct {
	addr string
}

// try add router after alloc router
func (r *routerImp) allocRoute(uid string, appId uint32) (resp *route.AllocRouteResp, err error) {

	resp = &route.AllocRouteResp{}
	// reply := lib.HttpReply{}
	url := fmt.Sprintf("%s/router/api/v1/allocroute", r.addr)
	header := map[string]string{lib.UserIdKey: uid, lib.AppIdKey: strconv.Itoa(int(appId))}
	err = lib.InnerDoHttp(url, http.MethodPost, nil, resp, header)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *routerImp) addRoute(uid string, appId uint32, req *route.AllocRouteResp) (err error) {
	header := map[string]string{lib.UserIdKey: uid, lib.AppIdKey: strconv.Itoa(int(appId))}
	url := fmt.Sprintf("%s/router/api/v1/addroute", r.addr)
	addReq := &route.AddRouteReq{
		VolumeId: req.VolId,
		GroupId:  req.GroupId,
		HashId:   req.HashId,
	}

	err = lib.InnerDoHttp(url, http.MethodPost, addReq, nil, header)
	if err != nil {
		return
	}

	return
}

func (r *routerImp) getRoute(uid string, appId uint32) (resp *route.RouteInfoDetail, err error) {
	rInfo := &route.RouteInfoDetail{}
	header := map[string]string{lib.UserIdKey: uid, lib.AppIdKey: strconv.Itoa(int(appId))}
	url := fmt.Sprintf("%s/router/api/v1/getroute", r.addr)

	err = lib.InnerDoHttp(url, http.MethodPost, nil, rInfo, header)
	if err != nil {
		return
	}

	return rInfo, nil
}

func (p *ProxyNode) createUser(ctx *Context) {
	userId := ctx.UserId()
	appId := ctx.AppId()

	log.LogDebugf("start create user req, uid %s, appid %d", userId, appId)

	resp, err := p.ru.allocRoute(userId, appId)
	if err != nil {
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("alloc route success, route %v", resp)

	vol, err := p.getVol(resp.VolId)
	if err != nil {
		ctx.ResponseErr(lib.ERR_ROUTE_NOT_EXIST.With(err))
		return
	}

	basePath := buildBasePath(resp.HashId, appId, userId)
	log.LogDebugf("start mkdirAll, path: %s", basePath)
	err = vol.mkdirAll(basePath)
	if err != nil {
		log.LogErrorf("mkdir path failed, dir [%s]", basePath)
		ctx.ResponseErr(err)
		return
	}

	err = p.ru.addRoute(userId, appId, resp)
	if err != nil && !lib.ERR_ROUTE_EXIST.Same(err) {
		log.LogErrorf("add route failed, req [%v], err %s", resp, err.Error())
		ctx.ResponseErr(err)
		return
	}

	ctx.ResponseOk()
}

func buildVolName(volId uint32) string {
	return fmt.Sprintf("volume_%d", volId)
}

func buildBasePath(hashId, appId uint32, userId string) string {
	return fmt.Sprintf("/%d/%s/%d", hashId, userId, 0)
}

func (v *Volume) mkdirAll(dir string) error {
	dirs := strings.Split(dir, "/")
	newPath := "/"
	for _, d := range dirs {
		if d == "/" || d == "" {
			continue
		}

		newPath = path.Join(newPath, d)
		log.LogDebugf("start mkdir, [%s]", newPath)
		err := v.mkdir(newPath, 0755)
		if err != nil && !lib.ERR_PROXY_EEXIST.Same(err) {
			log.LogErrorf("mkdir dir failed, [%s]", newPath)
			return err
		}
	}

	return nil
}
