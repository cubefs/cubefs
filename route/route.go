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
	"hash/crc32"
	"time"

	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

type AddRouteReq struct {
	GroupId  uint32 `json:"groupid"`
	VolumeId uint32 `json:"volumeid"`
	HashId   uint32 `json:"hashid"`
}

func (a *AddRouteReq) IsValid() bool {
	return true
}

func (p *RouteNode) addRoute(ctx *Context) {
	req := new(AddRouteReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	userId := ctx.UserId()
	appId := ctx.AppId()

	start := time.Now()
	log.LogDebugf("start add route req %v", req)
	vItem, err := p.volOp.GetVol(req.GroupId, req.VolumeId)
	if err != nil {
		log.LogErrorf("can't find vaild vol, req %v, err %s", req, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("get vol info %v", vItem)

	ur := &UserRoute{
		UserId:   userId,
		AppId:    appId,
		GroupId:  req.GroupId,
		VolumeId: req.VolumeId,
		Status:   lib.RouteNormalStatus,
		HashId:   req.HashId,
	}
	err = p.routeOp.AddRoute(ur)
	if err != nil {
		log.LogErrorf("add route failed, req %s, err %s", ur, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("add user route success, req %v, cost %s", ur, time.Since(start).String())
	ctx.ResponseOk()
}

type AllocRouteResp struct {
	GroupId uint32 `json:"groupId"`
	VolId   uint32 `json:"volumeId"`
	HashId  uint32 `json:"hashId"`
}

func (p *RouteNode) allocRoute(ctx *Context) {
	appId := ctx.AppId()
	userId := ctx.UserId()

	log.LogDebugf("start alloc route, user %s, app %d", userId, appId)

	resp, err := p.alloc(userId, appId)
	if err != nil {
		ctx.ResponseErr(err)
		log.LogErrorf("alloc route failed, user %s, app %d, err %s", userId, appId, err.Error())
		return
	}

	log.LogDebugf("alloc route succ, resp %v", resp)
	ctx.ResponseOkWith(resp)
}

func (p *RouteNode) alloc(userId string, appId uint32) (resp *AllocRouteResp, err error) {
	log.LogDebugf("try alloc route, userId %s, appId %d", userId, appId)

	p.lock.Lock()
	defer p.lock.Unlock()

	hashId := userIdHash(userId)
	resp = &AllocRouteResp{
		HashId: uint32(hashId % 1024),
	}

	routes, err := p.routeOp.ListRouteByUid(userId)
	if err != nil {
		log.LogErrorf("list route by uid failed, userId %s, err %s", userId, err.Error())
		return
	}

	for _, r := range routes {
		if r.AppId == appId {
			resp.VolId = r.VolumeId
			resp.GroupId = r.GroupId
			log.LogInfof("route is already added before")
			return
		}
	}

	if len(routes) > 0 {
		idx := hashId % (len(routes))
		resp.VolId = routes[idx].VolumeId
		resp.GroupId = routes[idx].GroupId
		log.LogInfof("alloc routes according to old route info succ")
		return
	}

	vols, err := p.volOp.ListVols()
	if err != nil {
		log.LogErrorf("list vols failed, err %s", err.Error())
		return
	}

	if len(vols) == 0 {
		log.LogErrorf("no valid vols")
		err = lib.ERR_ROUTE_NOT_EXIST.WithStr("no vaild vols")
		return
	}

	idx := hashId % len(vols)
	resp.VolId = vols[idx].VolumeId
	resp.GroupId = vols[idx].GroupId

	log.LogDebugf("alloc routers success")

	return
}

func userIdHash(userId string) int {
	return int(crc32.ChecksumIEEE([]byte(userId)))
}

type RouteInfoDetail struct {
	GroupId  uint32 `json:"groupid"`
	VolumeId uint32 `json:"volumeid"`
	Ak       string `json:"ak"`
	Sk       string `json:"sk"`
	HashId   uint32 `json:"hashid"`
	Status   uint8  `json:"status"`
}

func (p *RouteNode) getRoute(ctx *Context) {
	appId := ctx.AppId()
	userId := ctx.UserId()

	log.LogDebugf("start get route, user %s, app %d", userId, appId)

	r, err := p.routeOp.GetRoute(userId, int(appId))
	if err != nil {
		log.LogErrorf("can't find vaild router info, userId %s, appId %d, err %s", userId, appId, err.Error())
		ctx.ResponseErr(err)
		return
	}

	vol, err := p.volOp.GetVol(r.GroupId, r.VolumeId)
	if err != nil {
		log.LogErrorf("can't find vaild volume info, router %v, err %s", r, err.Error())
		ctx.ResponseErr(err)
		return
	}

	rfo := &RouteInfoDetail{
		GroupId:  uint32(r.GroupId),
		VolumeId: uint32(r.VolumeId),
		Ak:       vol.Ak,
		Sk:       vol.Sk,
		HashId:   uint32(r.HashId),
		Status:   r.Status,
	}

	log.LogDebugf("get route detail succ, resp %v", rfo)
	ctx.ResponseOkWith(rfo)
}

func (p *RouteNode) lockRoute(ctx *Context) {
	appId := ctx.AppId()
	userId := ctx.UserId()

	log.LogDebugf("start lock route, user %s, app %d", userId, appId)

	r, err := p.routeOp.GetRoute(userId, int(appId))
	if err != nil {
		log.LogErrorf("get route failed, userId %s, appId %d, err %s", userId, appId, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("start lock route, userId %s, appId %d, status before[%d]", userId, appId, r.Status)

	r.Status = lib.RouteLockStatus
	err = p.routeOp.UpdateRoute(r)
	if err != nil {
		log.LogErrorf("update route failed, userId %s, appId %d, err %s", userId, appId, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("lock route succ")
	ctx.ResponseOk()
}

type UnlockReq struct {
	GroupId  uint32 `json:"groupid"`
	VolumeId uint32 `json:"volumeid"`
	HashId   uint32 `json:"hashid"`
}

func (l *UnlockReq) IsValid() bool {
	return true
}

func (p *RouteNode) unlockRoute(ctx *Context) {
	appId := ctx.AppId()
	userId := ctx.UserId()

	req := new(UnlockReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	log.LogDebugf("start ulock route, user %s, app %d, req %v", userId, appId, req)

	r, err := p.routeOp.GetRoute(userId, int(appId))
	if err != nil {
		log.LogErrorf("get route failed, userId %s, appId %d, err %s", userId, appId, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("start ulock route, userId %s, appId %d, status before[%d]", userId, appId, r.Status)

	r.Status = lib.RouteNormalStatus
	r.GroupId = req.GroupId
	r.VolumeId = req.VolumeId
	r.HashId = req.HashId

	err = p.routeOp.UpdateRoute(r)
	if err != nil {
		log.LogErrorf("update route failed, route %v, err %s", r, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("ulock route succ, route %v", r)
	ctx.ResponseOk()
}

type AddVolReq struct {
	VolId   uint32 `json:"volumeid"`
	GroupId uint32 `json:"groupid"`
	Ak      string `json:"ak"`
	Sk      string `json:"sk"`
}

func (a *AddVolReq) IsValid() bool {
	if a.VolId == 0 {
		return false
	}
	return true
}

func (p *RouteNode) addVol(ctx *Context) {

	req := new(AddVolReq)
	err := ctx.ParseReq(req)
	if err != nil {
		return
	}

	log.LogDebugf("start add vol, req %v", req)

	e := &VolItem{
		VolumeId: req.VolId,
		GroupId:  req.GroupId,
		Ak:       req.Ak,
		Sk:       req.Sk,
	}

	err = p.volOp.AddVol(e)
	if err != nil {
		log.LogErrorf("add vol failed, req %v err %s", e, err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("add vol success, req %v", req)
	ctx.ResponseOk()
}

func (p *RouteNode) listVols(ctx *Context) {

	log.LogDebugf("start list vols")

	items, err := p.volOp.ListVols()
	if err != nil {
		log.LogErrorf("list vols failed, err %s", err.Error())
		ctx.ResponseErr(err)
		return
	}

	log.LogDebugf("add vol success, len %v", len(items))
	ctx.ResponseOkWith(items)
}
