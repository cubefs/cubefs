// Copyright 2018 The CubeFS Authors.
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

package monitor

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/statistics"
)

type MonitorAPI struct {
	mc *MonitorClient
}

func (api *MonitorAPI) GetClusterTopIP(table, cluster, module, start, end, order string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorClusterTopIP)
	request.addParam("tableUnit", table)
	request.addParam("cluster", cluster)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("order", order)
	request.addParam("limit", strconv.Itoa(limit))
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetClusterTopVol(table, cluster, module, start, end, order string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorClusterTopVol)
	request.addParam("tableUnit", table)
	request.addParam("cluster", cluster)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("order", order)
	request.addParam("limit", strconv.Itoa(limit))
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetOpTopIP(table, cluster, op, start, end, order string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorOpTopIP)
	request.addParam("tableUnit", table)
	request.addParam("cluster", cluster)
	request.addParam("op", op)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("order", order)
	request.addParam("limit", strconv.Itoa(limit))
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetOpTopVol(table, cluster, op, start, end, order string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorOpTopVol)
	request.addParam("tableUnit", table)
	request.addParam("cluster", cluster)
	request.addParam("op", op)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("order", order)
	request.addParam("limit", strconv.Itoa(limit))
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetTopPartition(table, cluster, module, start, end string, limit int, group, order, ip, op string) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorTopPartition)
	request.addParam("table", table)
	request.addParam("cluster", cluster)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("limit", strconv.Itoa(limit))
	request.addParam("group", group)
	request.addParam("order", order)
	request.addParam("ip", ip)
	request.addParam("op", op)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetTopOp(table, cluster, module, start, end string, limit int, order, ip, vol, pid string) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorTopOp)
	request.addParam("table", table)
	request.addParam("cluster", cluster)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("limit", strconv.Itoa(limit))
	request.addParam("order", order)
	request.addParam("ip", ip)
	request.addParam("vol", vol)
	request.addParam("pid", pid)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetTopIP(table, cluster, module, start, end string, limit int, order, op, vol string) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorTopIP)
	request.addParam("table", table)
	request.addParam("cluster", cluster)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
	request.addParam("limit", strconv.Itoa(limit))
	request.addParam("order", order)
	request.addParam("vol", vol)
	request.addParam("op", op)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.QueryView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}