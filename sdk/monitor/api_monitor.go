// Copyright 2018 The Chubao Authors.
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

func (api *MonitorAPI) GetIPTopPartition(nodeAddr string, moduleType string, timestamp string) (view *proto.MonitorView, err error) {
	var request = newAPIRequest(http.MethodPost, statistics.MonitorIPTopPartition)
	request.addParam("ip", nodeAddr)
	request.addParam("module", moduleType)
	request.addParam("time", timestamp)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.MonitorView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetTopPartitionOp(nodeAddr string, moduleType string, timestamp string, partitionId string) (view *proto.MonitorView, err error) {
	var request = newAPIRequest(http.MethodPost, statistics.MonitorTopPartitionOp)
	request.addParam("ip", nodeAddr)
	request.addParam("module", moduleType)
	request.addParam("time", timestamp)
	request.addParam("pid", partitionId)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.MonitorView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}
func (api *MonitorAPI) GetTopVol(moduleType string, timestamp string, unit string) (view *proto.MonitorView, err error) {
	var request = newAPIRequest(http.MethodPost, statistics.MonitorTopVol)
	request.addParam("module", moduleType)
	request.addParam("time", timestamp)
	request.addParam("unit", unit)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.MonitorView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetTopVolOp(volName string, timeStamp string, unit string) (view *proto.MonitorView, err error) {
	var request = newAPIRequest(http.MethodPost, statistics.MonitorTopVolOp)
	request.addParam("vol", volName)
	request.addParam("time", timeStamp)
	request.addParam("unit", unit)
	var data []byte
	if data, err = api.mc.serveMonitorRequest(request); err != nil {
		return
	}
	view = &proto.MonitorView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *MonitorAPI) GetClusterTopIP(table, module, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorClusterTopIP)
	request.addParam("table", table)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
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

func (api *MonitorAPI) GetClusterTopVol(table, module, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorClusterTopVol)
	request.addParam("table", table)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
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

func (api *MonitorAPI) GetClusterTopPartition(table, module, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorClusterTopPartition)
	request.addParam("table", table)
	request.addParam("module", module)
	request.addParam("start", start)
	request.addParam("end", end)
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

func (api *MonitorAPI) GetOpTopIP(table, op, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorOpTopIP)
	request.addParam("table", table)
	request.addParam("op", op)
	request.addParam("start", start)
	request.addParam("end", end)
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

func (api *MonitorAPI) GetOpTopVol(table, op, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorOpTopVol)
	request.addParam("table", table)
	request.addParam("op", op)
	request.addParam("start", start)
	request.addParam("end", end)
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

func (api *MonitorAPI) GetOpTopPartition(table, op, start, end string, limit int) (view *proto.QueryView, err error) {
	var request = newAPIRequest(http.MethodGet, statistics.MonitorOpTopPartition)
	request.addParam("table", table)
	request.addParam("op", op)
	request.addParam("start", start)
	request.addParam("end", end)
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
