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

package convert

import (
	"encoding/json"
	"github.com/cubefs/cubefs/convertnode"
	"github.com/cubefs/cubefs/proto"
	"net/http"
	"strconv"
)

type ConvertAPI struct {
	cc *ConvertClient
}

func (api *ConvertAPI) List() (view *proto.ConvertNodeViewInfo, err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTLIST)
	var data []byte
	if data, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}
	view = &proto.ConvertNodeViewInfo{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *ConvertAPI) AddConvertTask(cluster, volName string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTADD)
	request.addParam("cluster", cluster)
	request.addParam("vol", volName)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjTaskType)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) QueryConvertTask(cluster, volName string) (view *proto.ConvertTaskInfo,err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTINFO)
	var data []byte

	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjTaskType)
	request.addParam("cluster", cluster)
	request.addParam("vol", volName)

	if data, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	view = &proto.ConvertTaskInfo{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) DelConvertTask(cluster, volName string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTDEL)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjTaskType)
	request.addParam("cluster", cluster)
	request.addParam("vol", volName)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) StartConvertTask(cluster, volName string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTSTART)
	request.addParam("cluster", cluster)
	request.addParam("vol", volName)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjTaskType)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) StopConvertTask(cluster, volName string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTSTOP)
	request.addParam("cluster", cluster)
	request.addParam("vol", volName)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjTaskType)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) StartConvertNode() (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTSTART)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjServerType)
	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}
	return
}

func (api *ConvertAPI) QueryConvertNode(cluster, volName string) (view *proto.ConvertNodeViewInfo,err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTINFO)
	var data []byte
	if data, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	view = &proto.ConvertNodeViewInfo{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *ConvertAPI) StopConvertNode() (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTSTOP)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjServerType)
	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) AddConvertClusterConf(cluster, nodes string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTADD)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjClusterType)
	request.addParam("cluster", cluster)
	request.addParam("nodes", nodes)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) QueryConvertClusterConf(cluster string) (view *proto.ConvertClusterDetailInfo,err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTINFO)
	var data []byte

	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjClusterType)
	request.addParam("cluster", cluster)

	if data, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	view = &proto.ConvertClusterDetailInfo{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) DelConvertClusterConf(cluster, nodes string) (err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTDEL)
	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjClusterType)
	request.addParam("cluster", cluster)
	request.addParam("nodes", nodes)

	if _, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	return
}

func (api *ConvertAPI) QueryConvertProcessorInfo(id int) (view *proto.ConvertProcessorDetailInfo,err error) {
	var request = newAPIRequest(http.MethodGet, convertnode.CONVERTINFO)
	var data []byte

	request.addParam(convertnode.CLIObjTypeKey, convertnode.CLIObjProcessorType)
	request.addParam("processor",  strconv.Itoa(id))

	if data, err = api.cc.serveConvertRequest(request); err != nil {
		return
	}

	view = &proto.ConvertProcessorDetailInfo{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}

	return
}