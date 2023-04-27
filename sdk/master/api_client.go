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

package master

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cubefs/cubefs/proto"
)

type Decoder func([]byte) ([]byte, error)

func (d Decoder) Decode(raw []byte) ([]byte, error) {
	return d(raw)
}

type ClientAPI struct {
	mc *MasterClient
}

func (api *ClientAPI) GetVolume(volName string, authKey string) (vv *proto.VolView, err error) {
	var request = newAPIRequest(http.MethodPost, proto.ClientVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("baseVersion", proto.BaseVersion)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	vv = &proto.VolView{}
	if err = json.Unmarshal(data, vv); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetVolumeWithoutAuthKey(volName string) (vv *proto.VolView, err error) {
	var request = newAPIRequest(http.MethodPost, proto.ClientVol)
	request.addParam("name", volName)
	request.addHeader(proto.SkipOwnerValidation, strconv.FormatBool(true))
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	vv = &proto.VolView{}
	if err = json.Unmarshal(data, vv); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetVolumeWithAuthnode(volName string, authKey string, token string, decoder Decoder) (vv *proto.VolView, err error) {
	var body []byte
	var request = newAPIRequest(http.MethodPost, proto.ClientVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam(proto.ClientMessage, token)
	if body, err = api.mc.serveRequest(request); err != nil {
		return
	}
	if decoder != nil {
		if body, err = decoder.Decode(body); err != nil {
			return
		}
	}
	vv = &proto.VolView{}
	if err = json.Unmarshal(body, vv); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetVolumeStat(volName string) (info *proto.VolStatInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.ClientVolStat)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	info = &proto.VolStatInfo{}
	if err = json.Unmarshal(data, info); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetToken(volName, tokenKey string) (token *proto.Token, err error) {
	var request = newAPIRequest(http.MethodGet, proto.TokenGetURI)
	request.addParam("name", volName)
	request.addParam("token", url.QueryEscape(tokenKey))
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	token = &proto.Token{}
	if err = json.Unmarshal(data, token); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetMetaPartition(partitionID uint64, volName string) (partition *proto.MetaPartitionInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.ClientMetaPartition)
	request.addParam("id", strconv.FormatUint(partitionID, 10))
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	partition = &proto.MetaPartitionInfo{}
	if err = json.Unmarshal(data, partition); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetMetaPartitions(volName string) (views []*proto.MetaPartitionView, err error) {
	var request = newAPIRequest(http.MethodGet, proto.ClientMetaPartitions)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	if err = json.Unmarshal(data, &views); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetDataPartitions(volName string) (view *proto.DataPartitionsView, err error) {
	path := proto.ClientDataPartitions
	if proto.IsDbBack {
		path = proto.ClientDataPartitionsDbBack
	}
	var request = newAPIRequest(http.MethodGet, path)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	view = &proto.DataPartitionsView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetEcPartitions(volName string) (view *proto.EcPartitionsView, err error) {
	path := proto.ClientEcPartitions
	var request = newAPIRequest(http.MethodGet, path)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	view = &proto.EcPartitionsView{}
	if err = json.Unmarshal(data, view); err != nil {
		return
	}
	return
}

func (api *ClientAPI) ApplyVolMutex(app, volName, addr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminApplyVolMutex)
	request.addParam("app", app)
	request.addParam("name", volName)
	request.addParam("addr", addr)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *ClientAPI) ReleaseVolMutex(app, volName, addr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminReleaseVolMutex)
	request.addParam("app", app)
	request.addParam("name", volName)
	request.addParam("addr", addr)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *ClientAPI) GetVolMutex(app, volName string) (*proto.VolWriteMutexInfo, error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetVolMutex)
	request.addParam("app", app)
	request.addParam("name", volName)
	data, err := api.mc.serveRequest(request)
	if err != nil {
		return nil, err
	}
	volWriteMutexInfo := &proto.VolWriteMutexInfo{}
	if err = json.Unmarshal(data, volWriteMutexInfo); err != nil {
		return nil, err
	}
	return volWriteMutexInfo, nil
}

func (api *ClientAPI) SetClientPkgAddr(addr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminSetClientPkgAddr)
	request.addParam("addr", addr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *ClientAPI) GetClientPkgAddr() (addr string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetClientPkgAddr)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	if err = json.Unmarshal(data, &addr); err != nil {
		return
	}
	return
}
