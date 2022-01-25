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

package master

import (
	"encoding/json"
	"math/rand"
	"net/http"
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

func (api *ClientAPI) GetMetaPartition(partitionID uint64) (partition *proto.MetaPartitionInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.ClientMetaPartition)
	request.addParam("id", strconv.FormatUint(partitionID, 10))
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
	var request = newAPIRequest(http.MethodGet, proto.ClientDataPartitions)
	request.addParam("name", volName)

	lastLeader := api.mc.leaderAddr
	defer api.mc.setLeader(lastLeader)
	randIndex := rand.Intn(len(api.mc.masters))

	api.mc.setLeader(api.mc.masters[randIndex])
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
