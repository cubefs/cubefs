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
	"fmt"
	"math/rand"

	"github.com/cubefs/cubefs/proto"
)

type Decoder func([]byte) ([]byte, error)

func (d Decoder) Decode(raw []byte) ([]byte, error) {
	return d(raw)
}

type ClientAPI struct {
	mc *MasterClient
	h  map[string]string // extra headers
}

func (api *ClientAPI) WithHeader(key, val string) *ClientAPI {
	return &ClientAPI{mc: api.mc, h: mergeHeader(api.h, key, val)}
}

func (api *ClientAPI) EncodingWith(encoding string) *ClientAPI {
	return api.WithHeader(headerAcceptEncoding, encoding)
}

func (api *ClientAPI) EncodingGzip() *ClientAPI {
	return api.EncodingWith(encodingGzip)
}

func (api *ClientAPI) GetVolume(volName string, authKey string) (vv *proto.VolView, err error) {
	vv = &proto.VolView{}
	err = api.mc.requestWith(vv, newRequest(post, proto.ClientVol).
		Header(api.h).Param(anyParam{"name", volName}, anyParam{"authKey", authKey}))
	return
}

func (api *ClientAPI) GetVolumeWithoutAuthKey(volName string) (vv *proto.VolView, err error) {
	vv = &proto.VolView{}
	err = api.mc.requestWith(vv, newRequest(post, proto.ClientVol).
		Header(api.h, proto.SkipOwnerValidation, "true").addParam("name", volName))
	return
}

func (api *ClientAPI) GetVolumeWithAuthnode(volName string, authKey string, token string, decoder Decoder) (vv *proto.VolView, err error) {
	var body []byte
	request := newRequest(post, proto.ClientVol).Header(api.h)
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
	info = &proto.VolStatInfo{}
	err = api.mc.requestWith(info, newRequest(get, proto.ClientVolStat).
		Header(api.h).Param(anyParam{"name", volName}, anyParam{"version", proto.LFClient}))
	return
}

func (api *ClientAPI) GetMetaPartition(partitionID uint64) (partition *proto.MetaPartitionInfo, err error) {
	partition = &proto.MetaPartitionInfo{}
	err = api.mc.requestWith(partition, newRequest(get, proto.ClientMetaPartition).
		Header(api.h).addParamAny("id", partitionID))
	return
}

func (api *ClientAPI) GetMetaPartitions(volName string) (views []*proto.MetaPartitionView, err error) {
	views = make([]*proto.MetaPartitionView, 0)
	err = api.mc.requestWith(&views, newRequest(get, proto.ClientMetaPartitions).
		Header(api.h).addParam("name", volName))
	return
}

func (api *ClientAPI) GetDataPartitionsFromLeader(volName string) (view *proto.DataPartitionsView, err error) {
	request := newRequest(get, proto.ClientDataPartitions).Header(api.h).addParam("name", volName)
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

func (api *ClientAPI) GetDataPartitions(volName string) (view *proto.DataPartitionsView, err error) {
	lastLeader := api.mc.leaderAddr
	defer api.mc.SetLeader(lastLeader)
	randIndex := rand.Intn(len(api.mc.masters))
	if randIndex >= len(api.mc.masters) {
		err = fmt.Errorf("master len %v less or equal request index %v", len(api.mc.masters), randIndex)
		return
	}
	api.mc.SetLeader(api.mc.masters[randIndex])
	view, err = api.GetDataPartitionsFromLeader(volName)
	return
}

func (api *ClientAPI) GetPreLoadDataPartitions(volName string) (view *proto.DataPartitionsView, err error) {
	view = &proto.DataPartitionsView{}
	err = api.mc.requestWith(view, newRequest(get, proto.ClientDataPartitions).
		Header(api.h).addParam("name", volName))
	return
}
