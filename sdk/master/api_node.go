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
	"context"
	"strconv"

	"github.com/cubefs/cubefs/proto"
)

type NodeAPI struct {
	mc *MasterClient
	h  map[string]string // extra headers
}

func (api *NodeAPI) WithHeader(key, val string) *NodeAPI {
	return &NodeAPI{mc: api.mc, h: mergeHeader(api.h, key, val)}
}

func (api *NodeAPI) EncodingWith(encoding string) *NodeAPI {
	return api.WithHeader(headerAcceptEncoding, encoding)
}

func (api *NodeAPI) EncodingGzip() *NodeAPI {
	return api.EncodingWith(encodingGzip)
}

func (api *NodeAPI) AddDataNode(ctx context.Context, serverAddr, zoneName string) (id uint64, err error) {
	ctx = proto.ContextWithOperation(ctx, "AddDataNode")
	request := newRequest(ctx, get, proto.AddDataNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddDataNodeWithAuthNode(ctx context.Context, serverAddr, zoneName, clientIDKey string) (id uint64, err error) {
	ctx = proto.ContextWithOperation(ctx, "AddDataNodeWithAuthNode")
	request := newRequest(ctx, get, proto.AddDataNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("clientIDKey", clientIDKey)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNode(ctx context.Context, serverAddr, zoneName string) (id uint64, err error) {
	ctx = proto.ContextWithOperation(ctx, "AddMetaNode")
	request := newRequest(ctx, get, proto.AddMetaNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNodeWithAuthNode(ctx context.Context, serverAddr, zoneName, clientIDKey string) (id uint64, err error) {
	ctx = proto.ContextWithOperation(ctx, "AddMetaNodeWithAuthNode")
	request := newRequest(ctx, get, proto.AddMetaNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("clientIDKey", clientIDKey)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) GetDataNode(ctx context.Context, serverHost string) (node *proto.DataNodeInfo, err error) {
	node = &proto.DataNodeInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetDataNode")
	err = api.mc.requestWith(node, newRequest(ctx, get, proto.GetDataNode).Header(api.h).addParam("addr", serverHost))
	return
}

func (api *NodeAPI) GetMetaNode(ctx context.Context, serverHost string) (node *proto.MetaNodeInfo, err error) {
	node = &proto.MetaNodeInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetMetaNode")
	err = api.mc.requestWith(node, newRequest(ctx, get, proto.GetMetaNode).Header(api.h).addParam("addr", serverHost))
	return
}

func (api *NodeAPI) ResponseMetaNodeTask(ctx context.Context, task *proto.AdminTask) (err error) {
	ctx = proto.ContextWithOperation(ctx, "ResponseMetaNodeTask")
	return api.mc.request(newRequest(ctx, post, proto.GetMetaNodeTaskResponse).Header(api.h).Body(task))
}

func (api *NodeAPI) ResponseDataNodeTask(ctx context.Context, task *proto.AdminTask) (err error) {
	ctx = proto.ContextWithOperation(ctx, "ResponseDataNodeTask")
	return api.mc.request(newRequest(ctx, post, proto.GetDataNodeTaskResponse).Header(api.h).Body(task))
}

func (api *NodeAPI) DataNodeDecommission(ctx context.Context, nodeAddr string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DataNodeDecommission")
	request := newRequest(ctx, get, proto.DecommissionDataNode).Header(api.h).NoTimeout()
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeDecommission(ctx context.Context, nodeAddr string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "MetaNodeDecommission")
	request := newRequest(ctx, get, proto.DecommissionMetaNode).Header(api.h).NoTimeout()
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeMigrate(ctx context.Context, srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "MetaNodeMigrate")
	request := newRequest(ctx, get, proto.MigrateMetaNode).Header(api.h).NoTimeout()
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeMigrate(ctx context.Context, srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DataNodeMigrate")
	request := newRequest(ctx, get, proto.MigrateDataNode).Header(api.h).NoTimeout()
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) AddLcNode(ctx context.Context, serverAddr string) (id uint64, err error) {
	ctx = proto.ContextWithOperation(ctx, "AddLcNode")
	request := newRequest(ctx, get, proto.AddLcNode).Header(api.h).addParam("addr", serverAddr)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) ResponseLcNodeTask(ctx context.Context, task *proto.AdminTask) (err error) {
	ctx = proto.ContextWithOperation(ctx, "ResponseLcNodeTask")
	return api.mc.request(newRequest(ctx, post, proto.GetLcNodeTaskResponse).Header(api.h).Body(task))
}
