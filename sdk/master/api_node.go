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

func (api *NodeAPI) AddDataNode(serverAddr, zoneName string, mediaType uint32) (id uint64, err error) {
	request := newRequest(get, proto.AddDataNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("mediaType", strconv.Itoa(int(mediaType)))
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddDataNodeWithAuthNode(serverAddr, raftHeartbeatPort, raftReplicaPort, zoneName, clientIDKey string, mediaType uint32) (id uint64, err error) {
	request := newRequest(get, proto.AddDataNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("heartbeatPort", raftHeartbeatPort)
	request.addParam("replicaPort", raftReplicaPort)
	request.addParam("zoneName", zoneName)
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("mediaType", strconv.Itoa(int(mediaType)))
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNode(serverAddr, zoneName string) (id uint64, err error) {
	request := newRequest(get, proto.AddMetaNode).Header(api.h)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNodeWithAuthNode(serverAddr, raftHeartbeatPort, raftReplicatePort, zoneName, clientIDKey string) (id uint64, err error) {
	request := newRequest(get, proto.AddMetaNode).Header(api.h)
	request.addParam("heartbeatPort", raftHeartbeatPort)
	request.addParam("replicaPort", raftReplicatePort)
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

func (api *NodeAPI) GetDataNode(serverHost string) (node *proto.DataNodeInfo, err error) {
	node = &proto.DataNodeInfo{}
	err = api.mc.requestWith(node, newRequest(get, proto.GetDataNode).Header(api.h).addParam("addr", serverHost))
	return
}

func (api *NodeAPI) GetMetaNode(serverHost string) (node *proto.MetaNodeInfo, err error) {
	node = &proto.MetaNodeInfo{}
	err = api.mc.requestWith(node, newRequest(get, proto.GetMetaNode).Header(api.h).addParam("addr", serverHost))
	return
}

func (api *NodeAPI) ResponseMetaNodeTask(task *proto.AdminTask) (err error) {
	return api.mc.request(newRequest(post, proto.GetMetaNodeTaskResponse).Header(api.h).Body(task))
}

func (api *NodeAPI) ResponseDataNodeTask(task *proto.AdminTask) (err error) {
	return api.mc.request(newRequest(post, proto.GetDataNodeTaskResponse).Header(api.h).Body(task))
}

func (api *NodeAPI) DataNodeDecommission(nodeAddr string, count int, clientIDKey string, raftForce bool) (err error) {
	request := newRequest(get, proto.DecommissionDataNode).Header(api.h).NoTimeout()
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("raftForceDel", strconv.FormatBool(raftForce))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeDecommission(nodeAddr string, count int, clientIDKey string) (err error) {
	request := newRequest(get, proto.DecommissionMetaNode).Header(api.h).NoTimeout()
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeMigrate(srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	request := newRequest(get, proto.MigrateMetaNode).Header(api.h).NoTimeout()
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeMigrate(srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	request := newRequest(get, proto.MigrateDataNode).Header(api.h).NoTimeout()
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) AddLcNode(serverAddr string) (id uint64, err error) {
	request := newRequest(get, proto.AddLcNode).Header(api.h).addParam("addr", serverAddr)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) ResponseLcNodeTask(task *proto.AdminTask) (err error) {
	return api.mc.request(newRequest(post, proto.GetLcNodeTaskResponse).Header(api.h).Body(task))
}

func (api *NodeAPI) QueryDecommissionedDisks(addr string) (disks *proto.DecommissionedDisks, err error) {
	disks = &proto.DecommissionedDisks{}
	err = api.mc.requestWith(disks, newRequest(get, proto.QueryDisableDisk).Header(api.h).addParam("addr", addr))
	return
}

func (api *NodeAPI) QueryCancelDecommissionedDataNode(addr string) (err error) {
	err = api.mc.request(newRequest(get, proto.CancelDecommissionDataNode).Header(api.h).addParam("addr", addr))
	return
}

func (api *NodeAPI) AddFlashNode(serverAddr, zoneName, version string) (id uint64, err error) {
	request := newRequest(post, proto.FlashNodeAdd).Header(api.h).
		addParam("addr", serverAddr).addParam("zoneName", zoneName).addParam("version", version)
	val, err := api.mc.serveRequest(request)
	if err != nil {
		return
	}
	id, err = strconv.ParseUint(string(val), 10, 64)
	return
}

func (api *NodeAPI) SetFlashNode(addr, state string) (err error) {
	return api.mc.request(newRequest(post, proto.FlashNodeSet).Header(api.h).
		addParam("addr", addr).addParam("state", state))
}

func (api *NodeAPI) RemoveFlashNode(nodeAddr string) (result string, err error) {
	request := newRequest(post, proto.FlashNodeRemove).Header(api.h).addParam("addr", nodeAddr).NoTimeout()
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *NodeAPI) GetFlashNode(addr string) (node proto.FlashNodeViewInfo, err error) {
	err = api.mc.requestWith(&node, newRequest(get, proto.FlashNodeGet).Header(api.h).addParam("addr", addr))
	return
}

func (api *AdminAPI) ListFlashNodes(all bool) (zoneFlashNodes map[string][]*proto.FlashNodeViewInfo, err error) {
	zoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	err = api.mc.requestWith(&zoneFlashNodes, newRequest(get, proto.FlashNodeList).Header(api.h).addParamAny("all", all))
	return
}
