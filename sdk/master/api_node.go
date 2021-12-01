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
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
)

type NodeAPI struct {
	mc *MasterClient
}

func (api *NodeAPI) AddDataNode(serverAddr, zoneName string) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddDataNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNode(serverAddr, zoneName string, version uint32) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddMetaNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("version", fmt.Sprintf("%d", version))
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) GetDataNode(serverHost string) (node *proto.DataNodeInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.GetDataNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.DataNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) GetMetaNode(serverHost string) (node *proto.MetaNodeInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.GetMetaNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.MetaNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) ResponseMetaNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.GetMetaNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		log.LogErrorf("serveRequest: %v", err.Error())
		return
	}
	return
}

func (api *NodeAPI) ResponseDataNodeTask(task *proto.AdminTask) (err error) {

	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.GetDataNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeDecommission(nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionDataNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeDiskDecommission(nodeAddr, diskID string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionDisk)
	request.addParam("addr", nodeAddr)
	request.addParam("disk", diskID)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeDecommission(nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionMetaNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeGetPartition(addr string, id uint64) (node *proto.DNDataPartitionInfo, err error) {
	var request = newAPIRequest(http.MethodGet, "/partition")
	var buf []byte
	nodeClient := NewNodeClient(fmt.Sprintf("%v:%v", addr, api.mc.DataNodeProfPort), false, DATANODE)
	nodeClient.DataNodeProfPort = api.mc.DataNodeProfPort
	request.addParam("id", strconv.FormatUint(id, 10))
	request.addHeader("isTimeOut", "false")
	if buf, err = nodeClient.serveRequest(request); err != nil {
		return
	}
	node = &proto.DNDataPartitionInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeGetPartition(addr string, id uint64) (node *proto.MNMetaPartitionInfo, err error) {
	var request = newAPIRequest(http.MethodGet, "/getPartitionById")
	var buf []byte
	nodeClient := NewNodeClient(fmt.Sprintf("%v:%v", addr, api.mc.MetaNodeProfPort), false, METANODE)
	nodeClient.MetaNodeProfPort = api.mc.MetaNodeProfPort
	request.addParam("pid", strconv.FormatUint(id, 10))
	request.addHeader("isTimeOut", "false")
	if buf, err = nodeClient.serveRequest(request); err != nil {
		return
	}
	node = &proto.MNMetaPartitionInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeValidateCRCReport(dpCrcInfo *proto.DataPartitionExtentCrcInfo) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(dpCrcInfo); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.DataNodeValidateCRCReport)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}
