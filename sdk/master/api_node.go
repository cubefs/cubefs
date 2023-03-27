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
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
)

type NodeAPI struct {
	mc *MasterClient
}

func (api *NodeAPI) AddDataNode(serverAddr, zoneName, version string) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddDataNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("version", version)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNode(serverAddr, zoneName, version string) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddMetaNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("version", version)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddCodecNode(serverAddr, version string) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddCodecNode)
	request.addParam("addr", serverAddr)
	request.addParam("version", version)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) CodEcNodeDecommission(nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionCodecNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) AddEcNode(serverAddr, httpPort, zoneName, version string) (id uint64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AddEcNode)
	request.addParam("addr", serverAddr)
	request.addParam("httpPort", httpPort)
	request.addParam("zoneName", zoneName)
	request.addParam("version", version)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) GetEcScrubInfo() (scrubInfo proto.UpdateEcScrubInfoRequest, err error) {
	var respData []byte
	var respInfo proto.UpdateEcScrubInfoRequest
	var request = newAPIRequest(http.MethodGet, proto.AdminClusterGetScrub)
	request.addHeader("isTimeOut", "false")

	if respData, err = api.mc.serveRequest(request); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &respInfo); err != nil {
		return
	}
	return respInfo, err
}

func (api *NodeAPI) EcNodeDecommission(nodeAddr string) (data []byte, err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionEcNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")

	data, err = api.mc.serveRequest(request)
	return
}

func (api *NodeAPI) EcNodeDiskDecommission(nodeAddr, diskID string) (data []byte, err error) {
	var request = newAPIRequest(http.MethodGet, proto.DecommissionEcDisk)
	request.addParam("addr", nodeAddr)
	request.addParam("disk", diskID)
	request.addHeader("isTimeOut", "false")

	data, err = api.mc.serveRequest(request)
	return
}

func (api *NodeAPI) EcNodegetTaskStatus() (taskView []*proto.MigrateTaskView, err error) {
	var data []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetAllTaskStatus)
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	taskView = make([]*proto.MigrateTaskView, 0)
	err = json.Unmarshal(data, &taskView)
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

func (api *NodeAPI) ResponseCodecNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.GetCodecNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) ResponseEcNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.GetEcNodeTaskResponse)
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
	node = new(proto.DNDataPartitionInfo)
	pInfoOld := new(proto.DNDataPartitionInfoOldVersion)
	if err = json.Unmarshal(buf, pInfoOld); err != nil {
		err = json.Unmarshal(buf, node)
		return
	}
	for _, ext := range pInfoOld.Files {
		extent := proto.ExtentInfoBlock{
			ext.FileID,
			ext.Size,
			uint64(ext.Crc),
			uint64(ext.ModifyTime),
		}
		node.Files = append(node.Files, extent)
	}
	node.RaftStatus = pInfoOld.RaftStatus
	node.Path = pInfoOld.Path
	node.VolName = pInfoOld.VolName
	node.Replicas = pInfoOld.Replicas
	node.Size = pInfoOld.Size
	node.ID = pInfoOld.ID
	node.Status = pInfoOld.Status
	node.FileCount = pInfoOld.FileCount
	node.Peers = pInfoOld.Peers
	node.Learners = pInfoOld.Learners
	node.Used = pInfoOld.Used
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

func (api *NodeAPI) DataNodeGetTinyExtentHolesAndAvali(addr string, partitionID, extentID uint64) (info *proto.DNTinyExtentInfo, err error) {
	var request = newAPIRequest(http.MethodGet, "/tinyExtentHoleInfo")
	var buf []byte
	nodeClient := NewNodeClient(fmt.Sprintf("%v:%v", addr, api.mc.DataNodeProfPort), false, DATANODE)
	nodeClient.DataNodeProfPort = api.mc.DataNodeProfPort
	request.addParam("partitionID", strconv.FormatUint(partitionID, 10))
	request.addParam("extentID", strconv.FormatUint(extentID, 10))
	request.addHeader("isTimeOut", "false")
	if buf, err = nodeClient.serveRequest(request); err != nil {
		return
	}
	info = &proto.DNTinyExtentInfo{}
	if err = json.Unmarshal(buf, &info); err != nil {
		return
	}
	return
}

func (api *NodeAPI) GetCodecNode(serverHost string) (node *proto.CodecNodeInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.GetCodecNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.CodecNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) GetEcNode(serverHost string) (node *proto.EcNodeInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.GetEcNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.EcNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) EcNodeGetTaskStatus() (taskView []*proto.MigrateTaskView, err error) {
	var data []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetAllTaskStatus)
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	taskView = make([]*proto.MigrateTaskView, 0)
	err = json.Unmarshal(data, &taskView)
	return
}

func (api *NodeAPI) DataNodeGetExtentCrc(addr string, partitionId, extentId uint64) (crc uint32, err error) {
	var request = newAPIRequest(http.MethodGet, "/extentCrc")
	var buf []byte
	nodeClient := NewNodeClient(fmt.Sprintf("%v:%v", addr, api.mc.DataNodeProfPort), false, DATANODE)
	nodeClient.DataNodeProfPort = api.mc.DataNodeProfPort
	request.addParam("partitionId", strconv.FormatUint(partitionId, 10))
	request.addParam("extentId", strconv.FormatUint(extentId, 10))
	request.addHeader("isTimeOut", "false")
	if buf, err = nodeClient.serveRequest(request); err != nil {
		return
	}
	resp := &struct {
		CRC uint32
	}{}
	if err = json.Unmarshal(buf, resp); err != nil {
		return
	}
	crc = resp.CRC
	return
}

func (api *NodeAPI) EcNodeGetExtentCrc(addr string, partitionId, extentId, stripeCount uint64, crc uint32) (resp *proto.ExtentCrcResponse, err error) {
	var request = newAPIRequest(http.MethodGet, "/extentCrc")
	var buf []byte
	nodeClient := NewNodeClient(fmt.Sprintf("%v:%v", addr, api.mc.EcNodeProfPort), false, ECNODE)
	nodeClient.EcNodeProfPort = api.mc.EcNodeProfPort
	request.addParam("partitionId", strconv.FormatUint(partitionId, 10))
	request.addParam("extentId", strconv.FormatUint(extentId, 10))
	request.addParam("stripeCount", strconv.FormatUint(stripeCount, 10))
	request.addParam("crc", strconv.FormatUint(uint64(crc), 10))
	request.addHeader("isTimeOut", "false")
	if buf, err = nodeClient.serveRequest(request); err != nil {
		return
	}
	resp = &proto.ExtentCrcResponse{}
	if err = json.Unmarshal(buf, resp); err != nil {
		return
	}

	return
}

func (api *NodeAPI) StopMigratingByDataNode(datanode string) string {
	var request = newAPIRequest(http.MethodGet, proto.AdminDNStopMigrating)
	request.addParam("addr", datanode)
	data, err := api.mc.serveRequest(request)
	if err != nil {
		return fmt.Sprintf("StopMigratingByDataNode fail:%v\n", err)
	}
	return string(data)
}
