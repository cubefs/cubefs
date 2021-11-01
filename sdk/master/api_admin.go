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
	"net/http"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
)

type AdminAPI struct {
	mc *MasterClient
}

func (api *AdminAPI) GetCluster() (cv *proto.ClusterView, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetCluster)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	cv = &proto.ClusterView{}
	if err = json.Unmarshal(buf, &cv); err != nil {
		return
	}
	return
}
func (api *AdminAPI) GetClusterStat() (cs *proto.ClusterStatInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminClusterStat)
	request.addHeader("isTimeOut", "false")
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	cs = &proto.ClusterStatInfo{}
	if err = json.Unmarshal(data, &cs); err != nil {
		return
	}
	return
}
func (api *AdminAPI) GetDataPartition(volName string, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetDataPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addParam("name", volName)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	partition = &proto.DataPartitionInfo{}
	if err = json.Unmarshal(buf, &partition); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DiagnoseDataPartition() (diagnosis *proto.DataPartitionDiagnosis, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminDiagnoseDataPartition)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	diagnosis = &proto.DataPartitionDiagnosis{}
	if err = json.Unmarshal(buf, &diagnosis); err != nil {
		return
	}
	return
}

func (api *AdminAPI) ResetDataPartition(partitionID uint64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminResetDataPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) ManualResetDataPartition(partitionID uint64, nodeAddrs string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminManualResetDataPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addParam("addr", nodeAddrs)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) ResetCorruptDataNode(nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminResetCorruptDataNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DiagnoseMetaPartition() (diagnosis *proto.MetaPartitionDiagnosis, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminDiagnoseMetaPartition)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	diagnosis = &proto.MetaPartitionDiagnosis{}
	if err = json.Unmarshal(buf, &diagnosis); err != nil {
		return
	}
	return
}
func (api *AdminAPI) ResetMetaPartition(partitionID uint64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminResetMetaPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}
func (api *AdminAPI) ManualResetMetaPartition(partitionID uint64, nodeAddrs string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminManualResetMetaPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addParam("addr", nodeAddrs)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}
func (api *AdminAPI) ResetCorruptMetaNode(nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminResetCorruptMetaNode)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

//func (api *AdminAPI) LoadDataPartition(volName string, partitionID uint64) (err error) {
//	var request = newAPIRequest(http.MethodGet, proto.AdminLoadDataPartition)
//	request.addParam("id", strconv.Itoa(int(partitionID)))
//	request.addParam("name", volName)
//	if _, err = api.mc.serveRequest(request); err != nil {
//		return
//	}
//	return
//}

func (api *AdminAPI) CreateDataPartition(volName string, count int) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateDataPartition)
	request.addParam("name", volName)
	request.addParam("count", strconv.Itoa(count))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DecommissionDataPartition(dataPartitionID uint64, nodeAddr, destAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDecommissionDataPartition)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if len(strings.TrimSpace(destAddr)) != 0 {
		request.addParam("destAddr", destAddr)
	}
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DecommissionMetaPartition(metaPartitionID uint64, nodeAddr, destAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDecommissionMetaPartition)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if len(strings.TrimSpace(destAddr)) != 0 {
		request.addParam("destAddr", destAddr)
	}
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteDataReplica)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddDataReplica)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddDataLearner(dataPartitionID uint64, nodeAddr string, autoPromote bool, threshold uint8) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddDataReplicaLearner)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("auto", strconv.FormatBool(autoPromote))
	request.addParam("threshold", strconv.FormatUint(uint64(threshold), 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) PromoteDataLearner(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminPromoteDataReplicaLearner)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteMetaReplica)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddMetaReplica)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddMetaReplicaLearner(metaPartitionID uint64, nodeAddr string, autoPromote bool, threshold uint8) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddMetaReplicaLearner)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("auto", strconv.FormatBool(autoPromote))
	request.addParam("threshold", strconv.FormatUint(uint64(threshold), 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) PromoteMetaReplicaLearner(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminPromoteMetaReplicaLearner)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteVolume(volName, authKey string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) UpdateVolume(volName string, capacity uint64, replicas int, followerRead, authenticate, enableToken, autoRepair bool, authKey, zoneName string, bucketPolicy uint8) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminUpdateVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("replicaNum", strconv.Itoa(replicas))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
	request.addParam("authenticate", strconv.FormatBool(authenticate))
	request.addParam("enableToken", strconv.FormatBool(enableToken))
	request.addParam("autoRepair", strconv.FormatBool(autoRepair))
	request.addParam("zoneName", zoneName)
	request.addParam("bucketPolicy", strconv.Itoa(int(bucketPolicy)))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateVolume(volName, owner string, mpCount int,
	dpSize uint64, capacity uint64, replicas int, followerRead bool, autoRepair bool, volWriteMutex bool, zoneName string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateVol)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("mpCount", strconv.Itoa(mpCount))
	request.addParam("size", strconv.FormatUint(dpSize, 10))
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
	request.addParam("autoRepair", strconv.FormatBool(autoRepair))
	request.addParam("volWriteMutex", strconv.FormatBool(volWriteMutex))
	request.addParam("zoneName", zoneName)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateDefaultVolume(volName, owner string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateVol)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("capacity", "10")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetVolumeSimpleInfo(volName string) (vv *proto.SimpleVolView, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetVol)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	vv = &proto.SimpleVolView{}
	if err = json.Unmarshal(data, &vv); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetClusterInfo() (ci *proto.ClusterInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetIP)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	ci = &proto.ClusterInfo{}
	if err = json.Unmarshal(data, &ci); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetLimitInfo(volName string) (info *proto.LimitInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetLimitInfo)
	request.addParam("name", volName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	info = &proto.LimitInfo{}
	if err = json.Unmarshal(data, &info); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateMetaPartition(volName string, inodeStart uint64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateMetaPartition)
	request.addParam("name", volName)
	request.addParam("start", strconv.FormatUint(inodeStart, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminListVols)
	request.addParam("keywords", keywords)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	volsInfo = make([]*proto.VolInfo, 0)
	if err = json.Unmarshal(data, &volsInfo); err != nil {
		return
	}
	return
}

func (api *AdminAPI) IsFreezeCluster(isFreeze bool) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminClusterFreeze)
	request.addParam("enable", strconv.FormatBool(isFreeze))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetMetaNodeThreshold(threshold float64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminSetMetaNodeThreshold)
	request.addParam("threshold", strconv.FormatFloat(threshold, 'f', 6, 64))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetRateLimit(info *proto.RateLimitInfo) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminSetNodeInfo)
	if info.Opcode >= 0 {
		request.addParam("opcode", strconv.FormatInt(int64(info.Opcode), 10))
	}
	if info.ClientReadVolRate >= 0 {
		request.addParam("clientReadVolRate", strconv.FormatInt(info.ClientReadVolRate, 10))
	}
	if info.ClientWriteVolRate >= 0 {
		request.addParam("clientWriteVolRate", strconv.FormatInt(info.ClientWriteVolRate, 10))
	}
	if info.ClientVolOpRate >= -1 {
		request.addParam("clientVolOpRate", strconv.FormatInt(info.ClientVolOpRate, 10))
	}
	if info.MetaNodeReqRate >= 0 {
		request.addParam("metaNodeReqRate", strconv.FormatInt(info.MetaNodeReqRate, 10))
	}
	if info.MetaNodeReqOpRate >= 0 {
		request.addParam("metaNodeReqOpRate", strconv.FormatInt(info.MetaNodeReqOpRate, 10))
	}
	if info.DataNodeRepairTaskCount > 0 {
		request.addParam("dataNodeRepairTaskCount", strconv.FormatInt(info.DataNodeRepairTaskCount, 10))
	}
	if info.DataNodeReqRate >= 0 {
		request.addParam("dataNodeReqRate", strconv.FormatInt(info.DataNodeReqRate, 10))
	}
	if info.DataNodeReqOpRate >= 0 {
		request.addParam("dataNodeReqOpRate", strconv.FormatInt(info.DataNodeReqOpRate, 10))
	}
	if info.DataNodeReqVolPartRate >= 0 {
		request.addParam("dataNodeReqVolPartRate", strconv.FormatInt(info.DataNodeReqVolPartRate, 10))
	}
	if info.DataNodeReqVolOpPartRate >= 0 {
		request.addParam("dataNodeReqVolOpPartRate", strconv.FormatInt(info.DataNodeReqVolOpPartRate, 10))
	}
	request.addParam("volume", info.Volume)
	request.addParam("zoneName", info.ZoneName)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}
