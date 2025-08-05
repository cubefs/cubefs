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
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type AdminAPI struct {
	mc *MasterClient
	h  map[string]string // extra headers
}

func (api *AdminAPI) WithHeader(key, val string) *AdminAPI {
	return &AdminAPI{mc: api.mc, h: mergeHeader(api.h, key, val)}
}

func (api *AdminAPI) EncodingWith(encoding string) *AdminAPI {
	return api.WithHeader(headerAcceptEncoding, encoding)
}

func (api *AdminAPI) EncodingGzip() *AdminAPI {
	return api.EncodingWith(encodingGzip)
}

func (api *AdminAPI) GetOpLog(dimension string, volName string, addr string, dpId string, diskName string) (opv *proto.OpLogView, err error) {
	opv = &proto.OpLogView{}
	err = api.mc.requestWith(opv, newRequest(get, proto.AdminGetOpLog).Header(api.h).Param(
		anyParam{"opLogDimension", dimension},
		anyParam{"volName", volName},
		anyParam{"addr", addr},
		anyParam{"dpId", dpId},
		anyParam{"diskName", diskName},
	))
	return
}

func (api *AdminAPI) GetCluster(volStorageClass bool) (cv *proto.ClusterView, err error) {
	cv = &proto.ClusterView{}
	err = api.mc.requestWith(cv, newRequest(get, proto.AdminGetCluster).Header(api.h).
		addParam("volStorageClass", strconv.FormatBool(volStorageClass)))
	return
}

func (api *AdminAPI) GetClusterDataNodes() (nodes []proto.NodeView, err error) {
	nodes = []proto.NodeView{}
	err = api.mc.requestWith(&nodes, newRequest(get, proto.AdminGetClusterDataNodes).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterMetaNodes() (nodes []proto.NodeView, err error) {
	nodes = []proto.NodeView{}
	err = api.mc.requestWith(&nodes, newRequest(get, proto.AdminGetClusterMetaNodes).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterNodeInfo() (cn *proto.ClusterNodeInfo, err error) {
	cn = &proto.ClusterNodeInfo{}
	err = api.mc.requestWith(cn, newRequest(get, proto.AdminGetNodeInfo).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterIP() (cp *proto.ClusterIP, err error) {
	cp = &proto.ClusterIP{}
	err = api.mc.requestWith(cp, newRequest(get, proto.AdminGetIP).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterStat() (cs *proto.ClusterStatInfo, err error) {
	cs = &proto.ClusterStatInfo{}
	err = api.mc.requestWith(cs, newRequest(get, proto.AdminClusterStat).Header(api.h).NoTimeout())
	return
}

func (api *AdminAPI) ListZones() (zoneViews []*proto.ZoneView, err error) {
	zoneViews = make([]*proto.ZoneView, 0)
	err = api.mc.requestWith(&zoneViews, newRequest(get, proto.GetAllZones).Header(api.h))
	return
}

func (api *AdminAPI) ListNodeSets(zoneName string) (nodeSetStats []*proto.NodeSetStat, err error) {
	params := make([]anyParam, 0)
	if zoneName != "" {
		params = append(params, anyParam{"zoneName", zoneName})
	}
	nodeSetStats = make([]*proto.NodeSetStat, 0)
	err = api.mc.requestWith(&nodeSetStats, newRequest(get, proto.GetAllNodeSets).Header(api.h).Param(params...))
	return
}

func (api *AdminAPI) GetNodeSet(nodeSetId string) (nodeSetStatInfo *proto.NodeSetStatInfo, err error) {
	nodeSetStatInfo = &proto.NodeSetStatInfo{}
	err = api.mc.requestWith(nodeSetStatInfo, newRequest(get, proto.GetNodeSet).
		Header(api.h).addParam("nodesetId", nodeSetId))
	return
}

func (api *AdminAPI) UpdateNodeSet(nodeSetId string, dataNodeSelector string, metaNodeSelector string) (err error) {
	return api.mc.request(newRequest(get, proto.UpdateNodeSet).Header(api.h).Param(
		anyParam{"nodesetId", nodeSetId},
		anyParam{"dataNodeSelector", dataNodeSelector},
		anyParam{"metaNodeSelector", metaNodeSelector},
	))
}

func (api *AdminAPI) UpdateZone(name string, enable bool, dataNodesetSelector string, metaNodesetSelector string, dataNodeSelector string, metaNodeSelector string) (err error) {
	return api.mc.request(newRequest(post, proto.UpdateZone).Header(api.h).Param(
		anyParam{"name", name},
		anyParam{"enable", enable},
		anyParam{"dataNodesetSelector", dataNodesetSelector},
		anyParam{"metaNodesetSelector", metaNodesetSelector},
		anyParam{"dataNodeSelector", dataNodeSelector},
		anyParam{"metaNodeSelector", metaNodeSelector},
	))
}

func (api *AdminAPI) Topo() (topo *proto.TopologyView, err error) {
	topo = &proto.TopologyView{}
	err = api.mc.requestWith(topo, newRequest(get, proto.GetTopologyView).Header(api.h))
	return
}

func (api *AdminAPI) GetDataPartition(volName string, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	partition = &proto.DataPartitionInfo{}
	err = api.mc.requestWith(partition, newRequest(get, proto.AdminGetDataPartition).
		Header(api.h).Param(anyParam{"id", partitionID}, anyParam{"name", volName}))
	return
}

func (api *AdminAPI) GetDataPartitionById(partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	partition = &proto.DataPartitionInfo{}
	err = api.mc.requestWith(partition, newRequest(get, proto.AdminGetDataPartition).
		Header(api.h).addParamAny("id", partitionID))
	return
}

func (api *AdminAPI) DiagnoseDataPartition(ignoreDiscardDp bool) (diagnosis *proto.DataPartitionDiagnosis, err error) {
	diagnosis = &proto.DataPartitionDiagnosis{}
	err = api.mc.requestWith(diagnosis, newRequest(get, proto.AdminDiagnoseDataPartition).
		Header(api.h).addParamAny("ignoreDiscard", ignoreDiscardDp))
	return
}

func (api *AdminAPI) DiagnoseMetaPartition() (diagnosis *proto.MetaPartitionDiagnosis, err error) {
	diagnosis = &proto.MetaPartitionDiagnosis{}
	err = api.mc.requestWith(diagnosis, newRequest(get, proto.AdminDiagnoseMetaPartition).Header(api.h))
	return
}

func (api *AdminAPI) LoadDataPartition(volName string, partitionID uint64, clientIDKey string) (err error) {
	return api.mc.request(newRequest(get, proto.AdminLoadDataPartition).Header(api.h).Param(
		anyParam{"id", partitionID},
		anyParam{"name", volName},
		anyParam{"clientIDKey", clientIDKey},
	))
}

func (api *AdminAPI) CreateDataPartition(volName string, count int, clientIDKey string, mediaType uint32) (err error) {
	return api.mc.request(newRequest(get, proto.AdminCreateDataPartition).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"count", count},
		anyParam{"clientIDKey", clientIDKey},
		anyParam{"mediaType", mediaType},
	))
}

func (api *AdminAPI) DecommissionDataPartition(dataPartitionID uint64, nodeAddr string, raftForce bool, clientIDKey, decommissionType string) (err error) {
	request := newRequest(get, proto.AdminDecommissionDataPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("raftForceDel", strconv.FormatBool(raftForce))
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("decommissionType", decommissionType)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DecommissionMetaPartition(metaPartitionID uint64, nodeAddr, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminDecommissionMetaPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteDataReplica(dataPartitionID uint64, nodeAddr, clientIDKey string, raftForce bool) (err error) {
	request := newRequest(get, proto.AdminDeleteDataReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("raftForceDel", strconv.FormatBool(raftForce))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddDataReplica(dataPartitionID uint64, nodeAddr, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminAddDataReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteMetaReplica(metaPartitionID uint64, nodeAddr string, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminDeleteMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddMetaReplica(metaPartitionID uint64, nodeAddr string, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminAddMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) QueryDataPartitionDecommissionStatus(partitionId uint64) (info *proto.DecommissionDataPartitionInfo, err error) {
	request := newRequest(get, proto.AdminQueryDataPartitionDecommissionStatus).Header(api.h)
	request.addParam("id", strconv.FormatUint(partitionId, 10))
	info = &proto.DecommissionDataPartitionInfo{}
	err = api.mc.requestWith(info, request)
	return
}

func (api *AdminAPI) DeleteVolume(volName, authKey string) (err error) {
	request := newRequest(get, proto.AdminDeleteVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteVolumeWithAuthNode(volName, authKey, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminDeleteVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) UnDeleteVolume(volName, authKey string, status bool) (err error) {
	request := newRequest(get, proto.AdminDeleteVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("delete", strconv.FormatBool(false))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) UpdateVolume(
	vv *proto.SimpleVolView,
	txTimeout int64,
	txMask string,
	txForceReset bool,
	txConflictRetryNum int64,
	txConflictRetryInterval int64,
	txOpLimit int,
	clientIDKey string,
	optVolCapClass int,
) (err error) {
	request := newRequest(get, proto.AdminUpdateVol).Header(api.h)
	request.addParam("name", vv.Name)
	request.addParam("description", vv.Description)
	request.addParam("crossZone", strconv.FormatBool(vv.CrossZone))
	request.addParam("authKey", util.CalcAuthKey(vv.Owner))
	request.addParam("zoneName", vv.ZoneName)
	request.addParam("capacity", strconv.FormatUint(vv.Capacity, 10))
	request.addParam("followerRead", strconv.FormatBool(vv.FollowerRead))
	request.addParam(proto.MetaFollowerReadKey, strconv.FormatBool(vv.MetaFollowerRead))
	request.addParam(proto.VolEnableDirectRead, strconv.FormatBool(vv.DirectRead))
	request.addParam(proto.MaximallyReadKey, strconv.FormatBool(vv.MaximallyRead))
	request.addParam("ebsBlkSize", strconv.Itoa(vv.ObjBlockSize))
	request.addParam("cacheCap", strconv.FormatUint(vv.CacheCapacity, 10))
	request.addParam("cacheAction", strconv.Itoa(vv.CacheAction))
	request.addParam("cacheThreshold", strconv.Itoa(vv.CacheThreshold))
	request.addParam("cacheTTL", strconv.Itoa(vv.CacheTtl))
	request.addParam("cacheHighWater", strconv.Itoa(vv.CacheHighWater))
	request.addParam("cacheLowWater", strconv.Itoa(vv.CacheLowWater))
	request.addParam("cacheLRUInterval", strconv.Itoa(vv.CacheLruInterval))
	request.addParam("cacheRuleKey", vv.CacheRule)
	request.addParam("dpReadOnlyWhenVolFull", strconv.FormatBool(vv.DpReadOnlyWhenVolFull))
	request.addParam("replicaNum", strconv.FormatUint(uint64(vv.DpReplicaNum), 10))
	request.addParam("enableQuota", strconv.FormatBool(vv.EnableQuota))
	request.addParam("deleteLockTime", strconv.FormatInt(vv.DeleteLockTime, 10))
	request.addParam("autoDpMetaRepair", strconv.FormatBool(vv.EnableAutoDpMetaRepair))
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("interval", strconv.FormatInt(vv.TrashInterval, 10))
	request.addParam("trashInterval", strconv.FormatInt(vv.TrashInterval, 10))
	request.addParam("accessTimeValidInterval", strconv.FormatInt(vv.AccessTimeInterval, 10))
	request.addParam("enablePersistAccessTime", strconv.FormatBool(vv.EnablePersistAccessTime))
	request.addParam("volStorageClass", strconv.FormatUint(uint64(vv.VolStorageClass), 10))
	request.addParam("forbidWriteOpOfProtoVersion0", strconv.FormatBool(vv.ForbidWriteOpOfProtoVer0))
	request.addParam(proto.LeaderRetryTimeoutKey, strconv.FormatUint(uint64(vv.LeaderRetryTimeOut), 10))
	request.addParamAny("remoteCacheEnable", vv.RemoteCacheEnable)
	request.addParamAny("remoteCachePath", vv.RemoteCachePath)
	request.addParamAny("remoteCacheAutoPrepare", vv.RemoteCacheAutoPrepare)
	request.addParamAny("remoteCacheTTL", vv.RemoteCacheTTL)
	request.addParamAny("remoteCacheReadTimeoutSec", vv.RemoteCacheReadTimeoutSec)
	request.addParam("remoteCacheMaxFileSizeGB", strconv.FormatInt(vv.RemoteCacheMaxFileSizeGB, 10))
	request.addParamAny("remoteCacheOnlyForNotSSD", vv.RemoteCacheOnlyForNotSSD)
	request.addParamAny("remoteCacheFollowerRead", vv.RemoteCacheFollowerRead)

	if txMask != "" {
		request.addParam("enableTxMask", txMask)
		request.addParam("txForceReset", strconv.FormatBool(txForceReset))
	}
	if txTimeout > 0 {
		request.addParam("txTimeout", strconv.FormatInt(txTimeout, 10))
	}
	if txConflictRetryNum > 0 {
		request.addParam("txConflictRetryNum", strconv.FormatInt(txConflictRetryNum, 10))
	}
	if txOpLimit > 0 {
		request.addParam("txOpLimit", strconv.Itoa(txOpLimit))
	}
	if txConflictRetryInterval > 0 {
		request.addParam("txConflictRetryInterval", strconv.FormatInt(txConflictRetryInterval, 10))
	}
	if optVolCapClass > 0 {
		request.addParam("quotaClass", strconv.FormatInt(int64(optVolCapClass), 10))
		request.addParam("quotaOfStorageClass", strconv.FormatInt(int64(vv.QuotaOfStorageClass[0].QuotaGB), 10))
	}
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) PutDataPartitions(volName string, dpsView []byte) (err error) {
	return api.mc.request(newRequest(post, proto.AdminPutDataPartitions).
		Header(api.h).addParam("name", volName).Body(dpsView))
}

func (api *AdminAPI) VolShrink(volName string, capacity uint64, authKey, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminVolShrink).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) VolExpand(volName string, capacity uint64, authKey, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminVolExpand).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) VolAddAllowedStorageClass(volName string, addAllowedStorageClass uint32, ebsBlkSize int, authKey, clientIDKey string, force bool) (err error) {
	request := newRequest(http.MethodGet, proto.AdminVolAddAllowedStorageClass).Header(api.h)
	request.addParam("name", volName)
	request.addParam("allowedStorageClass", strconv.FormatUint(uint64(addAllowedStorageClass), 10))
	request.addParam("ebsBlkSize", strconv.Itoa(ebsBlkSize))
	request.addParam("authKey", authKey)
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("force", strconv.FormatBool(force))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateVolName(volName, owner string, capacity uint64, deleteLockTime int64, crossZone, normalZonesFirst bool,
	business string, mpCount, dpCount, replicaNum, dpSize int, followerRead bool, zoneName, cacheRuleKey string, ebsBlkSize,
	cacheCapacity, cacheAction, cacheThreshold, cacheTTL, cacheHighWater, cacheLowWater, cacheLRUInterval int,
	dpReadOnlyWhenVolFull bool, txMask string, txTimeout uint32, txConflictRetryNum int64, txConflictRetryInterval int64, optEnableQuota string,
	clientIDKey string, volStorageClass uint32, allowedStorageClass string, optMetaFollowerRead string, optMaximallyRead string,
	remoteCacheEnable string, remoteCacheAutoPrepare string, remoteCachePath string, remoteCacheTTL int64, remoteCacheReadTimeout int64,
	remoteCacheMaxFileSizeGB int64, remoteCacheOnlyForNotSSD string, remoteCacheFollowerRead string,
) (err error) {
	request := newRequest(get, proto.AdminCreateVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("deleteLockTime", strconv.FormatInt(deleteLockTime, 10))
	request.addParam("crossZone", strconv.FormatBool(crossZone))
	request.addParam("normalZonesFirst", strconv.FormatBool(normalZonesFirst))
	request.addParam("description", business)
	request.addParam("mpCount", strconv.Itoa(mpCount))
	request.addParam("dpCount", strconv.Itoa(dpCount))
	request.addParam("replicaNum", strconv.Itoa(replicaNum))
	request.addParam("dpSize", strconv.Itoa(dpSize))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
	request.addParam(proto.MetaFollowerReadKey, optMetaFollowerRead)
	request.addParam(proto.MaximallyReadKey, optMaximallyRead)
	request.addParam("zoneName", zoneName)
	request.addParam("cacheRuleKey", cacheRuleKey)
	request.addParam("ebsBlkSize", strconv.Itoa(ebsBlkSize))
	request.addParam("cacheCap", strconv.Itoa(cacheCapacity))
	request.addParam("cacheAction", strconv.Itoa(cacheAction))
	request.addParam("cacheThreshold", strconv.Itoa(cacheThreshold))
	request.addParam("cacheTTL", strconv.Itoa(cacheTTL))
	request.addParam("cacheHighWater", strconv.Itoa(cacheHighWater))
	request.addParam("cacheLowWater", strconv.Itoa(cacheLowWater))
	request.addParam("cacheLRUInterval", strconv.Itoa(cacheLRUInterval))
	request.addParam("dpReadOnlyWhenVolFull", strconv.FormatBool(dpReadOnlyWhenVolFull))
	request.addParam("enableQuota", optEnableQuota)
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("volStorageClass", strconv.FormatUint(uint64(volStorageClass), 10))
	request.addParam("allowedStorageClass", allowedStorageClass)
	request.addParam("remoteCacheEnable", remoteCacheEnable)
	request.addParam("remoteCacheAutoPrepare", remoteCacheAutoPrepare)
	request.addParam("remoteCachePath", remoteCachePath)
	request.addParam("remoteCacheTTL", strconv.FormatInt(remoteCacheTTL, 10))
	request.addParam("remoteCacheReadTimeout", strconv.FormatInt(remoteCacheReadTimeout, 10))
	request.addParam("remoteCacheMaxFileSizeGB", strconv.FormatInt(remoteCacheMaxFileSizeGB, 10))
	request.addParam("remoteCacheOnlyForNotSSD", remoteCacheOnlyForNotSSD)
	request.addParam("remoteCacheFollowerRead", remoteCacheFollowerRead)

	if txMask != "" {
		request.addParam("enableTxMask", txMask)
	}
	if txTimeout > 0 {
		request.addParam("txTimeout", strconv.FormatUint(uint64(txTimeout), 10))
	}
	if txConflictRetryNum > 0 {
		request.addParam("txConflictRetryNum", strconv.FormatInt(txConflictRetryNum, 10))
	}
	if txConflictRetryInterval > 0 {
		request.addParam("txConflictRetryInterval", strconv.FormatInt(txConflictRetryInterval, 10))
	}
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) CreateDefaultVolume(volName, owner string) (err error) {
	request := newRequest(get, proto.AdminCreateVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("capacity", "10")
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetVolumeSimpleInfo(volName string) (vv *proto.SimpleVolView, err error) {
	vv = &proto.SimpleVolView{}
	err = api.mc.requestWith(vv, newRequest(get, proto.AdminGetVol).Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) SetVolumeForbidden(volName string, forbidden bool) (err error) {
	request := newRequest(post, proto.AdminVolForbidden).Header(api.h)
	request.addParam("name", volName)
	request.addParam("forbidden", strconv.FormatBool(forbidden))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetVolumeAuditLog(volName string, enable bool) (err error) {
	request := newRequest(post, proto.AdminVolEnableAuditLog).Header(api.h)
	request.addParam("name", volName)
	request.addParam("enable", strconv.FormatBool(enable))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetVolumeDpRepairBlockSize(volName string, repairSize uint64) (err error) {
	request := newRequest(post, proto.AdminVolSetDpRepairBlockSize).Header(api.h)
	request.addParam("name", volName)
	request.addParam("dpRepairBlockSize", strconv.FormatUint(repairSize, 10))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetMonitorPushAddr() (addr string, err error) {
	err = api.mc.requestWith(&addr, newRequest(get, proto.AdminGetMonitorPushAddr).Header(api.h))
	return
}

func (api *AdminAPI) UploadFlowInfo(volName string, flowInfo *proto.ClientReportLimitInfo) (vv *proto.LimitRsp2Client, err error) {
	if flowInfo == nil {
		return nil, fmt.Errorf("flowinfo is nil")
	}
	vv = &proto.LimitRsp2Client{}
	err = api.mc.requestWith(vv, newRequest(get, proto.QosUpload).Header(api.h).Body(flowInfo).
		Param(anyParam{"name", volName}, anyParam{"qosEnable", "true"}))
	log.LogInfof("action[UploadFlowInfo] enable %v", vv.Enable)
	return
}

func (api *AdminAPI) GetVolumeSimpleInfoWithFlowInfo(volName string) (vv *proto.SimpleVolView, err error) {
	vv = &proto.SimpleVolView{}
	err = api.mc.requestWith(vv, newRequest(get, proto.AdminGetVol).
		Header(api.h).Param(anyParam{"name", volName}, anyParam{"init", "true"}))
	return
}

// access control list
func (api *AdminAPI) CheckACL() (ci *proto.ClusterInfo, err error) {
	ci = &proto.ClusterInfo{}
	err = api.mc.requestWith(ci, newRequest(get, proto.AdminACL).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterInfo() (ci *proto.ClusterInfo, err error) {
	ci = &proto.ClusterInfo{}
	err = api.mc.requestWith(ci, newRequest(get, proto.AdminGetIP).Header(api.h))
	return
}

func (api *AdminAPI) GetVerInfo(volName string) (ci *proto.VolumeVerInfo, err error) {
	ci = &proto.VolumeVerInfo{}
	err = api.mc.requestWith(ci, newRequest(get, proto.AdminGetVolVer).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) CreateMetaPartition(volName string, count int, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminCreateMetaPartition).Header(api.h)
	request.addParam("name", volName)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	volsInfo = make([]*proto.VolInfo, 0)
	err = api.mc.requestWith(&volsInfo, newRequest(get, proto.AdminListVols).
		Header(api.h).addParam("keywords", keywords))
	return
}

func (api *AdminAPI) IsFreezeCluster(isFreeze bool, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminClusterFreeze).Header(api.h)
	request.addParam("enable", strconv.FormatBool(isFreeze))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetForbidMpDecommission(disable bool) (err error) {
	request := newRequest(get, proto.AdminClusterForbidMpDecommission).Header(api.h)
	request.addParam("enable", strconv.FormatBool(disable))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetMetaNodeThreshold(threshold float64, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminSetMetaNodeThreshold).Header(api.h)
	request.addParam("threshold", strconv.FormatFloat(threshold, 'f', 6, 64))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetMasterVolDeletionDelayTime(volDeletionDelayTimeHour int) (err error) {
	request := newRequest(get, proto.AdminSetMasterVolDeletionDelayTime)
	request.addParam("volDeletionDelayTime", strconv.FormatInt(int64(volDeletionDelayTimeHour), 10))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetClusterParas(batchCount, markDeleteRate, deleteWorkerSleepMs, autoRepairRate, loadFactor, maxDpCntLimit, maxMpCntLimit, clientIDKey string,
	enableAutoDecommissionDisk string, autoDecommissionDiskInterval string,
	enableAutoDpMetaRepair string, autoDpMetaRepairParallelCnt string,
	dpRepairTimeout string, dpTimeout string, dpBackupTimeout string,
	decommissionDpLimit, decommissionDiskLimit, forbidWriteOpOfProtoVersion0 string, mediaType string,
) (err error) {
	request := newRequest(get, proto.AdminSetNodeInfo).Header(api.h)
	request.addParam("batchCount", batchCount)
	request.addParam("markDeleteRate", markDeleteRate)
	request.addParam("deleteWorkerSleepMs", deleteWorkerSleepMs)
	request.addParam("autoRepairRate", autoRepairRate)
	request.addParam("loadFactor", loadFactor)
	request.addParam("maxDpCntLimit", maxDpCntLimit)
	request.addParam("maxMpCntLimit", maxMpCntLimit)
	request.addParam("clientIDKey", clientIDKey)

	// request.addParam("dataNodesetSelector", dataNodesetSelector)
	// request.addParam("metaNodesetSelector", metaNodesetSelector)
	// request.addParam("dataNodeSelector", dataNodeSelector)
	// request.addParam("metaNodeSelector", metaNodeSelector)
	// if markDiskBrokenThreshold != "" {
	//	request.addParam("markDiskBrokenThreshold", markDiskBrokenThreshold)
	// }
	if enableAutoDecommissionDisk != "" {
		request.addParam("autoDecommissionDisk", enableAutoDecommissionDisk)
	}
	if autoDecommissionDiskInterval != "" {
		request.addParam("autoDecommissionDiskInterval", autoDecommissionDiskInterval)
	}
	if enableAutoDpMetaRepair != "" {
		request.addParam("autoDpMetaRepair", enableAutoDpMetaRepair)
	}
	if autoDpMetaRepairParallelCnt != "" {
		request.addParam("autoDpMetaRepairParallelCnt", autoDpMetaRepairParallelCnt)
	}
	if dpRepairTimeout != "" {
		request.addParam("dpRepairTimeOut", dpRepairTimeout)
	}
	if dpTimeout != "" {
		request.addParam("dpTimeout", dpTimeout)
	}
	if dpBackupTimeout != "" {
		request.addParam("dpBackupTimeout", dpBackupTimeout)
	}
	if decommissionDpLimit != "" {
		request.addParam("decommissionLimit", decommissionDpLimit)
	}
	if decommissionDiskLimit != "" {
		request.addParam("decommissionDiskLimit", decommissionDiskLimit)
	}
	if forbidWriteOpOfProtoVersion0 != "" {
		request.addParam("forbidWriteOpOfProtoVersion0", forbidWriteOpOfProtoVersion0)
	}
	if mediaType != "" {
		request.addParam("dataMediaType", mediaType)
	}
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetClusterParas() (delParas map[string]string, err error) {
	request := newRequest(get, proto.AdminGetNodeInfo).Header(api.h)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	delParas = make(map[string]string)
	err = api.mc.requestWith(&delParas, newRequest(get, proto.AdminGetNodeInfo).Header(api.h))
	return
}

func (api *AdminAPI) CreatePreLoadDataPartition(volName string, count int, capacity, ttl uint64, zongs string) (view *proto.DataPartitionsView, err error) {
	view = &proto.DataPartitionsView{}
	err = api.mc.requestWith(view, newRequest(get, proto.AdminCreatePreLoadDataPartition).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"replicaNum", count},
		anyParam{"capacity", capacity},
		anyParam{"cacheTTL", ttl},
		anyParam{"zoneName", zongs},
	))
	return
}

func (api *AdminAPI) ListQuota(volName string) (quotaInfo []*proto.QuotaInfo, err error) {
	resp := &proto.ListMasterQuotaResponse{}
	if err = api.mc.requestWith(resp, newRequest(get, proto.QuotaList).
		Header(api.h).addParam("name", volName)); err != nil {
		log.LogErrorf("action[ListQuota] fail. %v", err)
		return
	}
	quotaInfo = resp.Quotas
	log.LogInfof("action[ListQuota] success.")
	return quotaInfo, err
}

func (api *AdminAPI) CreateQuota(volName string, quotaPathInfos []proto.QuotaPathInfo, maxFiles uint64, maxBytes uint64) (quotaId uint32, err error) {
	if err = api.mc.requestWith(&quotaId, newRequest(get, proto.QuotaCreate).
		Header(api.h).Body(&quotaPathInfos).Param(
		anyParam{"name", volName},
		anyParam{"maxFiles", maxFiles},
		anyParam{"maxBytes", maxBytes})); err != nil {
		log.LogErrorf("action[CreateQuota] fail. %v", err)
		return
	}
	log.LogInfof("action[CreateQuota] success.")
	return
}

func (api *AdminAPI) UpdateQuota(volName string, quotaId string, maxFiles uint64, maxBytes uint64) (err error) {
	request := newRequest(get, proto.QuotaUpdate).Header(api.h)
	request.addParam("name", volName)
	request.addParam("quotaId", quotaId)
	request.addParam("maxFiles", strconv.FormatUint(maxFiles, 10))
	request.addParam("maxBytes", strconv.FormatUint(maxBytes, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		log.LogErrorf("action[UpdateQuota] fail. %v", err)
		return
	}
	log.LogInfof("action[UpdateQuota] success.")
	return nil
}

func (api *AdminAPI) DeleteQuota(volName string, quotaId string) (err error) {
	request := newRequest(get, proto.QuotaDelete).Header(api.h)
	request.addParam("name", volName)
	request.addParam("quotaId", quotaId)
	if _, err = api.mc.serveRequest(request); err != nil {
		log.LogErrorf("action[DeleteQuota] fail. %v", err)
		return
	}
	log.LogInfo("action[DeleteQuota] success.")
	return nil
}

func (api *AdminAPI) GetQuota(volName string, quotaId string) (quotaInfo *proto.QuotaInfo, err error) {
	info := &proto.QuotaInfo{}
	if err = api.mc.requestWith(info, newRequest(get, proto.QuotaGet).Header(api.h).
		Param(anyParam{"name", volName}, anyParam{"quotaId", quotaId})); err != nil {
		log.LogErrorf("action[GetQuota] fail. %v", err)
		return
	}
	quotaInfo = info
	log.LogInfof("action[GetQuota] %v success.", *quotaInfo)
	return quotaInfo, err
}

func (api *AdminAPI) QueryBadDisks() (badDisks *proto.BadDiskInfos, err error) {
	badDisks = &proto.BadDiskInfos{}
	err = api.mc.requestWith(badDisks, newRequest(get, proto.QueryBadDisks).Header(api.h))
	return
}

func (api *AdminAPI) QueryDisks(addr string) (disks *proto.DiskInfos, err error) {
	disks = &proto.DiskInfos{}
	err = api.mc.requestWith(disks, newRequest(get, proto.QueryDisks).Header(api.h).
		addParam("addr", addr))
	return
}

func (api *AdminAPI) DiskDetail(addr string, diskPath string) (disk *proto.DiskInfo, err error) {
	disk = &proto.DiskInfo{}
	err = api.mc.requestWith(disk, newRequest(get, proto.QueryDiskDetail).Header(api.h).
		addParam("addr", addr).addParam("disk", diskPath))
	return
}

func (api *AdminAPI) DecommissionDisk(addr string, disk string) (err error) {
	return api.mc.request(newRequest(post, proto.DecommissionDisk).Header(api.h).
		addParam("addr", addr).addParam("disk", disk).addParam("decommissionType", "1"))
}

func (api *AdminAPI) RecommissionDisk(addr string, disk string) (err error) {
	return api.mc.request(newRequest(post, proto.RecommissionDisk).Header(api.h).
		addParam("addr", addr).addParam("disk", disk))
}

func (api *AdminAPI) QueryDecommissionDiskProgress(addr string, disk string) (progress *proto.DecommissionProgress, err error) {
	progress = &proto.DecommissionProgress{}
	err = api.mc.requestWith(progress, newRequest(post, proto.QueryDiskDecoProgress).
		Header(api.h).Param(anyParam{"addr", addr}, anyParam{"disk", disk}))
	return
}

func (api *AdminAPI) ListQuotaAll() (volsInfo []*proto.VolInfo, err error) {
	volsInfo = make([]*proto.VolInfo, 0)
	err = api.mc.requestWith(&volsInfo, newRequest(get, proto.QuotaListAll).Header(api.h))
	return
}

func (api *AdminAPI) GetDiscardDataPartition() (discardDpInfos *proto.DiscardDataPartitionInfos, err error) {
	discardDpInfos = &proto.DiscardDataPartitionInfos{}
	err = api.mc.requestWith(&discardDpInfos, newRequest(get, proto.AdminGetDiscardDp).Header(api.h))
	return
}

func (api *AdminAPI) SetDataPartitionDiscard(partitionId uint64, discard bool, force bool) (err error) {
	request := newRequest(post, proto.AdminSetDpDiscard).
		Header(api.h).
		addParam("id", strconv.FormatUint(partitionId, 10)).
		addParam("dpDiscard", strconv.FormatBool(discard)).
		addParam("force", strconv.FormatBool(force))
	if err = api.mc.request(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteVersion(volName string, verSeq string) (err error) {
	request := newRequest(get, proto.AdminDelVersion).Header(api.h)
	request.addParam("name", volName)
	request.addParam("verSeq", verSeq)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetStrategy(volName string, periodic string, count string, enable string, force string) (err error) {
	request := newRequest(get, proto.AdminSetVerStrategy).Header(api.h)
	request.addParam("name", volName)
	request.addParam("periodic", periodic)
	request.addParam("count", count)
	request.addParam("enable", enable)
	request.addParam("force", force)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) CreateVersion(volName string) (ver *proto.VolVersionInfo, err error) {
	ver = &proto.VolVersionInfo{}
	err = api.mc.requestWith(ver, newRequest(get, proto.AdminCreateVersion).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) GetLatestVer(volName string) (ver *proto.VolVersionInfo, err error) {
	ver = &proto.VolVersionInfo{}
	err = api.mc.requestWith(ver, newRequest(get, proto.AdminGetVersionInfo).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) GetVerList(volName string) (verList *proto.VolVersionInfoList, err error) {
	verList = &proto.VolVersionInfoList{}
	err = api.mc.requestWith(verList, newRequest(get, proto.AdminGetAllVersionInfo).
		Header(api.h).addParam("name", volName))
	log.LogDebugf("GetVerList. vol %v verList %v", volName, verList)
	for _, info := range verList.VerList {
		log.LogDebugf("GetVerList. vol %v verList %v", volName, info)
	}
	return
}

func (api *AdminAPI) SetBucketLifecycle(req *proto.LcConfiguration) (err error) {
	return api.mc.request(newRequest(post, proto.SetBucketLifecycle).Header(api.h).Body(req))
}

func (api *AdminAPI) GetBucketLifecycle(volume string) (lcConf *proto.LcConfiguration, err error) {
	lcConf = &proto.LcConfiguration{}
	err = api.mc.requestWith(lcConf, newRequest(get, proto.GetBucketLifecycle).
		Header(api.h).addParam("name", volume))
	return
}

func (api *AdminAPI) DelBucketLifecycle(volume string) (err error) {
	request := newRequest(get, proto.DeleteBucketLifecycle).Header(api.h)
	request.addParam("name", volume)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetS3QoSInfo() (data []byte, err error) {
	return api.mc.serveRequest(newRequest(get, proto.S3QoSGet).Header(api.h))
}

func (api *AdminAPI) SetAutoDecommissionDisk(enable bool) (err error) {
	request := newRequest(post, proto.AdminEnableAutoDecommissionDisk)
	request.addParam("enable", strconv.FormatBool(enable))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) QueryDecommissionFailedDisk(decommType int) (diskInfo []*proto.DecommissionFailedDiskInfo, err error) {
	request := newRequest(get, proto.AdminQueryDecommissionFailedDisk)
	request.addParam("decommissionType", strconv.FormatInt(int64(decommType), 10))

	diskInfo = make([]*proto.DecommissionFailedDiskInfo, 0)
	err = api.mc.requestWith(&diskInfo, request)
	return
}

func (api *AdminAPI) AbortDiskDecommission(addr string, disk string) (err error) {
	request := newRequest(post, proto.CancelDecommissionDisk)
	request.addParam("addr", addr)
	request.addParam("disk", disk)

	err = api.mc.request(request)
	return
}

func (api *AdminAPI) SetClusterDecommissionLimit(limit int32) (err error) {
	request := newAPIRequest(http.MethodPost, proto.AdminUpdateDecommissionLimit)
	request.addParam("decommissionLimit", strconv.FormatInt(int64(limit), 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) QueryDecommissionToken() (status []proto.DecommissionTokenStatus, err error) {
	var buf []byte
	request := newAPIRequest(http.MethodGet, proto.AdminQueryDecommissionToken)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	status = make([]proto.DecommissionTokenStatus, 0)
	if err = json.Unmarshal(buf, &status); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetVolTrashInterval(volName string, authKey string, interval time.Duration) (err error) {
	request := newAPIRequest(http.MethodPost, proto.AdminSetTrashInterval)
	request.addParam("name", volName)
	request.addParam("trashInterval", strconv.FormatInt(int64(interval.Minutes()), 10))
	request.addParam("authKey", authKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetDecommissionDiskLimit(limit uint32) (err error) {
	request := newRequest(post, proto.AdminUpdateDecommissionDiskLimit)
	request.addParam("decommissionDiskLimit", strconv.FormatUint(uint64(limit), 10))

	err = api.mc.request(request)
	return
}

func (api *AdminAPI) ResetDataPartitionRestoreStatus(dpId uint64) (ok bool, err error) {
	request := newRequest(post, proto.AdminResetDataPartitionRestoreStatus)
	request.addParam("id", strconv.FormatUint(dpId, 10))

	err = api.mc.requestWith(&ok, request)
	return
}

func (api *AdminAPI) GetUpgradeCompatibleSettings() (upgradeCompatibleSettings *proto.UpgradeCompatibleSettings, err error) {
	upgradeCompatibleSettings = &proto.UpgradeCompatibleSettings{}
	err = api.mc.requestWith(upgradeCompatibleSettings, newRequest(get, proto.AdminGetUpgradeCompatibleSettings).Header(api.h))
	return
}

func (api *AdminAPI) ChangeMasterLeader(leaderAddr string) (err error) {
	req := newRequest(get, proto.AdminChangeMasterLeader).Header(api.h)
	_, err = api.mc.requestOnce(req, leaderAddr)
	return
}

func (api *AdminAPI) TurnFlashGroup(enable bool) (result string, err error) {
	request := newRequest(post, proto.AdminFlashGroupTurn).Header(api.h).addParamAny("enable", enable)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *AdminAPI) CreateFlashGroup(slots string, weight int) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupCreate).
		Header(api.h).Param(anyParam{"slots", slots}, anyParam{"weight", weight}))
	return
}

func (api *AdminAPI) SetFlashGroup(flashGroupID uint64, isActive bool) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupSet).
		Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"enable", isActive}))
	return
}

func (api *AdminAPI) RemoveFlashGroup(flashGroupID uint64) (result string, err error) {
	request := newRequest(post, proto.AdminFlashGroupRemove).Header(api.h).addParamAny("id", flashGroupID)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *AdminAPI) flashGroupFlashNodes(uri string, flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, uri).Header(api.h).Param(
		anyParam{"id", flashGroupID}, anyParam{"count", count}, anyParam{"zoneName", zoneName}, anyParam{"addr", addr}))
	return
}

func (api *AdminAPI) FlashGroupAddFlashNode(flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodes(proto.AdminFlashGroupNodeAdd, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) FlashGroupRemoveFlashNode(flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodes(proto.AdminFlashGroupNodeRemove, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) GetFlashGroup(flashGroupID uint64) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupGet).
		Header(api.h).addParamAny("id", flashGroupID))
	return
}

func (api *AdminAPI) ListFlashGroup(isActive bool) (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).
		Header(api.h).Param(anyParam{"enable", isActive}))
	return
}

func (api *AdminAPI) ListFlashGroups() (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).Header(api.h))
	return
}

func (api *AdminAPI) ClientFlashGroups() (fgView proto.FlashGroupView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.ClientFlashGroups).Header(api.h))
	return
}

func (api *AdminAPI) CreateBalanceTask() (task *proto.ClusterPlan, err error) {
	task = &proto.ClusterPlan{
		Low:  make(map[string]*proto.ZonePressureView, 0),
		Plan: make([]*proto.MetaPartitionPlan, 0),
	}
	err = api.mc.requestWith(task, newRequest(get, proto.CreateBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) GetBalanceTask() (task *proto.ClusterPlan, err error) {
	task = &proto.ClusterPlan{
		Low:  make(map[string]*proto.ZonePressureView, 0),
		Plan: make([]*proto.MetaPartitionPlan, 0),
	}
	err = api.mc.requestWith(task, newRequest(get, proto.GetBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) RunBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.RunBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) StopBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.StopBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) DeleteBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.DeleteBalanceTask).Header(api.h))
	return
}
