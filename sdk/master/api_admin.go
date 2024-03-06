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
	"fmt"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
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

func (api *AdminAPI) GetCluster(ctx context.Context) (cv *proto.ClusterView, err error) {
	cv = &proto.ClusterView{}
	ctx = proto.ContextWithOperation(ctx, "GetCluster")
	err = api.mc.requestWith(cv, newRequest(ctx, get, proto.AdminGetCluster).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterNodeInfo(ctx context.Context) (cn *proto.ClusterNodeInfo, err error) {
	cn = &proto.ClusterNodeInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetClusterNodeInfo")
	err = api.mc.requestWith(cn, newRequest(ctx, get, proto.AdminGetNodeInfo).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterIP(ctx context.Context) (cp *proto.ClusterIP, err error) {
	cp = &proto.ClusterIP{}
	ctx = proto.ContextWithOperation(ctx, "GetClusterIP")
	err = api.mc.requestWith(cp, newRequest(ctx, get, proto.AdminGetIP).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterStat(ctx context.Context) (cs *proto.ClusterStatInfo, err error) {
	cs = &proto.ClusterStatInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetClusterStat")
	err = api.mc.requestWith(cs, newRequest(ctx, get, proto.AdminClusterStat).Header(api.h).NoTimeout())
	return
}

func (api *AdminAPI) ListZones(ctx context.Context) (zoneViews []*proto.ZoneView, err error) {
	zoneViews = make([]*proto.ZoneView, 0)
	ctx = proto.ContextWithOperation(ctx, "ListZones")
	err = api.mc.requestWith(&zoneViews, newRequest(ctx, get, proto.GetAllZones).Header(api.h))
	return
}

func (api *AdminAPI) ListNodeSets(ctx context.Context, zoneName string) (nodeSetStats []*proto.NodeSetStat, err error) {
	params := make([]anyParam, 0)
	if zoneName != "" {
		params = append(params, anyParam{"zoneName", zoneName})
	}
	nodeSetStats = make([]*proto.NodeSetStat, 0)
	ctx = proto.ContextWithOperation(ctx, "ListNodeSets")
	err = api.mc.requestWith(&nodeSetStats, newRequest(ctx, get, proto.GetAllNodeSets).Header(api.h).Param(params...))
	return
}

func (api *AdminAPI) GetNodeSet(ctx context.Context, nodeSetId string) (nodeSetStatInfo *proto.NodeSetStatInfo, err error) {
	nodeSetStatInfo = &proto.NodeSetStatInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetNodeSet")
	err = api.mc.requestWith(nodeSetStatInfo, newRequest(ctx, get, proto.GetNodeSet).
		Header(api.h).addParam("nodesetId", nodeSetId))
	return
}

func (api *AdminAPI) UpdateNodeSet(ctx context.Context, nodeSetId string, dataNodeSelector string, metaNodeSelector string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "UpdateNodeSet")
	return api.mc.request(newRequest(ctx, get, proto.UpdateNodeSet).Header(api.h).Param(
		anyParam{"nodesetId", nodeSetId},
		anyParam{"dataNodeSelector", dataNodeSelector},
		anyParam{"metaNodeSelector", metaNodeSelector},
	))
}

func (api *AdminAPI) UpdateZone(ctx context.Context, name string, enable bool, dataNodesetSelector string, metaNodesetSelector string, dataNodeSelector string, metaNodeSelector string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "UpdateZone")
	return api.mc.request(newRequest(ctx, post, proto.UpdateZone).Header(api.h).Param(
		anyParam{"name", name},
		anyParam{"enable", enable},
		anyParam{"dataNodesetSelector", dataNodesetSelector},
		anyParam{"metaNodesetSelector", metaNodesetSelector},
		anyParam{"dataNodeSelector", dataNodeSelector},
		anyParam{"metaNodeSelector", metaNodeSelector},
	))
}

func (api *AdminAPI) Topo(ctx context.Context) (topo *proto.TopologyView, err error) {
	topo = &proto.TopologyView{}
	ctx = proto.ContextWithOperation(ctx, "Topo")
	err = api.mc.requestWith(topo, newRequest(ctx, get, proto.GetTopologyView).Header(api.h))
	return
}

func (api *AdminAPI) GetDataPartition(ctx context.Context, volName string, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	partition = &proto.DataPartitionInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetDataPartition")
	err = api.mc.requestWith(partition, newRequest(ctx, get, proto.AdminGetDataPartition).
		Header(api.h).Param(anyParam{"id", partitionID}, anyParam{"name", volName}))
	return
}

func (api *AdminAPI) GetDataPartitionById(ctx context.Context, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	partition = &proto.DataPartitionInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetDataPartitionById")
	err = api.mc.requestWith(partition, newRequest(ctx, get, proto.AdminGetDataPartition).
		Header(api.h).addParamAny("id", partitionID))
	return
}

func (api *AdminAPI) DiagnoseDataPartition(ctx context.Context, ignoreDiscardDp bool) (diagnosis *proto.DataPartitionDiagnosis, err error) {
	diagnosis = &proto.DataPartitionDiagnosis{}
	ctx = proto.ContextWithOperation(ctx, "DiagnoseDataPartition")
	err = api.mc.requestWith(diagnosis, newRequest(ctx, get, proto.AdminDiagnoseDataPartition).
		Header(api.h).addParamAny("ignoreDiscard", ignoreDiscardDp))
	return
}

func (api *AdminAPI) DiagnoseMetaPartition(ctx context.Context) (diagnosis *proto.MetaPartitionDiagnosis, err error) {
	diagnosis = &proto.MetaPartitionDiagnosis{}
	ctx = proto.ContextWithOperation(ctx, "DiagnoseMetaPartition")
	err = api.mc.requestWith(diagnosis, newRequest(ctx, get, proto.AdminDiagnoseMetaPartition).Header(api.h))
	return
}

func (api *AdminAPI) LoadDataPartition(ctx context.Context, volName string, partitionID uint64, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "LoadDataPartition")
	return api.mc.request(newRequest(ctx, get, proto.AdminLoadDataPartition).Header(api.h).Param(
		anyParam{"id", partitionID},
		anyParam{"name", volName},
		anyParam{"clientIDKey", clientIDKey},
	))
}

func (api *AdminAPI) CreateDataPartition(ctx context.Context, volName string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "CreateDataPartition")
	return api.mc.request(newRequest(ctx, get, proto.AdminCreateDataPartition).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"count", count},
		anyParam{"clientIDKey", clientIDKey},
	))
}

func (api *AdminAPI) DecommissionDataPartition(ctx context.Context, dataPartitionID uint64, nodeAddr string, raftForce bool, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DecommissionDataPartition")
	request := newRequest(ctx, get, proto.AdminDecommissionDataPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("raftForceDel", strconv.FormatBool(raftForce))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DecommissionMetaPartition(ctx context.Context, metaPartitionID uint64, nodeAddr, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DecommissionMetaPartition")
	request := newRequest(ctx, get, proto.AdminDecommissionMetaPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteDataReplica(ctx context.Context, dataPartitionID uint64, nodeAddr, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteDataReplica")
	request := newRequest(ctx, get, proto.AdminDeleteDataReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddDataReplica(ctx context.Context, dataPartitionID uint64, nodeAddr, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "AddDataReplica")
	request := newRequest(ctx, get, proto.AdminAddDataReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteMetaReplica(ctx context.Context, metaPartitionID uint64, nodeAddr string, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteMetaReplica")
	request := newRequest(ctx, get, proto.AdminDeleteMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddMetaReplica(ctx context.Context, metaPartitionID uint64, nodeAddr string, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "AddMetaReplica")
	request := newRequest(ctx, get, proto.AdminAddMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteVolume(ctx context.Context, volName, authKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteVolume")
	request := newRequest(ctx, get, proto.AdminDeleteVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DeleteVolumeWithAuthNode(ctx context.Context, volName, authKey, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteVolumeWithAuthNode")
	request := newRequest(ctx, get, proto.AdminDeleteVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) UpdateVolume(
	ctx context.Context,
	vv *proto.SimpleVolView,
	txTimeout int64,
	txMask string,
	txForceReset bool,
	txConflictRetryNum int64,
	txConflictRetryInterval int64,
	txOpLimit int,
	clientIDKey string,
) (err error) {
	ctx = proto.ContextWithOperation(ctx, "UpdateVolume")
	request := newRequest(ctx, get, proto.AdminUpdateVol).Header(api.h)
	request.addParam("name", vv.Name)
	request.addParam("description", vv.Description)
	request.addParam("authKey", util.CalcAuthKey(vv.Owner))
	request.addParam("zoneName", vv.ZoneName)
	request.addParam("capacity", strconv.FormatUint(vv.Capacity, 10))
	request.addParam("followerRead", strconv.FormatBool(vv.FollowerRead))
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
	request.addParam("clientIDKey", clientIDKey)
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
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) PutDataPartitions(ctx context.Context, volName string, dpsView []byte) (err error) {
	ctx = proto.ContextWithOperation(ctx, "PutDataPartitions")
	return api.mc.request(newRequest(ctx, post, proto.AdminPutDataPartitions).
		Header(api.h).addParam("name", volName).Body(dpsView))
}

func (api *AdminAPI) VolShrink(ctx context.Context, volName string, capacity uint64, authKey, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "VolShrink")
	request := newRequest(ctx, get, proto.AdminVolShrink).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) VolExpand(ctx context.Context, volName string, capacity uint64, authKey, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "VolExpand")
	request := newRequest(ctx, get, proto.AdminVolExpand).Header(api.h)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) CreateVolName(ctx context.Context, volName, owner string, capacity uint64, deleteLockTime int64, crossZone, normalZonesFirst bool, business string,
	mpCount, dpCount, replicaNum, dpSize, volType int, followerRead bool, zoneName, cacheRuleKey string, ebsBlkSize,
	cacheCapacity, cacheAction, cacheThreshold, cacheTTL, cacheHighWater, cacheLowWater, cacheLRUInterval int,
	dpReadOnlyWhenVolFull bool, txMask string, txTimeout uint32, txConflictRetryNum int64, txConflictRetryInterval int64, optEnableQuota string,
	clientIDKey string,
) (err error) {
	ctx = proto.ContextWithOperation(ctx, "CreateVolName")
	request := newRequest(ctx, get, proto.AdminCreateVol).Header(api.h)
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
	request.addParam("volType", strconv.Itoa(volType))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
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

func (api *AdminAPI) CreateDefaultVolume(ctx context.Context, volName, owner string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "CreateDefaultVolume")
	request := newRequest(ctx, get, proto.AdminCreateVol).Header(api.h)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("capacity", "10")
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetVolumeSimpleInfo(ctx context.Context, volName string) (vv *proto.SimpleVolView, err error) {
	vv = &proto.SimpleVolView{}
	ctx = proto.ContextWithOperation(ctx, "GetVolumeSimpleInfo")
	err = api.mc.requestWith(vv, newRequest(ctx, get, proto.AdminGetVol).Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) SetVolumeForbidden(ctx context.Context, volName string, forbidden bool) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetVolumeForbidden")
	request := newRequest(ctx, post, proto.AdminVolForbidden).Header(api.h)
	request.addParam("name", volName)
	request.addParam("forbidden", strconv.FormatBool(forbidden))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetVolumeAuditLog(ctx context.Context, volName string, enable bool) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetVolumeAuditLog")
	request := newRequest(ctx, post, proto.AdminVolEnableAuditLog).Header(api.h)
	request.addParam("name", volName)
	request.addParam("enable", strconv.FormatBool(enable))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetMonitorPushAddr(ctx context.Context) (addr string, err error) {
	ctx = proto.ContextWithOperation(ctx, "GetMonitorPushAddr")
	err = api.mc.requestWith(&addr, newRequest(ctx, get, proto.AdminGetMonitorPushAddr).Header(api.h))
	return
}

func (api *AdminAPI) UploadFlowInfo(ctx context.Context, volName string, flowInfo *proto.ClientReportLimitInfo) (vv *proto.LimitRsp2Client, err error) {
	if flowInfo == nil {
		return nil, fmt.Errorf("flowinfo is nil")
	}
	vv = &proto.LimitRsp2Client{}
	ctx = proto.ContextWithOperation(ctx, "UploadFlowInfo")
	err = api.mc.requestWith(vv, newRequest(ctx, get, proto.QosUpload).Header(api.h).Body(flowInfo).
		Param(anyParam{"name", volName}, anyParam{"qosEnable", "true"}))
	log.Infof("action[UploadFlowInfo] enable %v", vv.Enable)
	return
}

func (api *AdminAPI) GetVolumeSimpleInfoWithFlowInfo(ctx context.Context, volName string) (vv *proto.SimpleVolView, err error) {
	vv = &proto.SimpleVolView{}
	ctx = proto.ContextWithOperation(ctx, "GetVolumeSimpleInfoWithFlowInfo")
	err = api.mc.requestWith(vv, newRequest(ctx, get, proto.AdminGetVol).
		Header(api.h).Param(anyParam{"name", volName}, anyParam{"init", "true"}))
	return
}

// access control list
func (api *AdminAPI) CheckACL(ctx context.Context) (ci *proto.ClusterInfo, err error) {
	ci = &proto.ClusterInfo{}
	ctx = proto.ContextWithOperation(ctx, "CheckACL")
	err = api.mc.requestWith(ci, newRequest(ctx, get, proto.AdminACL).Header(api.h))
	return
}

func (api *AdminAPI) GetClusterInfo(ctx context.Context) (ci *proto.ClusterInfo, err error) {
	ci = &proto.ClusterInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetClusterInfo")
	err = api.mc.requestWith(ci, newRequest(ctx, get, proto.AdminGetIP).Header(api.h))
	return
}

func (api *AdminAPI) GetVerInfo(ctx context.Context, volName string) (ci *proto.VolumeVerInfo, err error) {
	ci = &proto.VolumeVerInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetVerInfo")
	err = api.mc.requestWith(ci, newRequest(ctx, get, proto.AdminGetVolVer).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) CreateMetaPartition(ctx context.Context, volName string, count int, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "CreateMetaPartition")
	request := newRequest(ctx, get, proto.AdminCreateMetaPartition).Header(api.h)
	request.addParam("name", volName)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) ListVols(ctx context.Context, keywords string) (volsInfo []*proto.VolInfo, err error) {
	volsInfo = make([]*proto.VolInfo, 0)
	ctx = proto.ContextWithOperation(ctx, "ListVols")
	err = api.mc.requestWith(&volsInfo, newRequest(ctx, get, proto.AdminListVols).
		Header(api.h).addParam("keywords", keywords))
	return
}

func (api *AdminAPI) IsFreezeCluster(ctx context.Context, isFreeze bool, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "IsFreezeCluster")
	request := newRequest(ctx, get, proto.AdminClusterFreeze).Header(api.h)
	request.addParam("enable", strconv.FormatBool(isFreeze))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetForbidMpDecommission(ctx context.Context, disable bool) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetForbidMpDecommission")
	request := newRequest(ctx, get, proto.AdminClusterForbidMpDecommission).Header(api.h)
	request.addParam("enable", strconv.FormatBool(disable))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetMetaNodeThreshold(ctx context.Context, threshold float64, clientIDKey string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetMetaNodeThreshold")
	request := newRequest(ctx, get, proto.AdminSetMetaNodeThreshold).Header(api.h)
	request.addParam("threshold", strconv.FormatFloat(threshold, 'f', 6, 64))
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetClusterParas(ctx context.Context, batchCount, markDeleteRate, deleteWorkerSleepMs, autoRepairRate, loadFactor, maxDpCntLimit, clientIDKey string,
	dataNodesetSelector, metaNodesetSelector, dataNodeSelector, metaNodeSelector string,
) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetClusterParas")
	request := newRequest(ctx, get, proto.AdminSetNodeInfo).Header(api.h)
	request.addParam("batchCount", batchCount)
	request.addParam("markDeleteRate", markDeleteRate)
	request.addParam("deleteWorkerSleepMs", deleteWorkerSleepMs)
	request.addParam("autoRepairRate", autoRepairRate)
	request.addParam("loadFactor", loadFactor)
	request.addParam("maxDpCntLimit", maxDpCntLimit)
	request.addParam("clientIDKey", clientIDKey)

	request.addParam("dataNodesetSelector", dataNodesetSelector)
	request.addParam("metaNodesetSelector", metaNodesetSelector)
	request.addParam("dataNodeSelector", dataNodeSelector)
	request.addParam("metaNodeSelector", metaNodeSelector)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetClusterParas(ctx context.Context) (delParas map[string]string, err error) {
	ctx = proto.ContextWithOperation(ctx, "GetClusterParas")
	request := newRequest(ctx, get, proto.AdminGetNodeInfo).Header(api.h)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	delParas = make(map[string]string)
	err = api.mc.requestWith(&delParas, newRequest(ctx, get, proto.AdminGetNodeInfo).Header(api.h))
	return
}

func (api *AdminAPI) CreatePreLoadDataPartition(ctx context.Context, volName string, count int, capacity, ttl uint64, zongs string) (view *proto.DataPartitionsView, err error) {
	view = &proto.DataPartitionsView{}
	ctx = proto.ContextWithOperation(ctx, "CreatePreLoadDataPartition")
	err = api.mc.requestWith(view, newRequest(ctx, get, proto.AdminCreatePreLoadDataPartition).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"replicaNum", count},
		anyParam{"capacity", capacity},
		anyParam{"cacheTTL", ttl},
		anyParam{"zoneName", zongs},
	))
	return
}

func (api *AdminAPI) ListQuota(ctx context.Context, volName string) (quotaInfo []*proto.QuotaInfo, err error) {
	resp := &proto.ListMasterQuotaResponse{}
	ctx = proto.ContextWithOperation(ctx, "ListQuota")
	if err = api.mc.requestWith(resp, newRequest(ctx, get, proto.QuotaList).
		Header(api.h).addParam("name", volName)); err != nil {
		log.Errorf("action[ListQuota] fail. %v", err)
		return
	}
	quotaInfo = resp.Quotas
	log.Infof("action[ListQuota] success.")
	return quotaInfo, err
}

func (api *AdminAPI) CreateQuota(ctx context.Context, volName string, quotaPathInfos []proto.QuotaPathInfo, maxFiles uint64, maxBytes uint64) (quotaId uint32, err error) {
	ctx = proto.ContextWithOperation(ctx, "CreateQuota")
	if err = api.mc.requestWith(&quotaId, newRequest(ctx, get, proto.QuotaCreate).
		Header(api.h).Body(&quotaPathInfos).Param(
		anyParam{"name", volName},
		anyParam{"maxFiles", maxFiles},
		anyParam{"maxBytes", maxBytes})); err != nil {
		log.Errorf("action[CreateQuota] fail. %v", err)
		return
	}
	log.Infof("action[CreateQuota] success.")
	return
}

func (api *AdminAPI) UpdateQuota(ctx context.Context, volName string, quotaId string, maxFiles uint64, maxBytes uint64) (err error) {
	ctx = proto.ContextWithOperation(ctx, "UpdateQuota")
	request := newRequest(ctx, get, proto.QuotaUpdate).Header(api.h)
	request.addParam("name", volName)
	request.addParam("quotaId", quotaId)
	request.addParam("maxFiles", strconv.FormatUint(maxFiles, 10))
	request.addParam("maxBytes", strconv.FormatUint(maxBytes, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		log.Errorf("action[UpdateQuota] fail. %v", err)
		return
	}
	log.Infof("action[UpdateQuota] success.")
	return nil
}

func (api *AdminAPI) DeleteQuota(ctx context.Context, volName string, quotaId string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteQuota")
	request := newRequest(ctx, get, proto.QuotaDelete).Header(api.h)
	request.addParam("name", volName)
	request.addParam("quotaId", quotaId)
	if _, err = api.mc.serveRequest(request); err != nil {
		log.Errorf("action[DeleteQuota] fail. %v", err)
		return
	}
	log.Info("action[DeleteQuota] success.")
	return nil
}

func (api *AdminAPI) GetQuota(ctx context.Context, volName string, quotaId string) (quotaInfo *proto.QuotaInfo, err error) {
	info := &proto.QuotaInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetQuota")
	if err = api.mc.requestWith(info, newRequest(ctx, get, proto.QuotaGet).Header(api.h).
		Param(anyParam{"name", volName}, anyParam{"quotaId", quotaId})); err != nil {
		log.Errorf("action[GetQuota] fail. %v", err)
		return
	}
	quotaInfo = info
	log.Infof("action[GetQuota] %v success.", *quotaInfo)
	return quotaInfo, err
}

func (api *AdminAPI) QueryBadDisks(ctx context.Context) (badDisks *proto.BadDiskInfos, err error) {
	badDisks = &proto.BadDiskInfos{}
	ctx = proto.ContextWithOperation(ctx, "QueryBadDisks")
	err = api.mc.requestWith(badDisks, newRequest(ctx, get, proto.QueryBadDisks).Header(api.h))
	return
}

func (api *AdminAPI) DecommissionDisk(ctx context.Context, addr string, disk string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DecommissionDisk")
	return api.mc.request(newRequest(ctx, post, proto.DecommissionDisk).Header(api.h).
		addParam("addr", addr).addParam("disk", disk))
}

func (api *AdminAPI) RecommissionDisk(ctx context.Context, addr string, disk string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "RecommissionDisk")
	return api.mc.request(newRequest(ctx, post, proto.RecommissionDisk).Header(api.h).
		addParam("addr", addr).addParam("disk", disk))
}

func (api *AdminAPI) QueryDecommissionDiskProgress(ctx context.Context, addr string, disk string) (progress *proto.DecommissionProgress, err error) {
	progress = &proto.DecommissionProgress{}
	ctx = proto.ContextWithOperation(ctx, "QueryDecommissionDiskProgress")
	err = api.mc.requestWith(progress, newRequest(ctx, post, proto.QueryDiskDecoProgress).
		Header(api.h).Param(anyParam{"addr", addr}, anyParam{"disk", disk}))
	return
}

func (api *AdminAPI) ListQuotaAll(ctx context.Context) (volsInfo []*proto.VolInfo, err error) {
	volsInfo = make([]*proto.VolInfo, 0)
	ctx = proto.ContextWithOperation(ctx, "ListQuotaAll")
	err = api.mc.requestWith(&volsInfo, newRequest(ctx, get, proto.QuotaListAll).Header(api.h))
	return
}

func (api *AdminAPI) GetDiscardDataPartition(ctx context.Context) (discardDpInfos *proto.DiscardDataPartitionInfos, err error) {
	discardDpInfos = &proto.DiscardDataPartitionInfos{}
	ctx = proto.ContextWithOperation(ctx, "GetDiscardDataPartition")
	err = api.mc.requestWith(&discardDpInfos, newRequest(ctx, get, proto.AdminGetDiscardDp).Header(api.h))
	return
}

func (api *AdminAPI) SetDataPartitionDiscard(ctx context.Context, partitionId uint64, discard bool) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetDataPartitionDiscard")
	request := newRequest(ctx, post, proto.AdminSetDpDiscard).
		Header(api.h).
		addParam("id", strconv.FormatUint(partitionId, 10)).
		addParam("dpDiscard", strconv.FormatBool(discard))
	if err = api.mc.request(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteVersion(ctx context.Context, volName string, verSeq string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DeleteVersion")
	request := newRequest(ctx, get, proto.AdminDelVersion).Header(api.h)
	request.addParam("name", volName)
	request.addParam("verSeq", verSeq)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) SetStrategy(ctx context.Context, volName string, periodic string, count string, enable string, force string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetStrategy")
	request := newRequest(ctx, get, proto.AdminSetVerStrategy).Header(api.h)
	request.addParam("name", volName)
	request.addParam("periodic", periodic)
	request.addParam("count", count)
	request.addParam("enable", enable)
	request.addParam("force", force)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) CreateVersion(ctx context.Context, volName string) (ver *proto.VolVersionInfo, err error) {
	ver = &proto.VolVersionInfo{}
	ctx = proto.ContextWithOperation(ctx, "CreateVersion")
	err = api.mc.requestWith(ver, newRequest(ctx, get, proto.AdminCreateVersion).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) GetLatestVer(ctx context.Context, volName string) (ver *proto.VolVersionInfo, err error) {
	ver = &proto.VolVersionInfo{}
	ctx = proto.ContextWithOperation(ctx, "GetLatestVer")
	err = api.mc.requestWith(ver, newRequest(ctx, get, proto.AdminGetVersionInfo).
		Header(api.h).addParam("name", volName))
	return
}

func (api *AdminAPI) GetVerList(ctx context.Context, volName string) (verList *proto.VolVersionInfoList, err error) {
	verList = &proto.VolVersionInfoList{}
	ctx = proto.ContextWithOperation(ctx, "GetVerList")
	err = api.mc.requestWith(verList, newRequest(ctx, get, proto.AdminGetAllVersionInfo).
		Header(api.h).addParam("name", volName))
	log.Debugf("GetVerList. vol %v verList %v", volName, verList)
	for _, info := range verList.VerList {
		log.Debugf("GetVerList. vol %v verList %v", volName, info)
	}
	return
}

func (api *AdminAPI) SetBucketLifecycle(ctx context.Context, req *proto.LcConfiguration) (err error) {
	ctx = proto.ContextWithOperation(ctx, "SetBucketLifecycle")
	return api.mc.request(newRequest(ctx, post, proto.SetBucketLifecycle).Header(api.h).Body(req))
}

func (api *AdminAPI) GetBucketLifecycle(ctx context.Context, volume string) (lcConf *proto.LcConfiguration, err error) {
	lcConf = &proto.LcConfiguration{}
	ctx = proto.ContextWithOperation(ctx, "GetBucketLifecycle")
	err = api.mc.requestWith(lcConf, newRequest(ctx, get, proto.GetBucketLifecycle).
		Header(api.h).addParam("name", volume))
	return
}

func (api *AdminAPI) DelBucketLifecycle(ctx context.Context, volume string) (err error) {
	ctx = proto.ContextWithOperation(ctx, "DelBucketLifecycle")
	request := newRequest(ctx, get, proto.DeleteBucketLifecycle).Header(api.h)
	request.addParam("name", volume)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) GetS3QoSInfo(ctx context.Context) (data []byte, err error) {
	ctx = proto.ContextWithOperation(ctx, "GetS3QoSInfo")
	return api.mc.serveRequest(newRequest(ctx, get, proto.S3QoSGet).Header(api.h))
}
