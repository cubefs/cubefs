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
	"net/url"
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
	return api.GetClusterWithPool(volStorageClass, false)
}

func (api *AdminAPI) GetClusterWithPool(volStorageClass, volPool bool) (cv *proto.ClusterView, err error) {
	cv = &proto.ClusterView{}
	req := newRequest(get, proto.AdminGetCluster).Header(api.h).
		addParam("volStorageClass", strconv.FormatBool(volStorageClass)).
		addParam("volPool", strconv.FormatBool(volPool))
	err = api.mc.requestWith(cv, req)
	return
}

func (api *AdminAPI) GetClusterView() (cv *proto.ClusterView, err error) {
	cv = &proto.ClusterView{}
	err = api.mc.requestWith(cv, newRequest(get, proto.AdminGetCluster).Header(api.h))
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

func (api *AdminAPI) DiagnoseMetaPartition() (diagnosis *proto.MetaPartitionDiagnosisV1, err error) {
	diagnosis = &proto.MetaPartitionDiagnosisV1{}
	err = api.mc.requestWith(diagnosis, newRequest(get, proto.AdminDiagnoseMetaPartition).Header(api.h).Param(
		anyParam{"v1", "true"},
	))
	return
}

func (api *AdminAPI) LoadDataPartition(volName string, partitionID uint64, clientIDKey string) (err error) {
	return api.mc.request(newRequest(get, proto.AdminLoadDataPartition).Header(api.h).Param(
		anyParam{"id", partitionID},
		anyParam{"name", volName},
		anyParam{"clientIDKey", clientIDKey},
	))
}

func (api *AdminAPI) CreateDataPartition(volName string, count int, clientIDKey string, poolId uint8) (err error) {
	return api.mc.request(newRequest(get, proto.AdminCreateDataPartition).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"count", count},
		anyParam{"clientIDKey", clientIDKey},
		anyParam{"poolId", poolId},
	))
}

func (api *AdminAPI) DecommissionDataPartition(dataPartitionID uint64, nodeAddr string, dstNodeSet uint64, raftForce bool, weight int, clientIDKey string, decommissionType int) (err error) {
	request := newRequest(get, proto.AdminDecommissionDataPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if dstNodeSet != 0 {
		request.addParam("dstNodeSet", strconv.FormatUint(dstNodeSet, 10))
	}
	request.addParam("raftForceDel", strconv.FormatBool(raftForce))
	request.addParam("weight", strconv.Itoa(weight))
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("decommissionType", strconv.Itoa(decommissionType))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) DecommissionMetaPartition(metaPartitionID uint64, nodeAddr, clientIDKey string, storeMode proto.StoreMode) (err error) {
	request := newRequest(get, proto.AdminDecommissionMetaPartition).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	if storeMode != 0 {
		request.addParam("storeMode", strconv.FormatInt(int64(storeMode), 10))
	}
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

func (api *AdminAPI) DeleteMetaReplica(metaPartitionID uint64, nodeAddr string, clientIDKey string, raftForceDel bool) (err error) {
	request := newRequest(get, proto.AdminDeleteMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	request.addParam("force", strconv.FormatBool(raftForceDel))
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddMetaReplica(metaPartitionID uint64, nodeAddr string, clientIDKey string, storeMode proto.StoreMode) (err error) {
	request := newRequest(get, proto.AdminAddMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	if storeMode != proto.StoreModeDef {
		request.addParam("storeMode", strconv.FormatInt(int64(storeMode), 10))
	}
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) AddMetaPartitionLearner(metaPartitionID uint64, nodeAddr string, clientIDKey string, storeMode proto.StoreMode, manualPromote bool) (err error) {
	request := newRequest(get, proto.AdminAddMetaPartitionLearner).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	if storeMode != proto.StoreModeDef {
		request.addParam("storeMode", strconv.FormatInt(int64(storeMode), 10))
	}
	if manualPromote {
		request.addParam("manualPromote", "true")
	}
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) PromoteMetaReplica(metaPartitionID uint64, nodeAddr string, clientIDKey string) (err error) {
	request := newRequest(get, proto.AdminPromoteMetaReplica).Header(api.h)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	request.addParam("clientIDKey", clientIDKey)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *AdminAPI) QueryDataPartitionDecommissionStatusUpdateRecords(partitionId uint64) (records []*proto.DecommissionStatusRecord, err error) {
	request := newRequest(get, proto.AdminQueryDataPartitionDecommissionStatusUpdateRecords).Header(api.h)
	request.addParam("id", strconv.FormatUint(partitionId, 10))
	records = make([]*proto.DecommissionStatusRecord, 0)
	err = api.mc.requestWith(&records, request)
	return
}

func (api *AdminAPI) QueryDataPartitionDecommissionStatus(partitionId uint64, showQueuedTask bool) (info interface{}, err error) {
	request := newRequest(get, proto.AdminQueryDataPartitionDecommissionStatus).Header(api.h)
	request.addParam("id", strconv.FormatUint(partitionId, 10))
	request.addParam("showQueuedTask", strconv.FormatBool(showQueuedTask))
	if showQueuedTask {
		info = &struct {
			DecommissionInfo *proto.DecommissionDataPartitionInfo
			TaskQueue        []proto.DecommissionTaskInfo
		}{}
	} else {
		info = &struct {
			DecommissionInfo *proto.DecommissionDataPartitionInfo
			QueuedTaskNum    int
		}{}
	}
	err = api.mc.requestWith(info, request)
	return
}

func (api *AdminAPI) QueryDataPartitionDiskDecommissionInfoStat() (infos []*proto.DecommissionInfoStat, err error) {
	request := newRequest(get, proto.AdminQueryDiskDecommissionInfoStat).Header(api.h)
	infos = make([]*proto.DecommissionInfoStat, 0)
	err = api.mc.requestWith(&infos, request)
	return
}

func (api *AdminAPI) QueryDataPartitionDataNodeDecommissionInfoStat() (infos []*proto.DecommissionInfoStat, err error) {
	request := newRequest(get, proto.AdminQueryDataNodeDecommissionInfoStat).Header(api.h)
	infos = make([]*proto.DecommissionInfoStat, 0)
	err = api.mc.requestWith(&infos, request)
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
	optVolQuotaPool int,
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
	request.addParam(proto.MetaNearReadKey, strconv.FormatBool(vv.MetaNearRead))
	request.addParam(proto.VolEnableDirectRead, strconv.FormatBool(vv.DirectRead))
	request.addParam(proto.VolIgnoreTinyRecover, strconv.FormatBool(vv.IgnoreTinyRecover))
	request.addParam(proto.MaximallyReadKey, strconv.FormatBool(vv.MaximallyRead))
	request.addParam("ebsBlkSize", strconv.Itoa(vv.ObjBlockSize))
	request.addParam("dpReadOnlyWhenVolFull", strconv.FormatBool(vv.DpReadOnlyWhenVolFull))
	request.addParam("replicaNum", strconv.FormatUint(uint64(vv.DpReplicaNum), 10))
	request.addParam("enableQuota", strconv.FormatBool(vv.EnableQuota))
	request.addParam("deleteLockTime", strconv.FormatInt(vv.DeleteLockTime, 10))
	request.addParam("autoDpMetaRepair", strconv.FormatBool(vv.EnableAutoDpMetaRepair))
	request.addParam("autoMpMetaRepair", strconv.FormatBool(vv.EnableAutoMpMetaRepair))
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
	request.addParamAny("remoteCacheReadTimeout", vv.RemoteCacheReadTimeout)
	request.addParam("remoteCacheMaxFileSizeGB", strconv.FormatInt(vv.RemoteCacheMaxFileSizeGB, 10))
	request.addParamAny("remoteCacheOnlyForNotSSD", vv.RemoteCacheOnlyForNotSSD)
	request.addParamAny("remoteCacheMultiRead", vv.RemoteCacheMultiRead)
	request.addParamAny("flashNodeTimeoutCount", vv.FlashNodeTimeoutCount)
	request.addParamAny("remoteCacheSameZoneTimeout", vv.RemoteCacheSameZoneTimeout)
	request.addParamAny("remoteCacheSameRegionTimeout", vv.RemoteCacheSameRegionTimeout)
	request.addParamAny("remoteCacheDisableTTL", vv.RemoteCacheDisableTTL)

	if vv.DefaultStoreMode != proto.StoreModeDef {
		request.addParam("storeMode", strconv.FormatInt(int64(vv.DefaultStoreMode), 10))
	}
	if vv.DpTag != "" {
		request.addParam("dpTag", url.QueryEscape(vv.DpTag))
	}
	if vv.MpTag != "" {
		request.addParam("mpTag", url.QueryEscape(vv.MpTag))
	}

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
	if optVolQuotaPool > 0 {
		request.addParam("quotaPool", strconv.FormatInt(int64(optVolQuotaPool), 10))
		request.addParam("quotaOfPool", strconv.FormatInt(int64(vv.QuotaOfPool[0].QuotaGB), 10))
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

func (api *AdminAPI) VolAddPool(volName string, poolId uint8, authKey, clientIDKey string) (err error) {
	request := newRequest(http.MethodGet, proto.AdminVolAddPool).Header(api.h)
	request.addParam("name", volName)
	request.addParam("poolId", strconv.FormatUint(uint64(poolId), 10))
	request.addParam("authKey", authKey)
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) VolUpdatePoolId(volName string, poolId uint8, poolName, authKey, clientIDKey string) (err error) {
	request := newRequest(http.MethodGet, proto.AdminVolUpdatePoolId).Header(api.h)
	request.addParam("name", volName)
	request.addParam("poolId", strconv.FormatUint(uint64(poolId), 10))
	request.addParam("poolName", poolName)
	request.addParam("authKey", authKey)
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateVolName(volName, owner string, capacity uint64, deleteLockTime int64, crossZone, normalZonesFirst bool,
	business string, mpCount, dpCount, replicaNum, dpSize int, followerRead bool, zoneName string, ebsBlkSize int,
	dpReadOnlyWhenVolFull bool, txMask string, txTimeout uint32, txConflictRetryNum int64, txConflictRetryInterval int64, optEnableQuota string,
	clientIDKey string, volStorageClass uint32, allowedStorageClass string, optMetaFollowerRead string, optMetaNearRead string, optMaximallyRead string,
	remoteCacheEnable string, remoteCacheAutoPrepare string, remoteCachePath string, remoteCacheTTL int64, remoteCacheReadTimeout int64,
	remoteCacheMaxFileSizeGB int64, remoteCacheOnlyForNotSSD string, remoteCacheMultiRead string, flashNodeTimeoutCount int64,
	remoteCacheSameZoneTimeout int64, remoteCacheSameRegionTimeout int64, storeMode proto.StoreMode,
	poolId uint8, pools string, remoteCacheDisableTTL bool,
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
	request.addParam(proto.MetaNearReadKey, optMetaNearRead)
	request.addParam(proto.MaximallyReadKey, optMaximallyRead)
	request.addParam("zoneName", zoneName)
	request.addParam("ebsBlkSize", strconv.Itoa(ebsBlkSize))
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
	request.addParam("remoteCacheMultiRead", remoteCacheMultiRead)
	request.addParamAny("flashNodeTimeoutCount", flashNodeTimeoutCount)
	request.addParamAny("remoteCacheSameZoneTimeout", remoteCacheSameZoneTimeout)
	request.addParamAny("remoteCacheSameRegionTimeout", remoteCacheSameRegionTimeout)
	request.addParam("remoteCacheDisableTTL", strconv.FormatBool(remoteCacheDisableTTL))
	if storeMode != proto.StoreModeDef {
		request.addParam("storeMode", strconv.FormatInt(int64(storeMode), 10))
	}
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
	if poolId > 0 {
		request.addParam("poolId", strconv.FormatUint(uint64(poolId), 10))
	}
	if pools != "" {
		request.addParam("allowedPools", pools)
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

func (api *AdminAPI) SetMasterFlashTopoDeletionDelayTime(flashTopoDeletionDelayTimeHour int) (err error) {
	request := newRequest(get, proto.AdminSetMasterFlashTopoDeletionDelayTime)
	request.addParam("flashTopoDeletionDelayTime", strconv.FormatInt(int64(flashTopoDeletionDelayTimeHour), 10))
	_, err = api.mc.serveRequest(request)
	return
}

type ClusterParas struct {
	BatchCount                             string
	MarkDeleteRate                         string
	DeleteWorkerSleepMs                    string
	AutoRepairRate                         string
	LoadFactor                             string
	MaxDpCntLimit                          string
	MaxMpCntLimit                          string
	ClientIDKey                            string
	EnableAutoDecommissionDisk             string
	AutoDecommissionDiskInterval           string
	EnableAutoDpMetaRepair                 string
	AutoDpMetaRepairParallelCnt            string
	EnableAutoMpMetaRepair                 string
	AutoMpMetaRepairParallelCnt            string
	AutoDistributionOptimization           string
	DpRepairTimeout                        string
	DpTimeout                              string
	MpTimeout                              string
	DpBackupTimeout                        string
	DecommissionDpLimit                    string
	DecommissionDiskLimit                  string
	DecommissionFirstHostDiskParallelLimit string
	ForbidWriteOpOfProtoVersion0           string
	MediaType                              string
	HandleTimeout                          string
	ReadDataNodeTimeout                    string
	RackAware                              string
	DistributionOptimizationConDpCnt       string
	DistributionOptimizationThreshold      string
	RemoteCacheTTL                         string
	RemoteCacheReadTimeout                 string
	RemoteCacheMultiRead                   string
	FlashNodeTimeoutCount                  string
	RemoteCacheSameZoneTimeout             string
	RemoteCacheSameRegionTimeout           string
	FlashHotKeyMissCount                   string
	FlashReadFlowLimit                     string
	FlashWriteFlowLimit                    string
	FlashKeyFlowLimit                      string
	RemoteClientFlowLimit                  string
	EnableMpDecommissionByLearner          string
	LearnerRecoverTimeoutSeconds           string
	MetaAutoAddReplicaLimit                string
	MetaManualDecommissionLimit            string
	MetaBalanceLimit                       string
	MetaManualAddReplicaLimit              string
	DpLimitSsdBaseCount                    string
	DpLimitSsdFactor                       string
	DpLimitHddBaseCount                    string
	DpLimitHddFactor                       string
	AutoFixTag                             string
	DefaultDpTag                           string
	DefaultMpTag                           string
	PoolId                                 string
}

func (api *AdminAPI) SetClusterParas(params *ClusterParas) (err error) {
	if params == nil {
		return fmt.Errorf("cluster params is nil")
	}
	request := newRequest(get, proto.AdminSetNodeInfo).Header(api.h)
	request.addParam("batchCount", params.BatchCount)
	request.addParam("markDeleteRate", params.MarkDeleteRate)
	request.addParam("deleteWorkerSleepMs", params.DeleteWorkerSleepMs)
	request.addParam("autoRepairRate", params.AutoRepairRate)
	request.addParam("loadFactor", params.LoadFactor)
	request.addParam("maxDpCntLimit", params.MaxDpCntLimit)
	request.addParam("maxMpCntLimit", params.MaxMpCntLimit)
	request.addParam("clientIDKey", params.ClientIDKey)

	// request.addParam("dataNodesetSelector", dataNodesetSelector)
	// request.addParam("metaNodesetSelector", metaNodesetSelector)
	// request.addParam("dataNodeSelector", dataNodeSelector)
	// request.addParam("metaNodeSelector", metaNodeSelector)
	// if markDiskBrokenThreshold != "" {
	//	request.addParam("markDiskBrokenThreshold", markDiskBrokenThreshold)
	// }
	if params.EnableAutoDecommissionDisk != "" {
		request.addParam("autoDecommissionDisk", params.EnableAutoDecommissionDisk)
	}
	if params.AutoDecommissionDiskInterval != "" {
		request.addParam("autoDecommissionDiskInterval", params.AutoDecommissionDiskInterval)
	}
	if params.EnableAutoDpMetaRepair != "" {
		request.addParam("autoDpMetaRepair", params.EnableAutoDpMetaRepair)
	}
	if params.AutoDpMetaRepairParallelCnt != "" {
		request.addParam("autoDpMetaRepairParallelCnt", params.AutoDpMetaRepairParallelCnt)
	}
	if params.EnableAutoMpMetaRepair != "" {
		request.addParam("autoMpMetaRepair", params.EnableAutoMpMetaRepair)
	}
	if params.AutoMpMetaRepairParallelCnt != "" {
		request.addParam("autoMpMetaRepairParallelCnt", params.AutoMpMetaRepairParallelCnt)
	}
	if params.AutoDistributionOptimization != "" {
		request.addParam("autoDistributionOptimization", params.AutoDistributionOptimization)
	}
	if params.DpRepairTimeout != "" {
		request.addParam("dpRepairTimeOut", params.DpRepairTimeout)
	}
	if params.DpTimeout != "" {
		request.addParam("dpTimeout", params.DpTimeout)
	}
	if params.MpTimeout != "" {
		request.addParam("mpTimeout", params.MpTimeout)
	}
	if params.DpBackupTimeout != "" {
		request.addParam("dpBackupTimeout", params.DpBackupTimeout)
	}
	if params.DecommissionDpLimit != "" {
		request.addParam("decommissionLimit", params.DecommissionDpLimit)
	}
	if params.DecommissionDiskLimit != "" {
		request.addParam("decommissionDiskLimit", params.DecommissionDiskLimit)
	}
	if params.DecommissionFirstHostDiskParallelLimit != "" {
		request.addParam("decommissionFirstHostDiskParallelLimit", params.DecommissionFirstHostDiskParallelLimit)
	}
	if params.ForbidWriteOpOfProtoVersion0 != "" {
		request.addParam("forbidWriteOpOfProtoVersion0", params.ForbidWriteOpOfProtoVersion0)
	}
	if params.MediaType != "" {
		request.addParam("dataMediaType", params.MediaType)
	}
	if params.HandleTimeout != "" {
		request.addParam("flashNodeHandleReadTimeout", params.HandleTimeout)
	}
	if params.ReadDataNodeTimeout != "" {
		request.addParam("flashNodeReadDataNodeTimeout", params.ReadDataNodeTimeout)
	}
	if params.RackAware != "" {
		request.addParam("rackAware", params.RackAware)
	}
	// Distribution optimization parameters
	if params.DistributionOptimizationConDpCnt != "" {
		request.addParam("distributionOptimizationConDpCnt", params.DistributionOptimizationConDpCnt)
	}
	if params.DistributionOptimizationThreshold != "" {
		request.addParam("distributionOptimizationThreshold", params.DistributionOptimizationThreshold)
	}
	if params.FlashHotKeyMissCount != "" {
		request.addParam("flashHotKeyMissCount", params.FlashHotKeyMissCount)
	}
	// remoteCache config
	if params.RemoteCacheTTL != "" {
		request.addParamAny("remoteCacheTTL", params.RemoteCacheTTL)
	}
	if params.RemoteCacheReadTimeout != "" {
		request.addParamAny("remoteCacheReadTimeout", params.RemoteCacheReadTimeout)
	}
	if params.RemoteCacheMultiRead != "" {
		request.addParamAny("remoteCacheMultiRead", params.RemoteCacheMultiRead)
	}
	if params.FlashNodeTimeoutCount != "" {
		request.addParamAny("flashNodeTimeoutCount", params.FlashNodeTimeoutCount)
	}
	if params.RemoteCacheSameZoneTimeout != "" {
		request.addParamAny("remoteCacheSameZoneTimeout", params.RemoteCacheSameZoneTimeout)
	}
	if params.RemoteCacheSameRegionTimeout != "" {
		request.addParamAny("remoteCacheSameRegionTimeout", params.RemoteCacheSameRegionTimeout)
	}
	if params.FlashReadFlowLimit != "" {
		request.addParamAny("flashReadFlowLimit", params.FlashReadFlowLimit)
	}
	if params.FlashWriteFlowLimit != "" {
		request.addParamAny("flashWriteFlowLimit", params.FlashWriteFlowLimit)
	}
	if params.FlashKeyFlowLimit != "" {
		request.addParamAny("flashKeyFlowLimit", params.FlashKeyFlowLimit)
	}
	if params.RemoteClientFlowLimit != "" {
		request.addParamAny("remoteClientFlowLimit", params.RemoteClientFlowLimit)
	}
	if params.EnableMpDecommissionByLearner != "" {
		request.addParam("enableMpDecommissionByLearner", params.EnableMpDecommissionByLearner)
	}
	if params.LearnerRecoverTimeoutSeconds != "" {
		request.addParam("learnerRecoverTimeoutSeconds", params.LearnerRecoverTimeoutSeconds)
	}
	if params.DpLimitSsdBaseCount != "" {
		request.addParamAny("dpLimitSsdBaseCount", params.DpLimitSsdBaseCount)
	}
	if params.DpLimitSsdFactor != "" {
		request.addParamAny("dpLimitSsdFactor", params.DpLimitSsdFactor)
	}
	if params.DpLimitHddBaseCount != "" {
		request.addParamAny("dpLimitHddBaseCount", params.DpLimitHddBaseCount)
	}
	if params.DpLimitHddFactor != "" {
		request.addParamAny("dpLimitHddFactor", params.DpLimitHddFactor)
	}
	if params.PoolId != "" {
		request.addParam("poolId", params.PoolId)
	}
	if params.MetaAutoAddReplicaLimit != "" {
		request.addParam("metaAutoAddReplicaLimit", params.MetaAutoAddReplicaLimit)
	}
	if params.MetaManualDecommissionLimit != "" {
		request.addParam("metaManualDecommissionLimit", params.MetaManualDecommissionLimit)
	}
	if params.MetaBalanceLimit != "" {
		request.addParam("metaBalanceLimit", params.MetaBalanceLimit)
	}
	if params.MetaManualAddReplicaLimit != "" {
		request.addParam("metaManualAddReplicaLimit", params.MetaManualAddReplicaLimit)
	}
	if params.AutoFixTag != "" {
		request.addParam("autoFixTag", params.AutoFixTag)
	}
	if params.DefaultDpTag != "" {
		request.addParam("defaultDpTag", url.QueryEscape(params.DefaultDpTag))
	}
	if params.DefaultMpTag != "" {
		request.addParam("defaultMpTag", url.QueryEscape(params.DefaultMpTag))
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

// ListStoragePools lists all storage pools
func (api *AdminAPI) ListStoragePools() (pools []*proto.StoragePoolInfo, err error) {
	pools = []*proto.StoragePoolInfo{}
	err = api.mc.requestWith(&pools, newRequest(get, proto.AdminListStoragePools).Header(api.h))
	return
}

// GetStoragePool gets storage pool by ID
func (api *AdminAPI) GetStoragePool(poolId uint8) (pool *proto.StoragePoolInfo, err error) {
	pool = &proto.StoragePoolInfo{}
	err = api.mc.requestWith(pool, newRequest(get, proto.AdminGetStoragePool).Header(api.h).
		addParam("id", strconv.FormatUint(uint64(poolId), 10)))
	return
}

// CreateStoragePool creates a new storage pool
func (api *AdminAPI) CreateStoragePool(poolInfo *proto.StoragePoolInfo) (err error) {
	params := []anyParam{
		{"id", poolInfo.Id},
		{"name", poolInfo.Name},
	}
	if poolInfo.StorageClass > 0 {
		params = append(params, anyParam{"storageClass", poolInfo.StorageClass})
	}
	if poolInfo.CId > 0 {
		params = append(params, anyParam{"cId", poolInfo.CId})
	}
	if poolInfo.ECAddr != "" {
		params = append(params, anyParam{"ecAddr", poolInfo.ECAddr})
	}
	return api.mc.request(newRequest(post, proto.AdminCreateStoragePool).Header(api.h).Param(params...))
}

// UpdateStoragePool updates storage pool fields
func (api *AdminAPI) UpdateStoragePool(poolId uint8, poolInfo *proto.StoragePoolInfo) (err error) {
	params := []anyParam{
		{"id", poolId},
	}
	if poolInfo.Name != "" {
		params = append(params, anyParam{"name", poolInfo.Name})
	}
	if poolInfo.CId > 0 {
		params = append(params, anyParam{"cId", poolInfo.CId})
	}
	if poolInfo.ECAddr != "" {
		params = append(params, anyParam{"ecAddr", poolInfo.ECAddr})
	}
	return api.mc.request(newRequest(post, proto.AdminUpdateStoragePool).Header(api.h).Param(params...))
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

func (api *AdminAPI) DecommissionDisk(addr string, disk string, weight int, raftForceDel bool) (err error) {
	return api.mc.request(newRequest(post, proto.DecommissionDisk).Header(api.h).
		addParam("addr", addr).addParam("disk", disk).addParam("decommissionType", "1").addParam("weight", strconv.Itoa(weight)).addParam("raftForceDel", strconv.FormatBool(raftForceDel)))
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

func (api *AdminAPI) TurnFlashGroupByName(name string, enable bool) (result string, err error) {
	request := newRequest(post, proto.AdminFlashGroupTurn).Header(api.h).
		addParamAny("enable", enable).addParam("name", name)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *AdminAPI) CreateFlashGroup(slots string, weight int, gradualFlag bool, step uint32) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupCreate).
		Header(api.h).Param(anyParam{"slots", slots}, anyParam{"weight", weight}, anyParam{"gradualFlag", gradualFlag},
		anyParam{"step", step}))
	return
}

func (api *AdminAPI) CreateFlashGroupByName(name, slots string, weight int, gradualFlag bool, step uint32) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupCreate).
		Header(api.h).Param(anyParam{"slots", slots}, anyParam{"weight", weight}, anyParam{"gradualFlag", gradualFlag},
		anyParam{"step", step}, anyParam{"name", name}))
	return
}

func (api *AdminAPI) SetFlashGroup(flashGroupID uint64, isActive bool) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupSet).
		Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"enable", isActive}))
	return
}

func (api *AdminAPI) SetFlashGroupByName(name string, flashGroupID uint64, isActive bool) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, proto.AdminFlashGroupSet).
		Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"enable", isActive}, anyParam{"name", name}))
	return
}

func (api *AdminAPI) RemoveFlashGroup(flashGroupID uint64, gradualFlag bool, step uint32) (result string, err error) {
	request := newRequest(post, proto.AdminFlashGroupRemove).Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"gradualFlag", gradualFlag},
		anyParam{"step", step})
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *AdminAPI) RemoveFlashGroupByName(name string, flashGroupID uint64, gradualFlag bool, step uint32) (result string, err error) {
	request := newRequest(post, proto.AdminFlashGroupRemove).Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"gradualFlag", gradualFlag},
		anyParam{"step", step}, anyParam{"name", name})
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *AdminAPI) flashGroupFlashNodes(uri string, flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, uri).Header(api.h).Param(
		anyParam{"id", flashGroupID}, anyParam{"count", count}, anyParam{"zoneName", zoneName}, anyParam{"addr", addr}))
	return
}

func (api *AdminAPI) flashGroupFlashNodesByName(name, uri string, flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(post, uri).Header(api.h).Param(
		anyParam{"id", flashGroupID}, anyParam{"count", count}, anyParam{"zoneName", zoneName}, anyParam{"addr", addr}, anyParam{"name", name}))
	return
}

func (api *AdminAPI) FlashGroupAddFlashNode(flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodes(proto.AdminFlashGroupNodeAdd, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) FlashGroupAddFlashNodeByName(name string, flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodesByName(name, proto.AdminFlashGroupNodeAdd, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) FlashGroupRemoveFlashNode(flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodes(proto.AdminFlashGroupNodeRemove, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) FlashGroupRemoveFlashNodeByName(name string, flashGroupID uint64, count int, zoneName, addr string,
) (fgView proto.FlashGroupAdminView, err error) {
	return api.flashGroupFlashNodesByName(name, proto.AdminFlashGroupNodeRemove, flashGroupID, count, zoneName, addr)
}

func (api *AdminAPI) GetFlashGroup(flashGroupID uint64) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupGet).
		Header(api.h).addParamAny("id", flashGroupID))
	return
}

func (api *AdminAPI) GetFlashGroupByName(name string, flashGroupID uint64) (fgView proto.FlashGroupAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupGet).
		Header(api.h).Param(anyParam{"id", flashGroupID}, anyParam{"name", name}))
	return
}

func (api *AdminAPI) ListFlashGroup(isActive bool) (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).
		Header(api.h).Param(anyParam{"enable", isActive}))
	return
}

func (api *AdminAPI) ListFlashGroupByName(name string, isActive bool, showAllTopo bool) (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).
		Header(api.h).Param(anyParam{"enable", isActive}, anyParam{"name", name}, anyParam{"showAllTopo", showAllTopo}))
	return
}

func (api *AdminAPI) ListFlashGroups() (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).Header(api.h))
	return
}

func (api *AdminAPI) ListFlashGroupsByName(name string, showAllTopo bool) (fgView proto.FlashGroupsAdminView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.AdminFlashGroupList).Header(api.h).
		Param(anyParam{"name", name}, anyParam{"showAllTopo", showAllTopo}))
	return
}

func (api *AdminAPI) ClientFlashGroups(topoName string) (fgView proto.FlashGroupView, err error) {
	err = api.mc.requestWith(&fgView, newRequest(get, proto.ClientFlashGroups).Header(api.h).addParamAny("name", topoName))
	return
}

func (api *AdminAPI) CreateMetaNodeBalanceTask() (task *proto.ClusterPlan, err error) {
	task = &proto.ClusterPlan{
		Low:  make(map[string]*proto.ZonePressureView),
		Plan: make([]*proto.MetaBalancePlan, 0),
	}
	err = api.mc.requestWith(task, newRequest(get, proto.CreateMetaNodeBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) GetMetaNodeBalanceTask() (task *proto.ClusterPlan, err error) {
	task = &proto.ClusterPlan{
		Low:  make(map[string]*proto.ZonePressureView),
		Plan: make([]*proto.MetaBalancePlan, 0),
	}
	err = api.mc.requestWith(task, newRequest(get, proto.GetMetaNodeBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) RunMetaNodeBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.RunMetaNodeBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) StopMetaNodeBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.StopMetaNodeBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) DeleteMetaNodeBalanceTask() (result string, err error) {
	err = api.mc.requestWith(&result, newRequest(get, proto.DeleteMetaNodeBalanceTask).Header(api.h))
	return
}

func (api *AdminAPI) CancelDpDistributionOptimization() (err error) {
	request := newRequest(post, proto.AdminCancelDpDistributionOptimization)
	err = api.mc.request(request)
	return
}

func (api *AdminAPI) QueryDistributionOptimizationStatus() (status *proto.DistributionOptimizationStatus, err error) {
	status = &proto.DistributionOptimizationStatus{}
	err = api.mc.requestWith(&status, newRequest(get, proto.AdminQueryDistributionOptimizationStatus).Header(api.h))
	return
}

func (api *AdminAPI) QueryDpDecommissionStatus(decommissionType int) (response *proto.QueryDecommissionStatusResponse, err error) {
	response = &proto.QueryDecommissionStatusResponse{}
	request := newRequest(get, proto.AdminQueryDpDecommissionStatus).Header(api.h)
	request.addParam("decommissionType", strconv.FormatInt(int64(decommissionType), 10))
	err = api.mc.requestWith(response, request)
	return
}

func (api *AdminAPI) GetRemoteCacheConfig() (config *proto.RemoteCacheConfig, err error) {
	config = &proto.RemoteCacheConfig{}
	err = api.mc.requestWith(config, newRequest(get, proto.AdminGetRemoteCacheConfig).Header(api.h))
	return
}

// Flash topology admin APIs
func (api *AdminAPI) ListAllFlashTopos() (ftvs []*proto.FlashTopologyAdminView, err error) {
	ftvs = make([]*proto.FlashTopologyAdminView, 0)
	err = api.mc.requestWith(&ftvs, newRequest(get, proto.AdminFlashTopoList).Header(api.h))
	return
}

func (api *AdminAPI) AddFlashTopo(name, region string) (result string, err error) {
	req := newRequest(get, proto.AdminFlashTopoAdd).Header(api.h)
	req.addParam("name", name)
	req.addParam("region", region)
	var data []byte
	if data, err = api.mc.serveRequest(req); err != nil {
		return
	}
	return string(data), nil
}

func (api *AdminAPI) DelFlashTopo(name string, gradualFlag bool, step uint32, forceDel bool) (result string, err error) {
	req := newRequest(get, proto.AdminFlashTopoDel).Header(api.h)
	req.addParam("name", name)
	req.addParamAny("gradualFlag", gradualFlag)
	req.addParamAny("step", step)
	req.addParamAny("forceDel", forceDel)
	var data []byte
	if data, err = api.mc.serveRequest(req); err != nil {
		return
	}
	return string(data), nil
}

func (api *AdminAPI) RenameFlashTopo(srcName, dstName string) (result string, err error) {
	req := newRequest(get, proto.AdminFlashTopoRename).Header(api.h)
	req.addParam("name", srcName)
	req.addParam("newName", dstName)
	var data []byte
	if data, err = api.mc.serveRequest(req); err != nil {
		return
	}
	return string(data), nil
}

func (api *AdminAPI) CancelDeleteFlashTopo(name string) (result string, err error) {
	req := newRequest(get, proto.AdminFlashTopoCancelDelete).Header(api.h)
	req.addParam("name", name)
	var data []byte
	if data, err = api.mc.serveRequest(req); err != nil {
		return
	}
	return string(data), nil
}

func (api *AdminAPI) GetSelectTagSummary(detail bool) (summary *proto.TagSummary, err error) {
	summary = &proto.TagSummary{}
	req := newRequest(get, proto.AdminGetTagSummary).Header(api.h)
	req.addParamAny("detail", detail)
	err = api.mc.requestWith(summary, req)
	return
}

func (api *AdminAPI) GetVolTagSummary(name string) (summary *proto.VolTagSummary, err error) {
	summary = &proto.VolTagSummary{}
	req := newRequest(get, proto.AdminGetVolTagSummary).Header(api.h)
	req.addParam("name", name)
	err = api.mc.requestWith(summary, req)
	return
}

func (api *AdminAPI) ClearSelectTagFailedKeys() (err error) {
	err = api.mc.request(newRequest(get, proto.AdminClearTagFailedKeys).Header(api.h))
	return
}
