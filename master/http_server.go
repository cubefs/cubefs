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
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (m *Server) startHTTPService(modulename string, cfg *config.Config) {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	m.registerAPIMiddleware(router)
	if m.cluster.authenticate {
		m.registerAuthenticationMiddleware(router)
	}
	exporter.InitWithRouter(modulename, cfg, router, m.port)
	addr := fmt.Sprintf(":%s", m.port)
	if m.bindIp {
		addr = fmt.Sprintf("%s:%s", m.ip, m.port)
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}

	serveAPI := func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
			return
		}
	}
	go serveAPI()
	m.apiServer = server
	return
}

func (m *Server) isClientPartitionsReq(r *http.Request) bool {
	return r.URL.Path == proto.ClientDataPartitions
}

func (m *Server) isFollowerRead(r *http.Request) (followerRead bool) {
	followerRead = false

	if r.URL.Path == proto.ClientDataPartitions && !m.partition.IsRaftLeader() {
		if volName, err := parseAndExtractName(r); err == nil {
			log.LogInfof("action[interceptor] followerRead vol[%v]", volName)
			if followerRead = m.cluster.followerReadManager.IsVolViewReady(volName); followerRead {
				log.LogInfof("action[interceptor] vol [%v] followerRead [%v], GetName[%v] IsRaftLeader[%v]",
					volName, followerRead, r.URL.Path, m.partition.IsRaftLeader())
				return
			}
		}
	} else if r.URL.Path == proto.AdminChangeMasterLeader ||
		r.URL.Path == proto.AdminOpFollowerPartitionsRead ||
		r.URL.Path == proto.AdminPutDataPartitions ||
		r.URL.Path == "/metrics" {
		followerRead = true
	}
	return
}

func (m *Server) registerAPIMiddleware(route *mux.Router) {
	var interceptor mux.MiddlewareFunc = func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v]", r.Method, r.URL.Path, r.URL.Query())

				if m.partition.IsRaftLeader() {
					if err := m.cluster.apiLimiter.Wait(r.URL.Path); err != nil {
						log.LogWarnf("action[interceptor] too many requests, path[%v]", r.URL.Path)
						errMsg := fmt.Sprintf("too many requests for api: %s", html.EscapeString(r.URL.Path))
						http.Error(w, errMsg, http.StatusTooManyRequests)
						return
					}
				} else {
					if m.cluster.apiLimiter.IsFollowerLimiter(r.URL.Path) {
						if err := m.cluster.apiLimiter.Wait(r.URL.Path); err != nil {
							log.LogWarnf("action[interceptor] too many requests, path[%v]", r.URL.Path)
							errMsg := fmt.Sprintf("too many requests for api: %s", html.EscapeString(r.URL.Path))
							http.Error(w, errMsg, http.StatusTooManyRequests)
							return
						}
					}
				}

				log.LogInfof("action[interceptor] request, remote[%v] method[%v] path[%v] query[%v]",
					r.RemoteAddr, r.Method, r.URL.Path, r.URL.Query())
				if mux.CurrentRoute(r).GetName() == proto.AdminGetIP {
					next.ServeHTTP(w, r)
					return
				}

				isFollowerRead := m.isFollowerRead(r)
				if m.partition.IsRaftLeader() || isFollowerRead {
					if m.metaReady || isFollowerRead {
						log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v]", r.Method, r.URL.Path, r.URL.Query())
						next.ServeHTTP(w, r)
						return
					}
					log.LogWarnf("action[interceptor] leader meta has not ready")
					http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
					return
				} else if m.leaderInfo.addr != "" {
					if m.isClientPartitionsReq(r) {
						log.LogErrorf("action[interceptor] request, method[%v] path[%v] query[%v] status [%v]", r.Method, r.URL.Path, r.URL.Query(), isFollowerRead)
						http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
						return
					}
					m.proxy(w, r)
				} else {
					log.LogErrorf("action[interceptor] no leader,request[%v]", r.URL)
					http.Error(w, "no leader", http.StatusBadRequest)
					return
				}
			})
	}
	route.Use(interceptor)
}

// AuthenticationUri2MsgTypeMap define the mapping from authentication uri to message type
var AuthenticationUri2MsgTypeMap = map[string]proto.MsgType{
	// Master API cluster management
	proto.AdminClusterFreeze: proto.MsgMasterClusterFreezeReq,
	proto.AddRaftNode:        proto.MsgMasterAddRaftNodeReq,
	proto.RemoveRaftNode:     proto.MsgMasterRemoveRaftNodeReq,
	proto.AdminSetNodeInfo:   proto.MsgMasterSetNodeInfoReq,
	proto.AdminSetNodeRdOnly: proto.MsgMasterSetNodeRdOnlyReq,

	// Master API volume management
	proto.AdminCreateVol: proto.MsgMasterCreateVolReq,
	proto.AdminDeleteVol: proto.MsgMasterDeleteVolReq,
	proto.AdminUpdateVol: proto.MsgMasterUpdateVolReq,
	proto.AdminVolShrink: proto.MsgMasterVolShrinkReq,
	proto.AdminVolExpand: proto.MsgMasterVolExpandReq,

	// Master API meta partition management
	proto.AdminLoadMetaPartition:         proto.MsgMasterLoadMetaPartitionReq,
	proto.AdminDecommissionMetaPartition: proto.MsgMasterDecommissionMetaPartitionReq,
	proto.AdminChangeMetaPartitionLeader: proto.MsgMasterChangeMetaPartitionLeaderReq,
	proto.AdminCreateMetaPartition:       proto.MsgMasterCreateMetaPartitionReq,
	proto.AdminAddMetaReplica:            proto.MsgMasterAddMetaReplicaReq,
	proto.AdminDeleteMetaReplica:         proto.MsgMasterDeleteMetaReplicaReq,
	proto.QosUpdate:                      proto.MsgMasterQosUpdateReq,
	proto.QosUpdateZoneLimit:             proto.MsgMasterQosUpdateZoneLimitReq,
	proto.QosUpdateMasterLimit:           proto.MsgMasterQosUpdateMasterLimitReq,
	proto.QosUpdateClientParam:           proto.MsgMasterQosUpdateClientParamReq,

	// Master API data partition management
	proto.AdminCreateDataPartition:       proto.MsgMasterCreateDataPartitionReq,
	proto.AdminDataPartitionChangeLeader: proto.MsgMasterDataPartitionChangeLeaderReq,
	proto.AdminLoadDataPartition:         proto.MsgMasterLoadDataPartitionReq,
	proto.AdminDecommissionDataPartition: proto.MsgMasterDecommissionDataPartitionReq,
	proto.AdminAddDataReplica:            proto.MsgMasterAddDataReplicaReq,
	proto.AdminDeleteDataReplica:         proto.MsgMasterDeleteDataReplicaReq,
	proto.AdminSetDpRdOnly:               proto.MsgMasterSetDpRdOnlyReq,

	// Master API meta node management
	proto.AddMetaNode:               proto.MsgMasterAddMetaNodeReq,
	proto.DecommissionMetaNode:      proto.MsgMasterDecommissionMetaNodeReq,
	proto.MigrateMetaNode:           proto.MsgMasterMigrateMetaNodeReq,
	proto.AdminSetMetaNodeThreshold: proto.MsgMasterSetMetaNodeThresholdReq,
	proto.AdminUpdateMetaNode:       proto.MsgMasterUpdateMetaNodeReq,

	// Master API data node management
	proto.AddDataNode:                   proto.MsgMasterAddDataNodeReq,
	proto.DecommissionDataNode:          proto.MsgMasterDecommissionDataNodeReq,
	proto.MigrateDataNode:               proto.MsgMasterMigrateDataNodeReq,
	proto.CancelDecommissionDataNode:    proto.MsgMasterCancelDecommissionDataNodeReq,
	proto.DecommissionDisk:              proto.MsgMasterDecommissionDiskReq,
	proto.AdminUpdateNodeSetCapcity:     proto.MsgMasterUpdateNodeSetCapcityReq,
	proto.AdminUpdateNodeSetId:          proto.MsgMasterUpdateNodeSetIdReq,
	proto.AdminUpdateDomainDataUseRatio: proto.MsgMasterUpdateDomainDataUseRatioReq,
	proto.AdminUpdateZoneExcludeRatio:   proto.MsgMasterUpdateZoneExcludeRatioReq,
	proto.RecommissionDisk:              proto.MsgMasterRecommissionDiskReq,

	// Master API user management
	proto.UserCreate:          proto.MsgMasterUserCreateReq,
	proto.UserDelete:          proto.MsgMasterUserDeleteReq,
	proto.UserUpdate:          proto.MsgMasterUserUpdateReq,
	proto.UserUpdatePolicy:    proto.MsgMasterUserUpdatePolicyReq,
	proto.UserRemovePolicy:    proto.MsgMasterUserRemovePolicyReq,
	proto.UserDeleteVolPolicy: proto.MsgMasterUserDeleteVolPolicyReq,
	proto.UserTransferVol:     proto.MsgMasterUserTransferVolReq,

	// Master API zone management
	proto.UpdateZone: proto.MsgMasterUpdateZoneReq,
}

func (m *Server) registerAuthenticationMiddleware(router *mux.Router) {
	authenticationInterceptor := func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				split := strings.Split(r.RequestURI, "?")
				uriPath := split[0]
				msgType, match := AuthenticationUri2MsgTypeMap[uriPath]
				if match {
					if err := m.cluster.parseAndCheckClientIDKey(r, msgType); err != nil {
						log.LogInfof("action[AuthenticationInterceptor] parseAndCheckClientKey failed, RequestURI[%v], err[%v]",
							r.RequestURI, err)
						sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidClientIDKey, Msg: err.Error()})
						return
					}
				}
				next.ServeHTTP(w, r)
			})
	}
	router.Use(authenticationInterceptor)
}

func (m *Server) registerAPIRoutes(router *mux.Router) {
	// graphql api for cluster
	cs := &ClusterService{user: m.user, cluster: m.cluster, conf: m.config, leaderInfo: m.leaderInfo}
	m.registerHandler(router, proto.AdminClusterAPI, cs.Schema())

	us := &UserService{user: m.user, cluster: m.cluster}
	m.registerHandler(router, proto.AdminUserAPI, us.Schema())

	// vs := &VolumeService{user: m.user, cluster: m.cluster}
	// m.registerHandler(router, proto.AdminVolumeAPI, vs.Schema())

	// cluster management APIs
	router.NewRoute().Name(proto.AdminGetMasterApiList).
		Methods(http.MethodGet).
		Path(proto.AdminGetMasterApiList).
		HandlerFunc(m.getApiList)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetApiQpsLimit).
		HandlerFunc(m.setApiQpsLimit)
	router.NewRoute().Name(proto.AdminGetApiQpsLimit).
		Methods(http.MethodGet).
		Path(proto.AdminGetApiQpsLimit).
		HandlerFunc(m.getApiQpsLimit)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminRemoveApiQpsLimit).
		HandlerFunc(m.rmApiQpsLimit)
	router.NewRoute().Name(proto.AdminGetIP).
		Methods(http.MethodGet).
		Path(proto.AdminGetIP).
		HandlerFunc(m.getIPAddr)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetCluster).
		HandlerFunc(m.getCluster)
	router.NewRoute().Name(proto.AdminACL).
		Methods(http.MethodGet).
		Path(proto.AdminACL).
		HandlerFunc(m.aclOperate)
	router.NewRoute().Name(proto.AdminUid).
		Methods(http.MethodGet).
		Path(proto.AdminUid).
		HandlerFunc(m.UidOperate)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetClusterInfo).
		HandlerFunc(m.setClusterInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetMonitorPushAddr).
		HandlerFunc(m.getMonitorPushAddr)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterFreeze).
		HandlerFunc(m.setupAutoAllocation)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolForbidden).
		HandlerFunc(m.forbidVolume)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolEnableAuditLog).
		HandlerFunc(m.setEnableAuditLogForVolume)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterForbidMpDecommission).
		HandlerFunc(m.setupForbidMetaPartitionDecommission)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddRaftNode).
		HandlerFunc(m.addRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RemoveRaftNode).
		HandlerFunc(m.removeRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RaftStatus).
		HandlerFunc(m.getRaftStatus)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminClusterStat).HandlerFunc(m.clusterStat)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetCheckDataReplicasEnable).
		HandlerFunc(m.setCheckDataReplicasEnable)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetConfig).
		HandlerFunc(m.setConfigHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetConfig).
		HandlerFunc(m.getConfigHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDecommissionLimit).
		HandlerFunc(m.updateDecommissionLimit)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminQueryDecommissionLimit).
		HandlerFunc(m.queryDecommissionLimit)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminQueryDecommissionToken).
		HandlerFunc(m.queryDecommissionToken)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetFileStats).
		HandlerFunc(m.setFileStats)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetFileStats).
		HandlerFunc(m.getFileStats)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetClusterUuidEnable).
		HandlerFunc(m.setClusterUuidEnable)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetClusterUuid).
		HandlerFunc(m.getClusterUuid)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGenerateClusterUuid).
		HandlerFunc(m.generateClusterUuid)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetClusterValue).
		HandlerFunc(m.GetClusterValue)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDecommissionDiskFactor).
		HandlerFunc(m.updateDecommissionDiskFactor)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminQueryDecommissionDiskLimit).
		HandlerFunc(m.queryDecommissionDiskLimit)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminEnableAutoDecommissionDisk).
		HandlerFunc(m.enableAutoDecommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminQueryAutoDecommissionDisk).
		HandlerFunc(m.queryAutoDecommissionDisk)

	// volume management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateVol).
		HandlerFunc(m.createVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetVol).
		HandlerFunc(m.getVolSimpleInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteVol).
		HandlerFunc(m.markDeleteVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateVol).
		HandlerFunc(m.updateVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolShrink).
		HandlerFunc(m.volShrink)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolExpand).
		HandlerFunc(m.volExpand)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.ClientVol).
		HandlerFunc(m.getVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientVolStat).
		HandlerFunc(m.getVolStatInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetTopologyView).
		HandlerFunc(m.getTopology)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminListVols).
		HandlerFunc(m.listVols)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminChangeMasterLeader).
		HandlerFunc(m.changeMasterLeader)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminOpFollowerPartitionsRead).
		HandlerFunc(m.OpFollowerPartitionsRead)

	// multi version snapshot APIs
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminCreateVersion).
		HandlerFunc(m.CreateVersion)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminDelVersion).
		HandlerFunc(m.DelVersion)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetVersionInfo).
		HandlerFunc(m.GetVersionInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetAllVersionInfo).
		HandlerFunc(m.GetAllVersionInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetVolVer).
		HandlerFunc(m.getVolVer)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetVerStrategy).
		HandlerFunc(m.SetVerStrategy)

	// S3 lifecycle configuration APIS
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.SetBucketLifecycle).
		HandlerFunc(m.SetBucketLifecycle)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetBucketLifecycle).
		HandlerFunc(m.GetBucketLifecycle)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DeleteBucketLifecycle).
		HandlerFunc(m.DelBucketLifecycle)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddLcNode).
		HandlerFunc(m.addLcNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLcNode).
		HandlerFunc(m.lcnodeInfo)

	// node task response APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetDataNodeTaskResponse).
		HandlerFunc(m.handleDataNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetMetaNodeTaskResponse).
		HandlerFunc(m.handleMetaNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetLcNodeTaskResponse).
		HandlerFunc(m.handleLcNodeTaskResponse)

	// meta partition management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadMetaPartition).
		HandlerFunc(m.loadMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionMetaPartition).
		HandlerFunc(m.decommissionMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminChangeMetaPartitionLeader).
		HandlerFunc(m.changeMetaPartitionLeader)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminBalanceMetaPartitionLeader).
		HandlerFunc(m.balanceMetaPartitionLeader)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartitions).
		HandlerFunc(m.getMetaPartitions)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartition).
		HandlerFunc(m.getMetaPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosUpload).
		HandlerFunc(m.qosUpload)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosGetStatus).
		HandlerFunc(m.getQosStatus)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosGetClientsLimitInfo).
		HandlerFunc(m.getClientQosInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosUpdate).
		HandlerFunc(m.QosUpdate)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosUpdateZoneLimit).
		HandlerFunc(m.QosUpdateZoneLimit)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosGetZoneLimitInfo).
		HandlerFunc(m.QosGetZoneLimit)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosUpdateMasterLimit).
		HandlerFunc(m.getQosUpdateMasterLimit)
	// router.NewRoute().Methods(http.MethodGet).
	//	Path(proto.QosUpdateMagnify).
	//	HandlerFunc(m.QosUpdateMagnify)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QosUpdateClientParam).
		HandlerFunc(m.QosUpdateClientParam)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateMetaPartition).
		HandlerFunc(m.createMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddMetaReplica).
		HandlerFunc(m.addMetaReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteMetaReplica).
		HandlerFunc(m.deleteMetaReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseMetaPartition).
		HandlerFunc(m.diagnoseMetaPartition)

	// data partition management APIs
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetDataPartition).
		HandlerFunc(m.getDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateDataPartition).
		HandlerFunc(m.createDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreatePreLoadDataPartition).
		HandlerFunc(m.createPreLoadDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDataPartitionChangeLeader).
		HandlerFunc(m.changeDataPartitionLeader)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadDataPartition).
		HandlerFunc(m.loadDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionDataPartition).
		HandlerFunc(m.decommissionDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseDataPartition).
		HandlerFunc(m.diagnoseDataPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientDataPartitions).
		HandlerFunc(m.getDataPartitions)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminResetDataPartitionDecommissionStatus).
		HandlerFunc(m.resetDataPartitionDecommissionStatus)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminQueryDataPartitionDecommissionStatus).
		HandlerFunc(m.queryDataPartitionDecommissionStatus)

	// meta node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddMetaNode).
		HandlerFunc(m.addMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionMetaNode).
		HandlerFunc(m.decommissionMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.MigrateMetaNode).
		HandlerFunc(m.migrateMetaNodeHandler)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetMetaNode).
		HandlerFunc(m.getMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetMetaNodeThreshold).
		HandlerFunc(m.setMetaNodeThreshold)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddDataReplica).
		HandlerFunc(m.addDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteDataReplica).
		HandlerFunc(m.deleteDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateMetaNode).
		HandlerFunc(m.updateMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDataNode).
		HandlerFunc(m.updateDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetInvalidNodes).
		HandlerFunc(m.checkInvalidIDNodes)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminPutDataPartitions).
		HandlerFunc(m.putDataPartitions)

	// data node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddDataNode).
		HandlerFunc(m.addDataNode)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDataNode).
		HandlerFunc(m.decommissionDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryDataNodeDecoProgress).
		HandlerFunc(m.queryDataNodeDecoProgress)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.MigrateDataNode).
		HandlerFunc(m.migrateDataNodeHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.CancelDecommissionDataNode).
		HandlerFunc(m.cancelDecommissionDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryDataNodeDecoFailedDps).
		HandlerFunc(m.queryDataNodeDecoFailedDps)

	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetDataNode).
		HandlerFunc(m.getDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDisk).
		HandlerFunc(m.decommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RecommissionDisk).
		HandlerFunc(m.recommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RestoreStoppedAutoDecommissionDisk).
		HandlerFunc(m.restoreStoppedAutoDecommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryDiskDecoProgress).
		HandlerFunc(m.queryDiskDecoProgress)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.MarkDecoDiskFixed).
		HandlerFunc(m.markDecoDiskFixed)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.CancelDecommissionDisk).
		HandlerFunc(m.cancelDecommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryDecommissionDiskDecoFailedDps).
		HandlerFunc(m.queryDecommissionDiskDecoFailedDps)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryBadDisks).
		HandlerFunc(m.queryBadDisks)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryAllDecommissionDisk).
		HandlerFunc(m.queryAllDecommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QueryDisableDisk).
		HandlerFunc(m.queryDisableDisk)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeInfo).
		HandlerFunc(m.setNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeInfo).
		HandlerFunc(m.getNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetIsDomainOn).
		HandlerFunc(m.getIsDomainOn)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetAllNodeSetGrpInfo).
		HandlerFunc(m.getAllNodeSetGrpInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeSetGrpInfo).
		HandlerFunc(m.getNodeSetGrpInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateNodeSetCapcity).
		HandlerFunc(m.updateNodeSetCapacityHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateNodeSetId).
		HandlerFunc(m.updateNodeSetIdHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateNodeSetNodeSelector).
		HandlerFunc(m.updateNodeSetNodeSelector)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDomainDataUseRatio).
		HandlerFunc(m.updateDataUseRatioHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateZoneExcludeRatio).
		HandlerFunc(m.updateZoneExcludeRatioHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeRdOnly).
		HandlerFunc(m.setNodeRdOnlyHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetDpRdOnly).
		HandlerFunc(m.setDpRdOnlyHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetDpDiscard).
		HandlerFunc(m.setDpDiscardHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetDiscardDp).
		HandlerFunc(m.getDiscardDpHandler)

	// user management APIs
	router.NewRoute().Methods(http.MethodPost).
		Path(proto.UserCreate).
		HandlerFunc(m.createUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserDelete).
		HandlerFunc(m.deleteUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserUpdate).
		HandlerFunc(m.updateUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserUpdatePolicy).
		HandlerFunc(m.updateUserPolicy)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserRemovePolicy).
		HandlerFunc(m.removeUserPolicy)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserDeleteVolPolicy).
		HandlerFunc(m.deleteUserVolPolicy)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserGetAKInfo).
		HandlerFunc(m.getUserAKInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserGetInfo).
		HandlerFunc(m.getUserInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserList).
		HandlerFunc(m.getAllUsers)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserTransferVol).
		HandlerFunc(m.transferUserVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UsersOfVol).
		HandlerFunc(m.getUsersOfVol)

	// zone management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UpdateZone).
		HandlerFunc(m.updateZone)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetAllZones).
		HandlerFunc(m.listZone)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetAllNodeSets).
		HandlerFunc(m.listNodeSets)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetNodeSet).
		HandlerFunc(m.getNodeSet)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UpdateNodeSet).
		HandlerFunc(m.updateNodeSet)

	// Quota
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QuotaCreate).
		HandlerFunc(m.CreateQuota)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QuotaUpdate).
		HandlerFunc(m.UpdateQuota)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.QuotaDelete).
		HandlerFunc(m.DeleteQuota)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QuotaList).
		HandlerFunc(m.ListQuota)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QuotaGet).
		HandlerFunc(m.GetQuota)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.QuotaListAll).
		HandlerFunc(m.ListQuotaAll)

	// S3 API QoS Manager
	router.NewRoute().Methods(http.MethodPut, http.MethodPost).
		Path(proto.S3QoSSet).
		HandlerFunc(m.S3QosSet)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.S3QoSGet).
		HandlerFunc(m.S3QosGet)
	router.NewRoute().Methods(http.MethodDelete, http.MethodPost).
		Path(proto.S3QoSDelete).
		HandlerFunc(m.S3QosDelete)

	// APIs for FlashNode
	router.NewRoute().Methods(http.MethodPost).Path(proto.FlashNodeAdd).HandlerFunc(m.addFlashNode)
	router.NewRoute().Methods(http.MethodPost).Path(proto.FlashNodeSet).HandlerFunc(m.setFlashNode)
	router.NewRoute().Methods(http.MethodPost).Path(proto.FlashNodeRemove).HandlerFunc(m.removeFlashNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.FlashNodeGet).HandlerFunc(m.getFlashNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.FlashNodeList).HandlerFunc(m.listFlashNodes)

	// APIs for FlashGroup
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupTurn).HandlerFunc(m.turnFlashGroup)
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupCreate).HandlerFunc(m.createFlashGroup)
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupSet).HandlerFunc(m.setFlashGroup)
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupRemove).HandlerFunc(m.removeFlashGroup)
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupNodeAdd).HandlerFunc(m.flashGroupAddFlashNode)
	router.NewRoute().Methods(http.MethodPost).Path(proto.AdminFlashGroupNodeRemove).HandlerFunc(m.flashGroupRemoveFlashNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminFlashGroupGet).HandlerFunc(m.getFlashGroup)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminFlashGroupList).HandlerFunc(m.listFlashGroups)
	router.NewRoute().Methods(http.MethodGet).Path(proto.ClientFlashGroups).HandlerFunc(m.clientFlashGroups)
}

func (m *Server) registerHandler(router *mux.Router, model string, schema *graphql.Schema) {
	introspection.AddIntrospectionToSchema(schema)

	gHandler := graphql.HTTPHandler(schema)
	router.NewRoute().Name(model).Methods(http.MethodGet, http.MethodPost).Path(model).HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		userID := request.Header.Get(proto.UserKey)
		if userID == "" {
			ErrResponse(writer, fmt.Errorf("not found [%s] in header", proto.UserKey))
			return
		}

		if ui, err := m.user.getUserInfo(userID); err != nil {
			ErrResponse(writer, fmt.Errorf("user:[%s] not found ", userID))
			return
		} else {
			request = request.WithContext(context.WithValue(request.Context(), proto.UserInfoKey, ui))
		}

		gHandler.ServeHTTP(writer, request)
	})
}

func ErrResponse(w http.ResponseWriter, err error) {
	response := struct {
		Errors []string `json:"errors"`
	}{
		Errors: []string{err.Error()},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if _, e := w.Write(responseJSON); e != nil {
		log.LogErrorf("send response has err:[%s]", e)
	}
}

func (m *Server) newReverseProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.URL.Scheme = "http"
		request.URL.Host = m.leaderInfo.addr
	}}
}

func (m *Server) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}
