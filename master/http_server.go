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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"

	"github.com/chubaofs/chubaofs/util/buf"

	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"

	"github.com/gorilla/mux"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (m *Server) startHTTPService(modulename string, cfg *config.Config) (err error) {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	m.registerAPIMiddleware(router)
	exporter.InitWithRouter(modulename, cfg, router, m.port)
	var listener net.Listener
	if listener, err = net.Listen("tcp", colonSplit+m.port); err != nil {
		return
	}
	var serveAPI = func(ln net.Listener) {
		if err := http.Serve(ln, router); err != nil && err != http.ErrServerClosed {
			log.LogErrorf("serve http API: serve http server failed: err(%v)", err)
			return
		}
	}
	go serveAPI(listener)
	m.apiListener = listener
	return
}

func (m *Server) registerAPIMiddleware(route *mux.Router) {
	var interceptor mux.MiddlewareFunc = func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v] remoteAddr[%v]",
					r.Method, r.URL.Path, r.URL.Query(), r.RemoteAddr)
				if mux.CurrentRoute(r).GetName() == proto.AdminGetIP {
					next.ServeHTTP(w, r)
					return
				}
				if m.partition.IsRaftLeader() {
					if m.metaReady.Load() {
						next.ServeHTTP(w, r)
						return
					}
					log.LogWarnf("action[interceptor] leader meta has not ready")
					http.Error(w, m.leaderInfo.addr, http.StatusInternalServerError)
					return
				}
				leaderID, _ := m.partition.LeaderTerm()
				if m.leaderInfo.addr == "" || leaderID <= 0 {
					log.LogErrorf("action[interceptor] no leader,request[%v]", r.URL)
					http.Error(w, "no leader", http.StatusInternalServerError)
					return
				}
				m.proxy(w, r)
			})
	}
	route.Use(interceptor)
}

func (m *Server) registerAPIRoutes(router *mux.Router) {
	//graphql api for cluster
	cs := &ClusterService{user: m.user, cluster: m.cluster, conf: m.config, leaderInfo: m.leaderInfo}
	m.registerHandler(router, proto.AdminClusterAPI, cs.Schema())

	us := &UserService{user: m.user, cluster: m.cluster}
	m.registerHandler(router, proto.AdminUserAPI, us.Schema())

	vs := &VolumeService{user: m.user, cluster: m.cluster}
	m.registerHandler(router, proto.AdminVolumeAPI, vs.Schema())

	// cluster management APIs
	router.NewRoute().Name(proto.VersionPath).
		Methods(http.MethodGet).
		Path(proto.VersionPath).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			version := proto.MakeVersion("master")
			marshal, _ := json.Marshal(version)
			if _, err := w.Write(marshal); err != nil {
				log.LogErrorf("write version has err:[%s]", err.Error())
			}
		})

	// cluster management APIs
	router.NewRoute().Name(proto.AdminGetIP).
		Methods(http.MethodGet).
		Path(proto.AdminGetIP).
		HandlerFunc(m.getIPAddr)
	router.NewRoute().Name(proto.AdminGetLimitInfo).
		Methods(http.MethodGet).
		Path(proto.AdminGetLimitInfo).
		HandlerFunc(m.getLimitInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetCluster).
		HandlerFunc(m.getCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterEcSet).
		HandlerFunc(m.updateEcClusterInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterGetScrub).
		HandlerFunc(m.getEcScrubInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterFreeze).
		HandlerFunc(m.setupAutoAllocation)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddRaftNode).
		HandlerFunc(m.addRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RemoveRaftNode).
		HandlerFunc(m.removeRaftNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminClusterStat).HandlerFunc(m.clusterStat)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetClientPkgAddr).
		HandlerFunc(m.setClientPkgAddr)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetClientPkgAddr).
		HandlerFunc(m.getClientPkgAddr)

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
		Path(proto.AdminShrinkVolCapacity).
		HandlerFunc(m.shrinkVolCapacity)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateVolEcInfo).
		HandlerFunc(m.updateVolEcInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetVolConvertSt).
		HandlerFunc(m.setVolConvertTaskState)
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
		Path(proto.AdminApplyVolMutex).
		HandlerFunc(m.applyVolWriteMutex)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminReleaseVolMutex).
		HandlerFunc(m.releaseVolWriteMutex)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetVolMutex).
		HandlerFunc(m.getVolWriteMutexInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetVolConvertMode).
		HandlerFunc(m.setVolConvertMode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetVolMinRWPartition).
		HandlerFunc(m.setVolMinRWPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminSmartVolList).
		HandlerFunc(m.listSmartVols)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminCompactVolList).
		HandlerFunc(m.listCompactVols)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminCompactVolSet).
		HandlerFunc(m.setCompactVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminSetVolChildMaxCnt).
		HandlerFunc(m.setVolChildFileMaxCount)

	// node task response APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetDataNodeTaskResponse).
		HandlerFunc(m.handleDataNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetMetaNodeTaskResponse).
		HandlerFunc(m.handleMetaNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DataNodeValidateCRCReport).
		HandlerFunc(m.handleDataNodeValidateCRCReport)

	// meta partition management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadMetaPartition).
		HandlerFunc(m.loadMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionMetaPartition).
		HandlerFunc(m.decommissionMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSelectMetaReplicaNode).
		HandlerFunc(m.selectMetaReplaceNodeAddr)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartitions).
		HandlerFunc(m.getMetaPartitions)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartition).
		HandlerFunc(m.getMetaPartition)
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
		Path(proto.AdminAddMetaReplicaLearner).
		HandlerFunc(m.addMetaReplicaLearner)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminPromoteMetaReplicaLearner).
		HandlerFunc(m.promoteMetaReplicaLearner)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddDataReplicaLearner).
		HandlerFunc(m.addDataReplicaLearner)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminPromoteDataReplicaLearner).
		HandlerFunc(m.promoteDataReplicaLearner)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseMetaPartition).
		HandlerFunc(m.diagnoseMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminResetMetaPartition).
		HandlerFunc(m.resetMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminManualResetMetaPartition).
		HandlerFunc(m.manualResetMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminResetCorruptMetaNode).
		HandlerFunc(m.resetCorruptMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminMetaPartitionSetReuseState).
		HandlerFunc(m.setMetaPartitionEnableReuseState)

	// data partition management APIs
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetDataPartition).
		HandlerFunc(m.getDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateDataPartition).
		HandlerFunc(m.createDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadDataPartition).
		HandlerFunc(m.loadDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionDataPartition).
		HandlerFunc(m.decommissionDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseDataPartition).
		HandlerFunc(m.diagnoseDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminResetDataPartition).
		HandlerFunc(m.resetDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminFreezeDataPartition).
		HandlerFunc(m.freezeDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUnfreezeDataPartition).
		HandlerFunc(m.unfreezeDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminManualResetDataPartition).
		HandlerFunc(m.manualResetDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDataPartitionUpdate).
		HandlerFunc(m.updateDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolBatchUpdateDps).
		HandlerFunc(m.batchUpdateDataPartitions)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDataPartitionSetIsRecover).
		HandlerFunc(m.setDataPartitionIsRecover)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminMetaPartitionSetIsRecover).
		HandlerFunc(m.setMetaPartitionIsRecover)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminTransferDataPartition).
		HandlerFunc(m.transferDataPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientDataPartitions).
		HandlerFunc(m.getDataPartitions)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminCanMigrateDataPartitions).
		HandlerFunc(m.GetCanMigrateDp)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminCanDelDataPartitions).
		HandlerFunc(m.GetCanDelDp)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminDelDpAlreadyEc).
		HandlerFunc(m.DelDpAlreadyEc)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminDpMigrateEc).
		HandlerFunc(m.DpMigrateEcById)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminDpStopMigrating).
		HandlerFunc(m.DpStopMigrating)

	// meta node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddMetaNode).
		HandlerFunc(m.addMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionMetaNode).
		HandlerFunc(m.decommissionMetaNode)
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
		Path(proto.AdminSetMNRocksDBDiskThreshold).
		HandlerFunc(m.setMetaNodeRocksDBDiskUsedThreshold)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetMNMemModeRocksDBDiskThreshold).
		HandlerFunc(m.setMetaNodeMemModeRocksDBDiskUsedThreshold)

	// data node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddDataNode).
		HandlerFunc(m.addDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDataNode).
		HandlerFunc(m.decommissionDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminResetCorruptDataNode).
		HandlerFunc(m.resetCorruptDataNode)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetDataNode).
		HandlerFunc(m.getDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDisk).
		HandlerFunc(m.decommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeInfo).
		HandlerFunc(m.setNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeInfo).
		HandlerFunc(m.getNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeState).
		HandlerFunc(m.setNodeToOfflineState)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminMergeNodeSet).
		HandlerFunc(m.mergeNodeSet)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterAutoMergeNodeSet).
		HandlerFunc(m.setupAutoMergeNodeSet)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDNStopMigrating).
		HandlerFunc(m.DNStopMigrating)

	// APIs for CodecNodes
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetAllCodecNodes).
		HandlerFunc(m.getAllCodecNodes)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetCodecNode).
		HandlerFunc(m.getCodecNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddCodecNode).
		HandlerFunc(m.addCodecNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionCodecNode).
		HandlerFunc(m.decommissionCodecNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetCodecNodeTaskResponse).
		HandlerFunc(m.handleCodecNodeTaskResponse)

	// APIs for EcNode
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetEcNode).
		HandlerFunc(m.getEcNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddEcNode).
		HandlerFunc(m.addEcNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionEcNode).
		HandlerFunc(m.decommissionEcNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionEcDisk).
		HandlerFunc(m.decommissionEcDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetEcNodeTaskResponse).
		HandlerFunc(m.handleEcNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionEcPartition).
		HandlerFunc(m.decommissionEcPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetEcPartition).
		HandlerFunc(m.getEcPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.ClientEcPartitions).
		HandlerFunc(m.getEcPartitions)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseEcPartition).
		HandlerFunc(m.diagnoseEcPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteEcReplica).
		HandlerFunc(m.deleteEcDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddEcReplica).
		HandlerFunc(m.addEcDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminEcPartitionRollBack).
		HandlerFunc(m.setEcPartitionRollBack)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetAllTaskStatus).
		HandlerFunc(m.GetAllTaskStatus)

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
		Path(proto.SetZoneRegion).
		HandlerFunc(m.setZoneRegion)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UpdateRegion).
		HandlerFunc(m.updateRegion)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetRegionView).
		HandlerFunc(m.getRegion)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.RegionList).
		HandlerFunc(m.regionList)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.CreateRegion).
		HandlerFunc(m.addRegion)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.CreateIDC).
		HandlerFunc(m.addIDC)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DeleteDC).
		HandlerFunc(m.deleteIDC)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.SetZoneIDC).
		HandlerFunc(m.setZoneIDC)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetIDCView).
		HandlerFunc(m.getIDC)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.IDCList).
		HandlerFunc(m.idcList)

	// APIs for token-based client permissions control
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.TokenAddURI).
		HandlerFunc(m.addToken)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.TokenGetURI).
		HandlerFunc(m.getToken)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.TokenDelURI).
		HandlerFunc(m.deleteToken)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.TokenUpdateURI).
		HandlerFunc(m.updateToken)
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
	return &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL.Scheme = "http"
			request.URL.Host = m.leaderInfo.addr
		},
		BufferPool: buf.NewBytePool(10000, 32*1024),
	}
}

func (m *Server) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}
