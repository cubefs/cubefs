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
	"net/http"
	"net/http/httputil"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (m *Server) startHTTPService() {
	go func() {
		m.handleFunctions()
		if err := http.ListenAndServe(colonSplit+m.port, nil); err != nil {
			log.LogErrorf("action[startHTTPService] failed,err[%v]", err)
			panic(err)
		}
	}()
	return
}

func (m *Server) handleFunctions() {
	http.HandleFunc(proto.AdminGetIP, m.getIPAddr)
	http.Handle(proto.AdminGetCluster, m.handlerWithInterceptor())
	http.Handle(proto.AdminGetDataPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminCreateDataPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminLoadDataPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminDecommissionDataPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminAddDataReplica, m.handlerWithInterceptor())
	http.Handle(proto.AdminDeleteDataReplica, m.handlerWithInterceptor())
	http.Handle(proto.AdminCreateVol, m.handlerWithInterceptor())
	http.Handle(proto.AdminGetVol, m.handlerWithInterceptor())
	http.Handle(proto.AdminDeleteVol, m.handlerWithInterceptor())
	http.Handle(proto.AdminUpdateVol, m.handlerWithInterceptor())
	http.Handle(proto.AdminClusterFreeze, m.handlerWithInterceptor())
	http.Handle(proto.AddDataNode, m.handlerWithInterceptor())
	http.Handle(proto.AddMetaNode, m.handlerWithInterceptor())
	http.Handle(proto.DecommissionDataNode, m.handlerWithInterceptor())
	http.Handle(proto.DecommissionDisk, m.handlerWithInterceptor())
	http.Handle(proto.DecommissionMetaNode, m.handlerWithInterceptor())
	http.Handle(proto.GetDataNode, m.handlerWithInterceptor())
	http.Handle(proto.GetMetaNode, m.handlerWithInterceptor())
	http.Handle(proto.AdminLoadMetaPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminDecommissionMetaPartition, m.handlerWithInterceptor())
	http.Handle(proto.AdminAddMetaReplica, m.handlerWithInterceptor())
	http.Handle(proto.AdminDeleteMetaReplica, m.handlerWithInterceptor())
	http.Handle(proto.ClientDataPartitions, m.handlerWithInterceptor())
	http.Handle(proto.ClientVol, m.handlerWithInterceptor())
	http.Handle(proto.ClientMetaPartitions, m.handlerWithInterceptor())
	http.Handle(proto.ClientMetaPartition, m.handlerWithInterceptor())
	http.Handle(proto.GetDataNodeTaskResponse, m.handlerWithInterceptor())
	http.Handle(proto.GetMetaNodeTaskResponse, m.handlerWithInterceptor())
	http.Handle(proto.AdminCreateMetaPartition, m.handlerWithInterceptor())
	http.Handle(proto.ClientVolStat, m.handlerWithInterceptor())
	http.Handle(proto.AddRaftNode, m.handlerWithInterceptor())
	http.Handle(proto.RemoveRaftNode, m.handlerWithInterceptor())
	http.Handle(proto.AdminSetMetaNodeThreshold, m.handlerWithInterceptor())
	http.Handle(proto.GetTopologyView, m.handlerWithInterceptor())
	http.Handle(proto.UserCreate, m.handlerWithInterceptor())
	http.Handle(proto.UserCreateWithKey, m.handlerWithInterceptor())
	http.Handle(proto.UserDelete, m.handlerWithInterceptor())
	http.Handle(proto.UserAddPolicy, m.handlerWithInterceptor())
	http.Handle(proto.UserDeletePolicy, m.handlerWithInterceptor())
	http.Handle(proto.UserDeleteVolPolicy, m.handlerWithInterceptor())
	http.Handle(proto.UserGetAKInfo, m.handlerWithInterceptor())
	http.Handle(proto.UserGetInfo, m.handlerWithInterceptor())
	http.Handle(proto.UserTransferVol, m.handlerWithInterceptor())
	http.Handle(proto.UpdateZone, m.handlerWithInterceptor())
	http.Handle(proto.GetAllZones, m.handlerWithInterceptor())
	return
}

func (m *Server) newReverseProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.URL.Scheme = "http"
		request.URL.Host = m.leaderInfo.addr
	}}
}

func (m *Server) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.partition.IsRaftLeader() {
				if m.metaReady {
					m.ServeHTTP(w, r)
					return
				}
				log.LogWarnf("action[handlerWithInterceptor] leader meta has not ready")
				http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
				return
			}
			if m.leaderInfo.addr == "" {
				log.LogErrorf("action[handlerWithInterceptor] no leader,request[%v]", r.URL)
				http.Error(w, "no leader", http.StatusBadRequest)
				return
			}
			m.proxy(w, r)
		})
}

func (m *Server) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}

func (m *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("URL[%v],remoteAddr[%v]", r.URL, r.RemoteAddr)
	switch r.URL.Path {
	case proto.AdminGetCluster:
		m.getCluster(w, r)
	case proto.AdminCreateDataPartition:
		m.createDataPartition(w, r)
	case proto.AdminGetDataPartition:
		m.getDataPartition(w, r)
	case proto.AdminLoadDataPartition:
		m.loadDataPartition(w, r)
	case proto.AdminDecommissionDataPartition:
		m.decommissionDataPartition(w, r)
	case proto.AdminAddDataReplica:
		m.addDataReplica(w, r)
	case proto.AdminDeleteDataReplica:
		m.deleteDataReplica(w, r)
	case proto.AdminCreateVol:
		m.createVol(w, r)
	case proto.AdminGetVol:
		m.getVolSimpleInfo(w, r)
	case proto.AdminDeleteVol:
		m.markDeleteVol(w, r)
	case proto.AdminUpdateVol:
		m.updateVol(w, r)
	case proto.AdminClusterFreeze:
		m.setupAutoAllocation(w, r)
	case proto.AddDataNode:
		m.addDataNode(w, r)
	case proto.GetDataNode:
		m.getDataNode(w, r)
	case proto.DecommissionDataNode:
		m.decommissionDataNode(w, r)
	case proto.DecommissionDisk:
		m.decommissionDisk(w, r)
	case proto.GetDataNodeTaskResponse:
		m.handleDataNodeTaskResponse(w, r)
	case proto.AddMetaNode:
		m.addMetaNode(w, r)
	case proto.GetMetaNode:
		m.getMetaNode(w, r)
	case proto.DecommissionMetaNode:
		m.decommissionMetaNode(w, r)
	case proto.GetMetaNodeTaskResponse:
		m.handleMetaNodeTaskResponse(w, r)
	case proto.ClientDataPartitions:
		m.getDataPartitions(w, r)
	case proto.ClientVol:
		m.getVol(w, r)
	case proto.ClientMetaPartitions:
		m.getMetaPartitions(w, r)
	case proto.ClientMetaPartition:
		m.getMetaPartition(w, r)
	case proto.ClientVolStat:
		m.getVolStatInfo(w, r)
	case proto.AdminLoadMetaPartition:
		m.loadMetaPartition(w, r)
	case proto.AdminDecommissionMetaPartition:
		m.decommissionMetaPartition(w, r)
	case proto.AdminCreateMetaPartition:
		m.createMetaPartition(w, r)
	case proto.AdminAddMetaReplica:
		m.addMetaReplica(w, r)
	case proto.AdminDeleteMetaReplica:
		m.deleteMetaReplica(w, r)
	case proto.AddRaftNode:
		m.addRaftNode(w, r)
	case proto.RemoveRaftNode:
		m.removeRaftNode(w, r)
	case proto.AdminSetMetaNodeThreshold:
		m.setMetaNodeThreshold(w, r)
	case proto.GetTopologyView:
		m.getTopology(w, r)
	case proto.UserCreate:
		m.createUser(w, r)
	case proto.UserCreateWithKey:
		m.createUserWithKey(w, r)
	case proto.UserDelete:
		m.deleteUser(w, r)
	case proto.UserAddPolicy:
		m.addUserPolicy(w, r)
	case proto.UserDeletePolicy:
		m.deleteUserPolicy(w, r)
	case proto.UserDeleteVolPolicy:
		m.deleteUserVolPolicy(w, r)
	case proto.UserGetAKInfo:
		m.getUserAKInfo(w, r)
	case proto.UserGetInfo:
		m.getUserInfo(w, r)
	case proto.UserTransferVol:
		m.transferUserVol(w, r)
	case proto.UpdateZone:
		m.updateZone(w, r)
	case proto.GetAllZones:
		m.listZone(w, r)
	default:
	}
}
