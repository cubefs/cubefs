// Copyright 2018 The Containerfs Authors.
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
	"fmt"
	"net/http"

	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"net/http/httputil"
)

const (
	// Admin APIs
	AdminGetCluster           = "/admin/getCluster"
	AdminGetDataPartition     = "/dataPartition/get"
	AdminLoadDataPartition    = "/dataPartition/load"
	AdminCreateDataPartition  = "/dataPartition/create"
	AdminDataPartitionOffline = "/dataPartition/offline"
	AdminDeleteVol            = "/vol/delete"
	AdminUpdateVol            = "/vol/update"
	AdminCreateVol            = "/admin/createVol"
	AdminClusterFreeze        = "/cluster/freeze"
	AdminGetIp                = "/admin/getIp"
	AdminCreateMP             = "/metaPartition/create"
	AdminSetCompactStatus     = "/compactStatus/set"
	AdminGetCompactStatus     = "/compactStatus/get"
	AdminSetMetaNodeThreshold = "/threshold/set"

	// Client APIs
	ClientDataPartitions = "/client/dataPartitions"
	ClientVol            = "/client/vol"
	ClientMetaPartition  = "/client/metaPartition"
	ClientVolStat        = "/client/volStat"

	//raft node APIs
	RaftNodeAdd    = "/raftNode/add"
	RaftNodeRemove = "/raftNode/remove"

	// Node APIs
	AddDataNode               = "/dataNode/add"
	DataNodeOffline           = "/dataNode/offline"
	DiskOffLine               = "/disk/offline"
	GetDataNode               = "/dataNode/get"
	AddMetaNode               = "/metaNode/add"
	MetaNodeOffline           = "/metaNode/offline"
	GetMetaNode               = "/metaNode/get"
	AdminLoadMetaPartition    = "/metaPartition/load"
	AdminMetaPartitionOffline = "/metaPartition/offline"

	// Operation response
	MetaNodeResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	DataNodeResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
)

func (m *Master) startHttpService() (err error) {
	go func() {
		m.handleFunctions()
		http.ListenAndServe(ColonSplit+m.port, nil)
	}()
	return
}

func (m *Master) handleFunctions() {
	http.HandleFunc(AdminGetIp, m.getIpAndClusterName)
	http.HandleFunc(AdminGetCluster, m.getCluster)
	http.Handle(AdminGetDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminCreateDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminLoadDataPartition, m.handlerWithInterceptor())
	http.Handle(AdminDataPartitionOffline, m.handlerWithInterceptor())
	http.Handle(AdminCreateVol, m.handlerWithInterceptor())
	http.Handle(AdminDeleteVol, m.handlerWithInterceptor())
	http.Handle(AdminUpdateVol, m.handlerWithInterceptor())
	http.Handle(AdminClusterFreeze, m.handlerWithInterceptor())
	http.Handle(AddDataNode, m.handlerWithInterceptor())
	http.Handle(AddMetaNode, m.handlerWithInterceptor())
	http.Handle(DataNodeOffline, m.handlerWithInterceptor())
	http.Handle(DiskOffLine, m.handlerWithInterceptor())
	http.Handle(MetaNodeOffline, m.handlerWithInterceptor())
	http.Handle(GetDataNode, m.handlerWithInterceptor())
	http.Handle(GetMetaNode, m.handlerWithInterceptor())
	//http.Handle(AdminLoadMetaPartition, m.handlerWithInterceptor())
	http.Handle(AdminMetaPartitionOffline, m.handlerWithInterceptor())
	http.Handle(ClientDataPartitions, m.handlerWithInterceptor())
	http.Handle(ClientVol, m.handlerWithInterceptor())
	http.Handle(ClientMetaPartition, m.handlerWithInterceptor())
	http.Handle(DataNodeResponse, m.handlerWithInterceptor())
	http.Handle(MetaNodeResponse, m.handlerWithInterceptor())
	http.Handle(AdminCreateMP, m.handlerWithInterceptor())
	http.Handle(ClientVolStat, m.handlerWithInterceptor())
	http.Handle(RaftNodeAdd, m.handlerWithInterceptor())
	http.Handle(RaftNodeRemove, m.handlerWithInterceptor())
	http.Handle(AdminSetCompactStatus, m.handlerWithInterceptor())
	http.Handle(AdminGetCompactStatus, m.handlerWithInterceptor())
	http.Handle(AdminSetMetaNodeThreshold, m.handlerWithInterceptor())
	http.Handle(GetTopologyView, m.handlerWithInterceptor())

	return
}

func (m *Master) newReverseProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.URL.Scheme = "http"
		request.URL.Host = m.leaderInfo.addr
	}}
}

func (m *Master) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {

			if m.partition.IsLeader() {
				m.ServeHTTP(w, r)
				return
			}
			if m.leaderInfo.addr == "" {
				log.LogErrorf("action[handlerWithInterceptor] no leader,request[%v]", r.URL)
				http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
				return
			} else {
				m.proxy(w, r)
			}
		})
}

func (m *Master) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("URL[%v],remoteAddr[%v]", r.URL, r.RemoteAddr)
	switch r.URL.Path {
	case AdminGetCluster:
		m.getCluster(w, r)
	case AdminCreateDataPartition:
		m.createDataPartition(w, r)
	case AdminGetDataPartition:
		m.getDataPartition(w, r)
	case AdminLoadDataPartition:
		m.loadDataPartition(w, r)
	case AdminDataPartitionOffline:
		m.dataPartitionOffline(w, r)
	case AdminCreateVol:
		m.createVol(w, r)
	case AdminDeleteVol:
		m.markDeleteVol(w, r)
	case AdminUpdateVol:
		m.updateVol(w, r)
	case AdminClusterFreeze:
		m.setDisableAutoAlloc(w, r)
	case AddDataNode:
		m.addDataNode(w, r)
	case GetDataNode:
		m.getDataNode(w, r)
	case DataNodeOffline:
		m.dataNodeOffline(w, r)
	case DiskOffLine:
		m.diskOffline(w, r)
	case DataNodeResponse:
		m.dataNodeTaskResponse(w, r)
	case AddMetaNode:
		m.addMetaNode(w, r)
	case GetMetaNode:
		m.getMetaNode(w, r)
	case MetaNodeOffline:
		m.metaNodeOffline(w, r)
	case MetaNodeResponse:
		m.metaNodeTaskResponse(w, r)
	case ClientDataPartitions:
		m.getDataPartitions(w, r)
	case ClientVol:
		m.getVol(w, r)
	case ClientMetaPartition:
		m.getMetaPartition(w, r)
	case ClientVolStat:
		m.getVolStatInfo(w, r)
	case AdminLoadMetaPartition:
		m.loadMetaPartition(w, r)
	case AdminMetaPartitionOffline:
		m.metaPartitionOffline(w, r)
	case AdminCreateMP:
		m.createMetaPartition(w, r)
	case RaftNodeAdd:
		m.handleAddRaftNode(w, r)
	case RaftNodeRemove:
		m.handleRemoveRaftNode(w, r)
	case AdminSetCompactStatus:
		m.setCompactStatus(w, r)
	case AdminGetCompactStatus:
		m.getCompactStatus(w, r)
	case AdminSetMetaNodeThreshold:
		m.setMetaNodeThreshold(w, r)
	case GetTopologyView:
		m.getTopology(w, r)
	default:

	}
}

func getReturnMessage(requestType, remoteAddr, message string, code int) (logMsg string) {
	logMsg = fmt.Sprintf("type[%s] From [%s] httpCode[%d] Because [%s] ", requestType, remoteAddr, code, message)
	return
}

func HandleError(message string, err error, code int, w http.ResponseWriter) {
	log.LogErrorf("errMsg:%v errStack:%v", message, errors.ErrorStack(err))
	http.Error(w, message, code)
}
