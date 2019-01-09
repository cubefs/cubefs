// Copyright 2018 The Container File System Authors.
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

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"net/http/httputil"
)

// api
const (
	// Admin APIs
	adminGetCluster                = "/admin/getCluster"
	AdminGetDataPartition          = "/dataPartition/get"
	adminLoadDataPartition         = "/dataPartition/load"
	adminCreateDataPartition       = "/dataPartition/create"
	adminDecommissionDataPartition = "/dataPartition/decommission"
	adminDeleteVol                 = "/vol/delete"
	adminUpdateVol                 = "/vol/update"
	adminCreateVol                 = "/admin/createVol"
	adminClusterFreeze             = "/cluster/freeze"
	AdminGetIP                     = "/admin/getIp"
	adminCreateMP                  = "/metaPartition/create"
	adminSetMetaNodeThreshold      = "/threshold/set"

	// Client APIs
	clientDataPartitions = "/client/partitions"
	clientVol            = "/client/vol"
	clientMetaPartition  = "/client/metaPartition"
	clientVolStat        = "/client/volStat"

	//raft node APIs
	addRaftNode    = "/raftNode/add"
	removeRaftNode = "/raftNode/remove"

	// Node APIs
	AddDataNode                    = "/dataNode/add"
	decommissionDataNode           = "/dataNode/decommission"
	decommissionDisk               = "/disk/decommission"
	getDataNode                    = "/dataNode/get"
	addMetaNode                    = "/metaNode/add"
	decommissionMetaNode           = "/metaNode/decommission"
	getMetaNode                    = "/metaNode/get"
	adminLoadMetaPartition         = "/metaPartition/load"
	adminDecommissionMetaPartition = "/metaPartition/decommission"

	// Operation response
	getMetaNodeTaskResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'

	getTopologyView = "/topo/get"
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
	http.HandleFunc(AdminGetIP, m.getIPAddr)
	http.HandleFunc(adminGetCluster, m.getCluster)
	http.Handle(AdminGetDataPartition, m.handlerWithInterceptor())
	http.Handle(adminCreateDataPartition, m.handlerWithInterceptor())
	http.Handle(adminLoadDataPartition, m.handlerWithInterceptor())
	http.Handle(adminDecommissionDataPartition, m.handlerWithInterceptor())
	http.Handle(adminCreateVol, m.handlerWithInterceptor())
	http.Handle(adminDeleteVol, m.handlerWithInterceptor())
	http.Handle(adminUpdateVol, m.handlerWithInterceptor())
	http.Handle(adminClusterFreeze, m.handlerWithInterceptor())
	http.Handle(AddDataNode, m.handlerWithInterceptor())
	http.Handle(addMetaNode, m.handlerWithInterceptor())
	http.Handle(decommissionDataNode, m.handlerWithInterceptor())
	http.Handle(decommissionDisk, m.handlerWithInterceptor())
	http.Handle(decommissionMetaNode, m.handlerWithInterceptor())
	http.Handle(getDataNode, m.handlerWithInterceptor())
	http.Handle(getMetaNode, m.handlerWithInterceptor())
	http.Handle(adminLoadMetaPartition, m.handlerWithInterceptor())
	http.Handle(adminDecommissionMetaPartition, m.handlerWithInterceptor())
	http.Handle(clientDataPartitions, m.handlerWithInterceptor())
	http.Handle(clientVol, m.handlerWithInterceptor())
	http.Handle(clientMetaPartition, m.handlerWithInterceptor())
	http.Handle(GetDataNodeTaskResponse, m.handlerWithInterceptor())
	http.Handle(getMetaNodeTaskResponse, m.handlerWithInterceptor())
	http.Handle(adminCreateMP, m.handlerWithInterceptor())
	http.Handle(clientVolStat, m.handlerWithInterceptor())
	http.Handle(addRaftNode, m.handlerWithInterceptor())
	http.Handle(removeRaftNode, m.handlerWithInterceptor())
	http.Handle(adminSetMetaNodeThreshold, m.handlerWithInterceptor())
	http.Handle(getTopologyView, m.handlerWithInterceptor())

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
			if m.partition.IsLeader() {
				m.ServeHTTP(w, r)
				return
			}
			if m.leaderInfo.addr == "" {
				log.LogErrorf("action[handlerWithInterceptor] no leader,request[%v]", r.URL)
				http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
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
	case adminGetCluster:
		m.getCluster(w, r)
	case adminCreateDataPartition:
		m.createDataPartition(w, r)
	case AdminGetDataPartition:
		m.getDataPartition(w, r)
	case adminLoadDataPartition:
		m.loadDataPartition(w, r)
	case adminDecommissionDataPartition:
		m.decommissionDataPartition(w, r)
	case adminCreateVol:
		m.createVol(w, r)
	case adminDeleteVol:
		m.markDeleteVol(w, r)
	case adminUpdateVol:
		m.updateVol(w, r)
	case adminClusterFreeze:
		m.setupAutoAllocation(w, r)
	case AddDataNode:
		m.addDataNode(w, r)
	case getDataNode:
		m.getDataNode(w, r)
	case decommissionDataNode:
		m.dataNodeOffline(w, r)
	case decommissionDisk:
		m.decommissionDisk(w, r)
	case GetDataNodeTaskResponse:
		m.handleDataNodeTaskResponse(w, r)
	case addMetaNode:
		m.addMetaNode(w, r)
	case getMetaNode:
		m.getMetaNode(w, r)
	case decommissionMetaNode:
		m.decommissionMetaNode(w, r)
	case getMetaNodeTaskResponse:
		m.handleMetaNodeTaskResponse(w, r)
	case clientDataPartitions:
		m.getDataPartitions(w, r)
	case clientVol:
		m.getVol(w, r)
	case clientMetaPartition:
		m.getMetaPartition(w, r)
	case clientVolStat:
		m.getVolStatInfo(w, r)
	case adminLoadMetaPartition:
		m.loadMetaPartition(w, r)
	case adminDecommissionMetaPartition:
		m.decommissionMetaPartition(w, r)
	case adminCreateMP:
		m.createMetaPartition(w, r)
	case addRaftNode:
		m.addRaftNode(w, r)
	case removeRaftNode:
		m.removeRaftNode(w, r)
	case adminSetMetaNodeThreshold:
		m.setMetaNodeThreshold(w, r)
	case getTopologyView:
		m.getTopology(w, r)
	default:

	}
}

func newLogMsg(requestType, remoteAddr, message string, code int) (logMsg string) {
	logMsg = fmt.Sprintf("type[%s] From [%s] httpCode[%d] Because [%s] ", requestType, remoteAddr, code, message)
	return
}

//HandleError send err to client
func HandleError(message string, err error, code int, w http.ResponseWriter) {
	log.LogErrorf("errMsg:%v errStack:%v", message, errors.ErrorStack(err))
	http.Error(w, message, code)
}
