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

package raftstore

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	"strings"
	"sync"
)

// Error definitions.
var (
	ErrNoSuchNode        = errors.New("no such node")
	ErrIllegalAddress    = errors.New("illegal address")
	ErrUnknownSocketType = errors.New("unknown socket type")
)

// This private struct defined necessary properties for node address info storage.
type nodeAddress struct {
	Heartbeat string
	Replicate string
}

// NodeManager defined necessary methods for node address management.
type NodeManager interface {
	// AddNode used to add node address information.
	AddNode(nodeID uint64, addr string)

	// AddNode adds node address with specified port.
	AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int)

	// DeleteNode used to delete node address information
	// of specified node ID from NodeManager if possible.
	DeleteNode(nodeID uint64)
}

// NodeResolver defined necessary methods for both node address resolving and management.
// It extends from SocketResolver and NodeManager.
type NodeResolver interface {
	raft.SocketResolver
	NodeManager
}

// This is the default parallel-safe implementation of NodeResolver interface.
type nodeResolver struct {
	nodeMap sync.Map
}

// NodeAddress resolve NodeID to net.Addr addresses.
// This method is necessary for SocketResolver interface implementation.
func (r *nodeResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	val, ok := r.nodeMap.Load(nodeID)
	if !ok {
		err = ErrNoSuchNode
		return
	}
	address, ok := val.(*nodeAddress)
	if !ok {
		err = ErrIllegalAddress
		return
	}
	switch stype {
	case raft.HeartBeat:
		addr = address.Heartbeat
	case raft.Replicate:
		addr = address.Replicate
	default:
		err = ErrUnknownSocketType
	}
	return
}

// AddNode adds node address information.
func (r *nodeResolver) AddNode(nodeID uint64, addr string) {
	r.AddNodeWithPort(nodeID, addr, 0, 0)
}

func (r *nodeResolver) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeatPort
	}
	if replicate == 0 {
		replicate = DefaultReplicatePort
	}
	if len(strings.TrimSpace(addr)) != 0 {
		r.nodeMap.Store(nodeID, &nodeAddress{
			Heartbeat: fmt.Sprintf("%s:%d", addr, heartbeat),
			Replicate: fmt.Sprintf("%s:%d", addr, replicate),
		})
	}
}

// DeleteNode deletes node address information of specified node ID from NodeManager if possible.
func (r *nodeResolver) DeleteNode(nodeID uint64) {
	r.nodeMap.Delete(nodeID)
}

// NewNodeResolver returns a new NodeResolver instance for node address management and resolving.
func NewNodeResolver() NodeResolver {
	return &nodeResolver{}
}

// AddNode add node address into specified NodeManger if possible.
func AddNode(manager NodeManager, nodeID uint64, addr string, heartbeat int, replicate int) {
	if manager != nil {
		log.LogInfof("add node %d %s\n", nodeID, addr)
		manager.AddNode(nodeID, addr)
	}
}

// DeleteNode delete node address data from specified NodeManager if possible.
func DeleteNode(manager NodeManager, nodeID uint64) {
	if manager != nil {
		manager.DeleteNode(nodeID)
	}
}
