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

package raftstore

import (
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
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

// This private struct defines the necessary properties for node address info.
type nodeAddress struct {
	Heartbeat string
	Replicate string
}

// NodeManager defines the necessary methods for node address management.
type NodeManager interface {
	// add node address with specified port.
	AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int)

	// delete node address information
	DeleteNode(nodeID uint64)
}

// NodeResolver defines the methods for node address resolving and management.
// It is extended from SocketResolver and NodeManager.
type NodeResolver interface {
	raft.SocketResolver
	NodeManager
}

// Default thread-safe implementation of the NodeResolver interface.
type nodeResolver struct {
	nodeMap sync.Map
}

// NodeAddress resolves NodeID as net.Addr.
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

// AddNodeWithPort adds node address with specified port.
func (r *nodeResolver) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeatPort
	}
	if replicate == 0 {
		replicate = DefaultReplicaPort
	}
	if len(strings.TrimSpace(addr)) != 0 {
		r.nodeMap.Store(nodeID, &nodeAddress{
			Heartbeat: fmt.Sprintf("%s:%d", addr, heartbeat),
			Replicate: fmt.Sprintf("%s:%d", addr, replicate),
		})
	}
}

// DeleteNode deletes the node address information of the specified node ID from the NodeManager if possible.
func (r *nodeResolver) DeleteNode(nodeID uint64) {
	r.nodeMap.Delete(nodeID)
}

// NewNodeResolver returns a new NodeResolver instance for node address management and resolving.
func NewNodeResolver() NodeResolver {
	return &nodeResolver{}
}
