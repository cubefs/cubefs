// Copyright 2018 The CFS Authors.
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

package datanode

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"sync/atomic"
)

// TODO what does the Post function do?
func (s *DataNode) Post(pkg *repl.Packet) error {
	if pkg.IsMasterCommand() {
		pkg.NeedReply = false
	}
	if pkg.Opcode == proto.OpStreamRead {
		pkg.NeedReply = false
	}
	if pkg.Opcode == proto.OpCreateDataPartition {
		pkg.NeedReply = true
	}
	s.cleanupPkg(pkg)
	s.addMetrics(pkg)
	return nil
}

func (s *DataNode) cleanupPkg(pkg *repl.Packet) {
	if pkg.IsMasterCommand() {
		return
	}
	if !isLeaderPacket(pkg) {
		return
	}
	s.releaseExtent(pkg)
	if pkg.ExtentType == proto.TinyExtentType && isWriteOperation(pkg) {
		pkg.PutConnectsToPool()
	}
}

func (s *DataNode) releaseExtent(pkg *repl.Packet) {
	if pkg == nil || !storage.IsTinyExtent(pkg.ExtentID) || pkg.ExtentID <= 0 || atomic.LoadInt32(&pkg.IsRelase) == HasReturnToStore {
		return
	}
	if pkg.ExtentType != proto.TinyExtentType || !isLeaderPacket(pkg) || !isWriteOperation(pkg) || !pkg.IsForwardPkg() {
		return
	}
	if pkg.Object == nil {
		return
	}
	partition := pkg.Object.(*DataPartition)
	store := partition.ExtentStore()
	if pkg.IsErrPacket() {
		store.SendToBadTinyExtentC(pkg.ExtentID)
	} else {
		store.SendToGoodTinyExtentC(pkg.ExtentID)
	}
	atomic.StoreInt32(&pkg.IsRelase, HasReturnToStore)
}

func (s *DataNode) addMetrics(reply *repl.Packet) {
	if reply.IsMasterCommand() {
		return
	}
	reply.AfterTp()
	if reply.Object == nil {
		return
	}
	partition := reply.Object.(*DataPartition)
	if partition == nil {
		return
	}
}
