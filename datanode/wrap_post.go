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

package datanode

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"sync/atomic"
)

func (s *DataNode) Post(p *repl.Packet) error {
	if p.IsMasterCommand() {
		p.NeedReply = false
	}
	if p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpExtentRepairRead {
		p.NeedReply = false
	}
	if p.Opcode == proto.OpCreateDataPartition {
		p.NeedReply = true
	}
	s.cleanupPkt(p)
	s.addMetrics(p)
	return nil
}

func (s *DataNode) cleanupPkt(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	if !isLeaderPacket(p) {
		return
	}
	s.releaseExtent(p)
	if p.ExtentType == proto.TinyExtentType && isWriteOperation(p) {
		p.PutConnsToPool()
	}
}

func (s *DataNode) releaseExtent(p *repl.Packet) {
	if p == nil || !storage.IsTinyExtent(p.ExtentID) || p.ExtentID <= 0 || atomic.LoadInt32(&p.IsReleased) == IsReleased {
		return
	}
	if p.ExtentType != proto.TinyExtentType || !isLeaderPacket(p) || !isWriteOperation(p) || !p.IsForwardPkt() {
		return
	}
	if p.Object == nil {
		return
	}
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.IsErrPacket() {
		store.SendToBrokenTinyExtentC(p.ExtentID)
	} else {
		store.SendToAvailableTinyExtentC(p.ExtentID)
	}
	atomic.StoreInt32(&p.IsReleased, IsReleased)
}

func (s *DataNode) addMetrics(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	p.AfterTp()
	if p.Object == nil {
		return
	}
	partition := p.Object.(*DataPartition)
	if partition == nil {
		return
	}
}
