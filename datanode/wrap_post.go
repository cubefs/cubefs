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

package datanode

import (
	"bytes"
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync/atomic"

	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
)

func (s *DataNode) Post(p *repl.Packet) error {
	if p.IsMasterCommand() {
		p.NeedReply = true
	}
	if p.IsReadOperation() && p.AfterPre {
		p.NeedReply = false
	}
	s.cleanupPkt(p)
	s.tryReleaseExtentForLocalTransition(p)
	s.addMetrics(p)
	return nil
}

func (s *DataNode) cleanupPkt(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	if !p.IsLeaderPacket() {
		return
	}
	s.releaseExtent(p)
}

func (s *DataNode) releaseExtent(p *repl.Packet) {
	if p == nil || !storage.IsTinyExtent(p.ExtentID) || p.ExtentID <= 0 || atomic.LoadInt32(&p.IsReleased) == IsReleased {
		return
	}
	if !proto.IsTinyExtentType(p.ExtentType) || !p.IsLeaderPacket() || !p.IsWriteOperation() || !p.IsForwardPkt() {
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

func (s *DataNode) tryReleaseExtentForLocalTransition(p *repl.Packet) {
	if p.Opcode != proto.OpExtentsLocalTransition || !p.IsForwardPacket() {
		return
	}

	var (
		err  error
		info = &proto.ExtentsLocalTransitionRequest{}
	)

	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(info); err != nil {
		log.LogErrorf("error when releaseExtentForLocalTransition %v", err)
		return
	}
	dstDp := s.space.Partition(info.DstDp)
	dstStore := dstDp.ExtentStore()
	for _, extent := range info.DstExtents {
		if storage.IsTinyExtent(extent.ExtentId) {
			if p.IsErrPacket() {
				dstStore.SendToBrokenTinyExtentC(extent.ExtentId)
			} else {
				dstStore.SendToAvailableTinyExtentC(extent.ExtentId)
			}
			atomic.StoreInt32(&p.IsReleased, IsReleased)
			break
		}
	}
}

func (s *DataNode) addMetrics(p *repl.Packet) {
	if p.IsMasterCommand() || p.ShallDegrade() {
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
