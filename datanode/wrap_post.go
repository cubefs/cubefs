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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"sync/atomic"
)

func (s *DataNode) Post(p *repl.Packet) error {
	// 标记稍后是否由复制协议自动回写响应包
	// 除成功的读请求外，均需要复制协议回写响应。
	// 处理成功的读请求回写响应在Operate阶段由相关处理函数处理，不需要复制协议回写响应。
	p.NeedReply = !(p.IsReadOperation() && !p.IsErrPacket())
	s.cleanupPkt(p)
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
	if p == nil || !proto.IsTinyExtent(p.ExtentID) || p.ExtentID <= 0 || atomic.LoadInt32(&p.IsReleased) == IsReleased {
		return
	}
	if !p.IsTinyExtentType() || !p.IsLeaderPacket() || !p.IsWriteOperation() || !p.IsForwardPkt() {
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
