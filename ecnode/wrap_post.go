// Copyright 2020 The CubeFS Authors.
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

package ecnode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
)

func (e *EcNode) Post(p *repl.Packet) error {
	if p.IsMasterCommand() {
		p.NeedReply = true
	}
	if p.IsReadOperation() {
		p.NeedReply = false
	}

	if p.Opcode == proto.OpNotExistErr {
		p.NeedReply = true
	}
	e.cleanupPkt(p)
	e.addMetrics(p)
	return nil
}

func (e *EcNode) cleanupPkt(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	if !p.IsLeaderPacket() {
		return
	}
}

func (e *EcNode) addMetrics(p *repl.Packet) {
	if p.IsMasterCommand() {
		return
	}
	p.AfterTp()
	if p.Object == nil {
		return
	}
	partition := p.Object.(*EcPartition)
	if partition == nil {
		return
	}
}
