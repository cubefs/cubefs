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

package metanode

import (
	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
)

type Packet struct {
	proto.Packet
}

// For send delete request to dataNode
func NewExtentDeletePacket(dp *DataPartition, ext *proto.ExtentKey) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMarkDelete
	p.StoreMode = proto.NormalExtentMode
	p.PartitionID = dp.PartitionID
	if storage.IsTinyExtent(ext.ExtentId) {
		p.StoreMode = proto.TinyExtentMode
		p.Data, _ = json.Marshal(ext)
		p.Size = uint32(len(p.Data))
	}
	p.FileID = ext.ExtentId
	p.ReqID = proto.GetReqID()
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))

	return p
}
