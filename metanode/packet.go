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

package metanode

import (
	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
)

type Packet struct {
	proto.Packet
}

// NewPacketToDeleteExtent returns a new packet to delete the extent.
func NewPacketToDeleteExtent(dp *DataPartition, ext *proto.ExtentKey) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMarkDelete
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = uint64(dp.PartitionID)
	if storage.IsTinyExtent(ext.ExtentId) {
		p.ExtentType = proto.TinyExtentType
		p.Data, _ = json.Marshal(ext)
		p.Size = uint32(len(p.Data))
	}
	p.ExtentID = ext.ExtentId
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.ArgLen = uint32(len(p.Arg))

	return p
}
