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

package metanode

import (
	"encoding/json"
	"hash/crc32"
	"net"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/iputil"
)

type Packet struct {
	proto.Packet
	remote string
}

func (p *Packet) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if err = p.Packet.ReadFromConn(c, timeoutSec); err != nil {
		return
	}
	if parts := strings.Split(c.RemoteAddr().String(), ":"); len(parts) > 1 {
		// Split ip address and port.
		p.remote = parts[0]
	}
	return
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
	if len(dp.Hosts) == 1 {
		p.RemainingFollowers = 127
	}

	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.ArgLen = uint32(len(p.Arg))

	return p
}

// NewPacketToBatchDeleteExtent returns a new packet to batch delete the extent.
func NewPacketToBatchDeleteExtent(dp *DataPartition, exts []*proto.ExtentKey) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchDeleteExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = uint64(dp.PartitionID)
	p.Data, _ = json.Marshal(exts)
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	if len(dp.Hosts) == 1 {
		p.RemainingFollowers = 127
	}
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.ArgLen = uint32(len(p.Arg))

	return p
}

// NewPacketToDeleteExtent returns a new packet to delete the extent.
func NewPacketToFreeInodeOnRaftFollower(partitionID uint64, freeInodes []byte) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMetaFreeInodesOnRaftFollower
	p.PartitionID = partitionID
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.Data = make([]byte, len(freeInodes))
	copy(p.Data, freeInodes)
	p.Size = uint32(len(p.Data))

	return p
}

func (p *Packet) ResetPackageData(req interface{}) {
	if p.IsNeedRemoveDupOperation() {
		_ = p.MarshalData(req)
	}
	return
}

func (p *Packet) IsNeedRemoveDupOperation() bool {
	if p.Opcode == proto.OpMetaExtentsAdd || p.Opcode == proto.OpMetaLinkInode ||
		p.Opcode == proto.OpMetaUnlinkInode || p.Opcode == proto.OpMetaBatchExtentsAdd || p.Opcode == proto.OpMetaSetattr ||
		p.Opcode == proto.OpMetaTruncate || p.Opcode == proto.OpMetaSetXAttr || p.Opcode == proto.OpMetaRemoveXAttr ||
		p.Opcode == proto.OpMetaCreateDentry || p.Opcode == proto.OpMetaDeleteDentry || p.Opcode == proto.OpMetaUpdateXAttr ||
		p.Opcode == proto.OpMetaExtentAddWithCheck || p.Opcode == proto.OpMetaTxCreateDentry || p.Opcode == proto.OpQuotaCreateDentry ||
		p.Opcode == proto.OpMetaTxDeleteDentry {
		return true
	}
	return false
}

func (p *Packet) FillClientRequestPacket(reqObj interface{}) (err error) {
	if p.CRC != 0 {
		//already fill
		return
	}

	var (
		clientIPUint32 uint32
		remoteIP       string
	)
	p.CRC = crc32.ChecksumIEEE(p.Data[:p.Size])
	if parts := strings.Split(p.remote, ":"); len(parts) >= 1 {
		remoteIP = parts[0]
	}
	if clientIPUint32, err = iputil.ConvertIPStrToUnit32(remoteIP); err != nil {
		return
	}
	switch p.Opcode {
	case proto.OpMetaLinkInode:
		req := reqObj.(*LinkInodeReq)
		req.ClientIP = clientIPUint32
	case proto.OpMetaUnlinkInode:
		req := reqObj.(*UnlinkInoReq)
		req.ClientIP = clientIPUint32
	case proto.OpMetaSetattr:
		req := reqObj.(*SetattrRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaTruncate:
		req := reqObj.(*ExtentsTruncateReq)
		req.ClientIP = clientIPUint32
	case proto.OpMetaSetXAttr:
		req := reqObj.(*proto.SetXAttrRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaUpdateXAttr:
		req := reqObj.(*proto.UpdateXAttrRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaRemoveXAttr:
		req := reqObj.(*proto.RemoveXAttrRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaExtentsAdd:
		req := reqObj.(*proto.AppendExtentKeyRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaExtentAddWithCheck:
		req := reqObj.(*proto.AppendExtentKeyWithCheckRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaBatchExtentsAdd:
		req := reqObj.(*proto.AppendExtentKeysRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaCreateDentry:
		req := reqObj.(*proto.CreateDentryRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaTxCreateDentry:
		req := reqObj.(*proto.TxCreateDentryRequest)
		req.ClientIP = clientIPUint32
	case proto.OpQuotaCreateDentry:
		req := reqObj.(*proto.QuotaCreateDentryRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaDeleteDentry:
		req := reqObj.(*proto.DeleteDentryRequest)
		req.ClientIP = clientIPUint32
	case proto.OpMetaTxDeleteDentry:
		req := reqObj.(*proto.TxDeleteDentryRequest)
		req.ClientIP = clientIPUint32
	default:
	}
	return
}
