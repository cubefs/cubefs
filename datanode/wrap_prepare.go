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
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"hash/crc32"
)

func (s *DataNode) Prepare(p *repl.Packet) (err error) {
	defer func() {
		if err != nil {
			p.PackErrorBody(repl.ActionPreparePkt, err.Error())
		}
	}()
	if p.IsMasterCommand() {
		return
	}
	p.BeforeTp(s.clusterID)
	err = s.checkStoreMode(p)
	if err != nil {
		return
	}
	if err = s.checkCrc(p); err != nil {
		return
	}
	if err = s.checkPartition(p); err != nil {
		return
	}

	// For certain packet, we meed to add some additional extent information.
	if err = s.addExtentInfo(p); err != nil {
		return
	}

	return
}

func (s *DataNode) checkStoreMode(p *repl.Packet) (err error) {
	if p.ExtentType == proto.TinyExtentType || p.ExtentType == proto.NormalExtentType {
		return nil
	}
	return ErrIncorrectStoreType
}

func (s *DataNode) checkCrc(p *repl.Packet) (err error) {
	if !isWriteOperation(p) {
		return
	}
	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc != p.CRC {
		return storage.CrcMismatchError
	}

	return
}

func (s *DataNode) checkPartition(p *repl.Packet) (err error) {
	dp := s.space.Partition(p.PartitionID)
	if dp == nil {
		err = errors.Errorf("partition %v is not exist", p.PartitionID)
		return
	}
	p.Object = dp
	if p.Opcode == proto.OpWrite || p.Opcode == proto.OpCreateExtent {
		if dp.Available() <= 0 {
			err = storage.NoSpaceError
			return
		}
	}
	return
}

func (s *DataNode) addExtentInfo(p *repl.Packet) error {
	partition := p.Object.(*DataPartition)
	store := p.Object.(*DataPartition).ExtentStore()
	if isLeaderPacket(p) && p.ExtentType == proto.TinyExtentType && isWriteOperation(p) {
		extentID, err := store.GetAvailableTinyExtent()
		if err != nil {
			return err
		}
		p.ExtentID = extentID
		p.ExtentOffset, err = store.GetTinyExtentOffset(extentID)
		if err != nil {
			return err
		}
	} else if isLeaderPacket(p) && p.Opcode == proto.OpCreateExtent {
		if partition.GetExtentCount() >= storage.MaxExtentId {
			return fmt.Errorf("partition %v has reached maxExtentId", p.PartitionID)
		}
		p.ExtentID = store.NextExtentID()
	}

	return nil
}

// A leader packet is the packet send to the leader and does not require packet forwarding.
func isLeaderPacket(p *repl.Packet) (ok bool) {
	if p.IsForwardPkt() && (isWriteOperation(p) || isCreateExtentOperation(p) || isMarkDeleteExtentOperation(p)) {
		ok = true
	}

	return
}

func isWriteOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpWrite
}

func isCreateExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpCreateExtent
}

func isMarkDeleteExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpMarkDelete
}

func isReadExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpRead
}
