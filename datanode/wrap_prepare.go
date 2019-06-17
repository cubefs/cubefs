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

package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
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
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.Object = dp
	if isWriteOperation(p) || isCreateExtentOperation(p) {
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
	var (
		extentID uint64
		err      error
	)
	if isLeaderPacket(p) && p.ExtentType == proto.TinyExtentType && isWriteOperation(p) {
		extentID, err = store.GetAvailableTinyExtent()
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v GetAvailableTinyExtent error %v", p.PartitionID, err.Error())
		}
		p.ExtentID = extentID
		p.ExtentOffset, err = store.GetTinyExtentOffset(extentID)
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v  %v GetTinyExtentOffset error %v", p.PartitionID, extentID, err.Error())
		}
	} else if isLeaderPacket(p) && isCreateExtentOperation(p) {
		if partition.GetExtentCount() >= storage.MaxExtentCount*3 {
			return fmt.Errorf("addExtentInfo partition %v has reached maxExtentId", p.PartitionID)
		}
		p.ExtentID, err = store.NextExtentID()
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v alloc NextExtentId error %v", p.PartitionID, err)
		}
	} else if isLeaderPacket(p) && isMarkDeleteExtentOperation(p) && isTinyExtentType(p) {
		record := new(proto.TinyExtentDeleteRecord)
		if err := json.Unmarshal(p.Data[:p.Size], record); err != nil {
			return fmt.Errorf("addExtentInfo failed %v", err.Error())
		}
		record.TinyDeleteFileOffset = store.NextTinyDeleteFileOffset()
		p.Data, _ = json.Marshal(record)
		p.Size = uint32(len(p.Data))
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
	return p.Opcode == proto.OpWrite || p.Opcode == proto.OpSyncWrite
}

func isCreateExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpCreateExtent
}

func isMarkDeleteExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpMarkDelete
}

func isTinyExtentType(p *repl.Packet) bool {
	return p.ExtentType == proto.TinyExtentType
}

func isReadExtentOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpExtentRepairRead || p.Opcode == proto.OpRead || p.Opcode == proto.OpReadTinyDelete
}
