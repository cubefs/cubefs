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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"hash/crc32"
)

// TODO add comments; what is this wrapper
func (s *DataNode) Prepare(pkg *repl.Packet) (err error) {
	defer func() {
		if err != nil {
			pkg.PackErrorBody(repl.ActionPreparePkg, err.Error())
		}
	}()
	if pkg.IsMasterCommand() {
		return
	}
	pkg.BeforeTp(s.clusterID)
	err = s.checkStoreMode(pkg)
	if err != nil {
		return
	}
	if err = s.checkCrc(pkg); err != nil {
		return
	}
	if err = s.checkPartition(pkg); err != nil {
		return
	}

	// TODO What does the addExtentInfo do here? 对于一些有特殊的包， 需要添加额外信息
	if err = s.addExtentInfo(pkg); err != nil {
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

func (s *DataNode) checkPartition(pkg *repl.Packet) (err error) {
	dp := s.space.Partition(pkg.PartitionID)
	if dp == nil {
		err = errors.Errorf("partition %v is not exist", pkg.PartitionID)
		return
	}
	pkg.Object = dp
	if pkg.Opcode == proto.OpWrite || pkg.Opcode == proto.OpCreateExtent {
		if dp.Available() <= 0 {
			err = storage.NoSpaceError
			return
		}
	}
	return
}


// TODO needs some explanation here
func (s *DataNode) addExtentInfo(pkg *repl.Packet) error {
	store := pkg.Object.(*DataPartition).ExtentStore()
	if isLeaderPacket(pkg) && pkg.ExtentType == proto.TinyExtentType && isWriteOperation(pkg) {
		extentID, err := store.GetGoodTinyExtent()
		if err != nil {
			return err
		}
		pkg.ExtentID = extentID
		pkg.ExtentOffset, err = store.GetTinyExtentoffset(extentID)
		if err != nil {
			return err
		}
	} else if isLeaderPacket(pkg) && pkg.Opcode == proto.OpCreateExtent {
		pkg.ExtentID = store.NextExtentID()
	}

	return nil
}

// TODO what is a leader packet?
// 这个包需不需要转发， 发给leader的包叫leaderpacket
func isLeaderPacket(p *repl.Packet) (ok bool) {
	if p.IsForwardPkg() && (isWriteOperation(p) || isCreateExtentOperation(p) || isMarkDeleteExtentOperation(p)) {
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
