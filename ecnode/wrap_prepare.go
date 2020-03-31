// Copyright 2020 The Chubao Authors.
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
	"fmt"
	"hash/crc32"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
)

func (e *EcNode) Prepare(p *repl.Packet) (err error) {
	defer func() {
		p.SetPacketHasPrepare()
		if err != nil {
			p.PackErrorBody(repl.ActionPreparePkt, err.Error())
		}
	}()

	if p.IsMasterCommand() {
		return
	}
	p.BeforeTp(e.clusterID)
	err = e.checkCrc(p)
	if err != nil {
		return
	}
	err = e.checkPartition(p)
	if err != nil {
		return
	}
	err = e.addExtentInfo(p)
	if err != nil {
		return
	}

	return
}

func (e *EcNode) checkCrc(p *repl.Packet) (err error) {
	if !p.IsWriteOperation() {
		return
	}
	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc != p.CRC {
		return storage.CrcMismatchError
	}

	return
}

func (e *EcNode) checkPartition(p *repl.Packet) (err error) {
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.Object = ep
	if p.IsWriteOperation() || p.IsCreateExtentOperation() {
		if ep.Available() <= 0 {
			err = storage.NoSpaceError
			return
		}
	}
	return
}

func (e *EcNode) addExtentInfo(p *repl.Packet) error {
	partition := p.Object.(*EcPartition)
	store := p.Object.(*EcPartition).ExtentStore()
	var (
		extentID uint64
		err      error
	)
	if p.IsLeaderPacket() && p.IsWriteOperation() {
		p.ExtentID = extentID
	} else if p.IsLeaderPacket() && p.IsCreateExtentOperation() {
		if partition.GetExtentCount() >= storage.MaxExtentCount*3 {
			return fmt.Errorf("addExtentInfo partition %v has reached maxExtentId", p.PartitionID)
		}
		p.ExtentID, err = store.NextExtentID()
		if err != nil {
			return fmt.Errorf("addExtentInfo partition %v alloc NextExtentId error %v", p.PartitionID, err)
		}
	}
	p.OrgBuffer = p.Data

	return nil
}
