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
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/log"
)

func (s *DataNode) Prepare(p *repl.Packet) (err error) {
	defer func() {
		p.SetPacketHasPrepare()
		if err != nil {
			p.PackErrorBody(repl.ActionPreparePkt, err.Error())
		} else {
			p.AfterPre = true
		}
	}()
	if p.IsMasterCommand() {
		return
	}
	atomic.AddUint64(&s.metricsCnt, 1)
	if !s.shallDegrade() {
		p.BeforeTp(s.clusterID)
		p.UnsetDegrade()
	} else {
		p.SetDegrade()
	}
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
	if err = s.checkPacketAndPrepare(p); err != nil {
		return
	}

	return
}

func (s *DataNode) checkStoreMode(p *repl.Packet) (err error) {
	if proto.IsTinyExtentType(p.ExtentType) || proto.IsNormalExtentType(p.ExtentType) {
		return
	}
	log.LogErrorf("action[checkStoreMode] dp [%v] reqId [%v] extent type %v", p.PartitionID, p.ReqID, p.ExtentType)
	return ErrIncorrectStoreType
}

func (s *DataNode) checkCrc(p *repl.Packet) (err error) {
	if !p.IsNormalWriteOperation() {
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
		// err = proto.ErrDataPartitionNotExists
		err = fmt.Errorf("data partition not exists %v", p.PartitionID)
		return
	}
	p.Object = dp
	if p.IsNormalWriteOperation() || p.IsCreateExtentOperation() {
		if dp.Available() <= 0 {
			err = storage.NoSpaceError
			return
		}
	}
	if p.IsNormalWriteOperation() || p.IsRandomWrite() {
		dp.disk.allocCheckLimit(proto.FlowWriteType, uint32(p.Size))
		dp.disk.allocCheckLimit(proto.IopsWriteType, 1)
	}
	return
}

func (s *DataNode) checkPacketAndPrepare(p *repl.Packet) error {
	partition := p.Object.(*DataPartition)
	store := p.Object.(*DataPartition).ExtentStore()
	var (
		extentID uint64
		err      error
	)

	log.LogDebugf("action[prepare.checkPacketAndPrepare] pack opcode (%v) p.IsLeaderPacket(%v) p (%v)", p.Opcode, p.IsLeaderPacket(), p)
	if p.IsRandomWrite() || p.IsSnapshotModWriteAppendOperation() || p.IsNormalWriteOperation() {
		if err = partition.CheckWriteVer(p); err != nil {
			return err
		}
	}
	if p.IsLeaderPacket() && proto.IsTinyExtentType(p.ExtentType) && p.IsNormalWriteOperation() {
		extentID, err = store.GetAvailableTinyExtent()
		if err != nil {
			return fmt.Errorf("checkPacketAndPrepare partition %v GetAvailableTinyExtent error %v", p.PartitionID, err.Error())
		}
		p.ExtentID = extentID
		p.ExtentOffset, err = store.GetTinyExtentOffset(extentID)
		if err != nil {
			return fmt.Errorf("checkPacketAndPrepare partition %v  %v GetTinyExtentOffset error %v", p.PartitionID, extentID, err.Error())
		}
	} else if p.IsLeaderPacket() && p.IsSnapshotModWriteAppendOperation() {
		if proto.IsTinyExtentType(p.ExtentType) {
			extentID, err = store.GetAvailableTinyExtent()
			if err != nil {
				log.LogErrorf("err %v", err)
				return fmt.Errorf("checkPacketAndPrepare partition %v GetAvailableTinyExtent error %v", p.PartitionID, err.Error())
			}
			p.ExtentID = extentID
			p.ExtentOffset, err = store.GetTinyExtentOffset(p.ExtentID)
			if err != nil {
				err = fmt.Errorf("checkPacketAndPrepare partition %v  %v GetTinyExtentOffset error %v", p.PartitionID, extentID, err.Error())
				log.LogErrorf("err %v", err)
			}
			log.LogDebugf("action[prepare.checkPacketAndPrepare] dp %v append randomWrite p.ExtentOffset %v Kernel(file)Offset %v",
				p.PartitionID, p.ExtentOffset, p.KernelOffset)
			return err
		}

		p.ExtentOffset, err = store.GetExtentSnapshotModOffset(p.ExtentID, p.Size)
		log.LogDebugf("action[prepare.checkPacketAndPrepare] pack (%v) partition %v %v", p, p.PartitionID, extentID)
		if err != nil {
			return fmt.Errorf("checkPacketAndPrepare partition %v  %v GetSnapshotModExtentOffset error %v", p.PartitionID, extentID, err.Error())
		}
	} else if p.IsLeaderPacket() && p.IsCreateExtentOperation() {
		if partition.isNormalType() && partition.GetExtentCount() >= storage.MaxExtentCount*3 {
			return fmt.Errorf("checkPacketAndPrepare partition %v has reached maxExtentId", p.PartitionID)
		}
		p.ExtentID, err = store.NextExtentID()
		if err != nil {
			return fmt.Errorf("checkPacketAndPrepare partition %v allocCheckLimit NextExtentId error %v", p.PartitionID, err)
		}
	} else if p.IsLeaderPacket() &&
		((p.IsMarkDeleteExtentOperation() && proto.IsTinyExtentType(p.ExtentType)) ||
			(p.IsMarkSplitExtentOperation() && !proto.IsTinyExtentType(p.ExtentType))) {

		log.LogDebugf("checkPacketAndPrepare. packet opCode %v p.ExtentType %v", p.Opcode, p.ExtentType)

		record := new(proto.TinyExtentDeleteRecord)
		if err := json.Unmarshal(p.Data[:p.Size], record); err != nil {
			return fmt.Errorf("checkPacketAndPrepare failed %v", err.Error())
		}
		p.Data, _ = json.Marshal(record)
		p.Size = uint32(len(p.Data))
	}

	if (p.IsCreateExtentOperation() || p.IsNormalWriteOperation()) && p.ExtentID == 0 {
		return fmt.Errorf("checkPacketAndPrepare partition %v invalid extent id. ", p.PartitionID)
	}

	p.OrgBuffer = p.Data

	return nil
}
