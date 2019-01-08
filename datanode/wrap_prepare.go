package datanode

import (
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"hash/crc32"
	"fmt"
)

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
	if err = s.addExtentInfo(pkg); err != nil {
		return
	}

	return
}

func (s *DataNode) checkStoreMode(p *repl.Packet) (err error) {
	if p.ExtentMode == proto.TinyExtentMode || p.ExtentMode == proto.NormalExtentMode {
		return nil
	}
	return ErrStoreTypeMismatch
}

func (s *DataNode) checkCrc(p *repl.Packet) (err error) {
	if !isWriteOperation(p) {
		return
	}
	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc != p.CRC {
		return storage.ErrPkgCrcMismatch
	}

	return
}

func (s *DataNode) checkPartition(pkg *repl.Packet) (err error) {
	dp := s.space.GetPartition(pkg.PartitionID)
	if dp == nil {
		err = errors.Errorf("partition %v is not exist", pkg.PartitionID)
		return
	}
	pkg.Object = dp
	if pkg.Opcode == proto.OpWrite || pkg.Opcode == proto.OpCreateExtent {
		if dp.Available() <= 0 {
			err = storage.ErrSyscallNoSpace
			return
		}
	}
	return
}

// If tinyExtent Write get the extentID and extentOffset
// If OpCreateExtent get new extentID
func (s *DataNode) addExtentInfo(pkg *repl.Packet) error {
	store := pkg.Object.(*DataPartition).GetStore()
	if isLeaderPacket(pkg) && pkg.ExtentMode == proto.TinyExtentMode && isWriteOperation(pkg) {
		extentID, err := store.GetAvaliTinyExtent() // GetConnect a valid tinyExtentId
		if err != nil {
			return err
		}
		pkg.ExtentID = extentID
		pkg.ExtentOffset, err = store.TinyExtentWritePrepare(extentID) // GetConnect offset of this extent file
		if err != nil {
			return err
		}
	} else if isLeaderPacket(pkg) && pkg.Opcode == proto.OpCreateExtent {
		extentId := store.NextExtentID()
		if extentId >= storage.MaxExtentId {
			return fmt.Errorf("partition %v has reached maxExtentId", pkg.PartitionID)
		}
		pkg.ExtentID = store.NextExtentID()
	}

	return nil
}

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
