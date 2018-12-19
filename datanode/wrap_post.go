package datanode

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"sync/atomic"
	"time"
)

func (s *DataNode) Post(pkg *repl.Packet) error {
	if pkg.IsMasterCommand() {
		pkg.NeedReply = false
	}
	if pkg.Opcode == proto.OpStreamRead {
		pkg.NeedReply = false
	}
	if pkg.Opcode == proto.OpCreateDataPartition {
		pkg.NeedReply = true
	}
	s.cleanupPkg(pkg)
	s.addMetrics(pkg)
	return nil
}

// The head node release tinyExtent to store
func (s *DataNode) cleanupPkg(pkg *repl.Packet) {
	if pkg.IsMasterCommand() {
		return
	}
	if !isLeaderPacket(pkg) {
		return
	}
	s.releaseExtent(pkg)
	if pkg.ExtentMode == proto.TinyExtentMode && isWriteOperation(pkg) {
		pkg.PutConnectsToPool()
	}
}

func (s *DataNode) releaseExtent(pkg *repl.Packet) {
	if pkg == nil || !storage.IsTinyExtent(pkg.ExtentID) || pkg.ExtentID <= 0 || atomic.LoadInt32(&pkg.IsRelase) == HasReturnToStore {
		return
	}
	if pkg.ExtentMode != proto.TinyExtentMode || !isLeaderPacket(pkg) || !isWriteOperation(pkg) || !pkg.IsForwardPkg() {
		return
	}
	if pkg.Object == nil {
		return
	}
	partition := pkg.Object.(*DataPartition)
	store := partition.GetStore()
	if pkg.IsErrPacket() {
		store.PutTinyExtentToUnavaliCh(pkg.ExtentID)
	} else {
		store.PutTinyExtentToAvaliCh(pkg.ExtentID)
	}
	atomic.StoreInt32(&pkg.IsRelase, HasReturnToStore)
}

func (s *DataNode) addMetrics(reply *repl.Packet) {
	if reply.IsMasterCommand() {
		return
	}
	reply.AfterTp()
	latency := time.Since(reply.TpObject.StartTime)
	if reply.Object == nil {
		return
	}
	partition := reply.Object.(*DataPartition)
	if partition == nil {
		return
	}
	if isWriteOperation(reply) {
		partition.AddWriteMetrics(uint64(latency))
	} else if isReadExtentOperation(reply) {
		partition.AddReadMetrics(uint64(latency))
	}
}
