package datanode

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
)

func (s *DataNode)prepare(pkg *repl.Packet)(err error){
	err=s.checkStoreMode(pkg)
	if err!=nil {
		return
	}

	return
}


func (s *DataNode) checkStoreMode(p *repl.Packet) (err error) {
	if p.StoreMode == proto.TinyExtentMode || p.StoreMode == proto.NormalExtentMode {
		return nil
	}
	return ErrStoreTypeMismatch
}

func isWriteOperation(p *repl.Packet) bool {
	return p.Opcode == proto.OpWrite
}


func (s *DataNode)checkCrc(p *repl.Packet)(err error){
	if !isWriteOperation(p){
		return
	}


	return
}