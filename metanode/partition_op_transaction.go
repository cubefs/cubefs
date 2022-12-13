package metanode

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) TxCommit(req *proto.TxApplyRequest, p *Packet) error {
	//val, err := json.Marshal(req)
	//if err != nil {
	//	return
	//}
	//status, err := mp.submit(opFSMTxCommit, val)
	//if err != nil {
	//	p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
	//	return
	//}
	//p.ResultCode = status.(uint8)

	//don't submit
	status, err := mp.txProcessor.txManager.commitTransaction(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status

	return err
}

func (mp *metaPartition) TxRollback(req *proto.TxApplyRequest, p *Packet) error {

	status, err := mp.txProcessor.txManager.rollbackTransaction(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status

	return err
}

func (mp *metaPartition) TxInodeCommit(req *proto.TxInodeApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxInodeCommit, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxInodeRollback(req *proto.TxInodeApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxInodeRollback, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxDentryCommit(req *proto.TxDentryApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxDentryCommit, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}

func (mp *metaPartition) TxDentryRollback(req *proto.TxDentryApplyRequest, p *Packet) error {
	val, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, err := mp.submit(opFSMTxDentryRollback, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}
	p.ResultCode = status.(uint8)

	return err
}
