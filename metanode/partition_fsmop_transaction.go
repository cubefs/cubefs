package metanode

import "github.com/cubefs/cubefs/proto"

func (mp *metaPartition) fsmTxRollback(txID string) (status uint8) {
	//status = proto.OpOk
	var err error
	status, err = mp.txProcessor.txManager.rollbackTxInfo(txID)
	if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxInfoNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}
}

func (mp *metaPartition) fsmTxInodeRollback(req *proto.TxInodeApplyRequest) (status uint8) {
	//status = proto.OpOk
	//var err error
	status, _ = mp.txProcessor.txResource.rollbackInode(req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(req *proto.TxDentryApplyRequest) (status uint8) {
	//status = proto.OpOk
	//var err error
	status, _ = mp.txProcessor.txResource.rollbackDentry(req)
	return
}

func (mp *metaPartition) fsmTxCommit(txID string) (status uint8) {
	//status = proto.OpOk
	var err error
	status, err = mp.txProcessor.txManager.commitTxInfo(txID)
	if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxInfoNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}
}

func (mp *metaPartition) fsmTxInodeCommit(txID string, inode uint64) (status uint8) {
	//status = proto.OpOk
	var err error
	status, err = mp.txProcessor.txResource.commitInode(txID, inode)
	if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxRbInodeNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}
}

func (mp *metaPartition) fsmTxDentryCommit(txID string, pId uint64, name string) (status uint8) {
	//status = proto.OpOk
	var err error
	status, err = mp.txProcessor.txResource.commitDentry(txID, pId, name)
	if err == nil && status == proto.OpOk {
		return
	} else {
		if err != nil && status == proto.OpTxRbDentryNotExistErr {
			status = proto.OpOk
			return
		}
		return status
	}
}
