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
// permissions and limitations under the License.k

package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) fsmTxRollback(txID string) (status uint8) {
	status = mp.txProcessor.txManager.rollbackTxInfo(txID)
	return
}

func (mp *metaPartition) fsmTxDelete(txID string) (status uint8) {
	status = mp.txProcessor.txManager.deleteTxInfo(txID)
	return
}

func (mp *metaPartition) fsmTxInodeRollback(req *proto.TxInodeApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackInode(req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(req *proto.TxDentryApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackDentry(req)
	return
}

func (mp *metaPartition) fsmTxSetState(req *proto.TxSetStateRequest) (status uint8) {
	status, _ = mp.txProcessor.txManager.txSetState(req)
	return
}

func (mp *metaPartition) fsmTxInit(txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	err := mp.txProcessor.txManager.registerTransaction(txInfo)
	if err != nil {
		log.LogErrorf("fsmTxInit: register transaction failed, txInfo %s, err %s", txInfo.String(), err.Error())
		return proto.OpTxInternalErr
	}
	return
}

func (mp *metaPartition) fsmTxCommit(txID string) (status uint8) {
	status, _ = mp.txProcessor.txManager.commitTxInfo(txID)
	return
}

func (mp *metaPartition) fsmTxInodeCommit(txID string, inode uint64) (status uint8) {
	//var err error
	status, _ = mp.txProcessor.txResource.commitInode(txID, inode)
	return
}

func (mp *metaPartition) fsmTxDentryCommit(txID string, pId uint64, name string) (status uint8) {
	//var err error
	status, _ = mp.txProcessor.txResource.commitDentry(txID, pId, name)
	return
}

func (mp *metaPartition) fsmTxCommitRM(txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if ifo == nil || ifo.Finish() {
		log.LogWarnf("fsmTxCommitRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
		return
	}

	mpId := mp.config.PartitionId
	for _, ifo := range txInfo.TxInodeInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxInodeCommit(ifo.TxID, ifo.Ino)
	}

	for _, ifo := range txInfo.TxDentryInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxDentryCommit(ifo.TxID, ifo.ParentId, ifo.Name)
	}

	ifo.SetFinish()
	return proto.OpOk
}

func (mp *metaPartition) fsmTxRollbackRM(txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if ifo == nil || ifo.Finish() {
		log.LogWarnf("fsmTxRollbackRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
		return
	}

	mpId := mp.config.PartitionId
	for _, ifo := range txInfo.TxInodeInfos {
		if ifo.MpID != mpId {
			continue
		}

		req := &proto.TxInodeApplyRequest{
			TxID:  ifo.TxID,
			Inode: ifo.Ino,
		}
		mp.fsmTxInodeRollback(req)
	}

	// delete from rb tree
	for _, ifo := range txInfo.TxDentryInfos {
		if ifo.MpID != mpId {
			continue
		}

		req := &proto.TxDentryApplyRequest{
			TxID: ifo.TxID,
			Pid:  ifo.ParentId,
			Name: ifo.Name,
		}
		mp.fsmTxDentryRollback(req)
	}

	ifo.SetFinish()
	return proto.OpOk
}

func (mp *metaPartition) inodeInTx(inode uint64) uint8 {
	inTx, txId := mp.txProcessor.txResource.isInodeInTransction(NewInode(inode, 0))
	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, inode %d, txId %s", inode, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}

func (mp *metaPartition) dentryInTx(parIno uint64, name string) uint8 {
	inTx, txId := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})

	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, parent inode %d, name %s, txId %s", parIno, name, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}
