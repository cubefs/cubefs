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

func (mp *metaPartition) fsmTxRollback(handle interface{}, txID string) (status uint8) {
	status = mp.txProcessor.txManager.rollbackTxInfo(handle, txID)
	return
}

func (mp *metaPartition) fsmTxDelete(handle interface{}, txID string) (status uint8) {
	status = mp.txProcessor.txManager.deleteTxInfo(handle, txID)
	return
}

func (mp *metaPartition) fsmTxInodeRollback(handle interface{}, req *proto.TxInodeApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackInode(handle, req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(handle interface{}, req *proto.TxDentryApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackDentry(handle, req)
	return
}

func (mp *metaPartition) fsmTxSetState(handle interface{}, req *proto.TxSetStateRequest) (status uint8) {
	status, _ = mp.txProcessor.txManager.txSetState(handle, req)
	return
}

func (mp *metaPartition) fsmTxInit(handle interface{}, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	err := mp.txProcessor.txManager.registerTransaction(handle, txInfo)
	if err != nil {
		log.LogErrorf("fsmTxInit: register transaction failed, txInfo %s, err %s", txInfo.String(), err.Error())
		return proto.OpTxInternalErr
	}
	return
}

func (mp *metaPartition) fsmTxCommit(handle interface{}, txID string) (status uint8) {
	status, _ = mp.txProcessor.txManager.commitTxInfo(handle, txID)
	return
}

func (mp *metaPartition) fsmTxInodeCommit(handle interface{}, txID string, inode uint64) (status uint8) {
	// var err error
	status, _ = mp.txProcessor.txResource.commitInode(handle, txID, inode)
	return
}

func (mp *metaPartition) fsmTxDentryCommit(handle interface{}, txID string, pId uint64, name string) (status uint8) {
	// var err error
	status, _ = mp.txProcessor.txResource.commitDentry(handle, txID, pId, name)
	return
}

func (mp *metaPartition) fsmTxCommitRM(handle interface{}, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo, err := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if err != nil {
		status = proto.OpErr
		log.LogErrorf("fsmTxCommitRM copyGetTx(%s) err: %s", txInfo.TxID, err.Error())
		return
	}
	if ifo == nil || ifo.Finish() {
		log.LogWarnf("fsmTxCommitRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
		return
	}

	mpId := mp.config.PartitionId
	for _, ifo := range txInfo.TxInodeInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxInodeCommit(handle, ifo.TxID, ifo.Ino)
	}

	for _, ifo := range txInfo.TxDentryInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxDentryCommit(handle, ifo.TxID, ifo.ParentId, ifo.Name)
	}

	ifo.SetFinish()
	err = mp.txProcessor.txManager.txTree.Update(handle, ifo)
	if err != nil {
		return proto.OpErr
	}
	return proto.OpOk
}

func (mp *metaPartition) fsmTxRollbackRM(handle interface{}, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo, err := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if err != nil {
		status = proto.OpErr
		log.LogErrorf("fsmTxRollbackRM: tx %v not found, err %v", txInfo, err)
		return
	}

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
		mp.fsmTxInodeRollback(handle, req)
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
		mp.fsmTxDentryRollback(handle, req)
	}

	ifo.SetFinish()
	err = mp.txProcessor.txManager.txTree.Update(handle, ifo)
	if err != nil {
		return proto.OpErr
	}
	return proto.OpOk
}

func (mp *metaPartition) inodeInTx(inode uint64) uint8 {
	inTx, txId, err := mp.txProcessor.txResource.isInodeInTransction(NewInode(inode, 0))
	if err != nil {
		return proto.OpErr
	}
	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, inode %d, txId %s", inode, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}

func (mp *metaPartition) dentryInTx(parIno uint64, name string) uint8 {
	inTx, txId, err := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})
	if err != nil {
		return proto.OpErr
	}

	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, parent inode %d, name %s, txId %s", parIno, name, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}

func (mp *metaPartition) txInodeInRb(inode uint64, newTxId string) (rbInode *TxRollbackInode) {
	rbIno, err := mp.txProcessor.txResource.getTxRbInode(inode)
	if err != nil {
		return nil
	}
	if rbIno != nil && rbIno.txInodeInfo.TxID == newTxId {
		return rbIno
	}

	return nil
}

func (mp *metaPartition) txDentryInRb(parIno uint64, name, newTxId string) bool {
	inTx, txId, err := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})
	if err != nil {
		return false
	}
	return inTx && txId == newTxId
}
