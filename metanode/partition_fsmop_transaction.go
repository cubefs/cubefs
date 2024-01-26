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

func (mp *metaPartition) fsmTxRollback(dbHandle interface{}, txID string) (status uint8, err error) {
	status, err = mp.txProcessor.txManager.rollbackTxInfo(dbHandle, txID)
	return
}

func (mp *metaPartition) fsmTxDelete(dbHandle interface{}, txID string) (status uint8, err error) {
	status, err = mp.txProcessor.txManager.deleteTxInfo(dbHandle, txID)
	return
}

func (mp *metaPartition) fsmTxInodeRollback(dbHandle interface{}, req *proto.TxInodeApplyRequest) (status uint8, err error) {
	status, err = mp.txProcessor.txResource.rollbackInode(dbHandle, req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(dbHandle interface{}, req *proto.TxDentryApplyRequest) (status uint8, err error) {
	status, err = mp.txProcessor.txResource.rollbackDentry(dbHandle, req)
	return
}

func (mp *metaPartition) fsmTxSetState(dbHandle interface{}, req *proto.TxSetStateRequest) (status uint8, err error) {
	status, err = mp.txProcessor.txManager.txSetState(dbHandle, req)
	return
}

func (mp *metaPartition) fsmTxInit(dbHandle interface{}, txInfo *proto.TransactionInfo) (status uint8, err error) {
	status = proto.OpOk
	err = mp.txProcessor.txManager.registerTransaction(dbHandle, txInfo)
	if err != nil {
		log.LogErrorf("fsmTxInit: register transaction failed, txInfo %s, err %s", txInfo.String(), err.Error())
		status = proto.OpTxInternalErr
		return
	}
	return
}

func (mp *metaPartition) fsmTxCommit(dbHandle interface{}, txID string) (status uint8, err error) {
	status, err = mp.txProcessor.txManager.commitTxInfo(dbHandle, txID)
	return
}

func (mp *metaPartition) fsmTxInodeCommit(dbHandle interface{}, txID string, inode uint64) (status uint8, err error) {
	status, err = mp.txProcessor.txResource.commitInode(dbHandle, txID, inode)
	return
}

func (mp *metaPartition) fsmTxDentryCommit(dbHandle interface{}, txID string, pId uint64, name string) (status uint8, err error) {
	status, err = mp.txProcessor.txResource.commitDentry(dbHandle, txID, pId, name)
	return
}

func (mp *metaPartition) fsmTxCommitRM(dbHandle interface{}, txInfo *proto.TransactionInfo) (status uint8, err error) {
	status = proto.OpOk
	ifo, err := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)

	if ifo == nil || ifo.Finish() {
		log.LogWarnf("fsmTxCommitRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
		return
	}

	mpId := mp.config.PartitionId
	for _, ifo := range txInfo.TxInodeInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxInodeCommit(dbHandle, ifo.TxID, ifo.Ino)
	}

	for _, ifo := range txInfo.TxDentryInfos {
		if ifo.MpID != mpId {
			continue
		}

		mp.fsmTxDentryCommit(dbHandle, ifo.TxID, ifo.ParentId, ifo.Name)
	}

	ifo.SetFinish()
	err = mp.txProcessor.txManager.txTree.Update(dbHandle, ifo)
	if err != nil {
		return
	}
	status = proto.OpOk
	return
}

func (mp *metaPartition) fsmTxRollbackRM(dbHandle interface{}, txInfo *proto.TransactionInfo) (status uint8, err error) {
	status = proto.OpOk
	ifo, err := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if err != nil {
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
		mp.fsmTxInodeRollback(dbHandle, req)
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
		mp.fsmTxDentryRollback(dbHandle, req)
	}

	ifo.SetFinish()
	err = mp.txProcessor.txManager.txTree.Update(dbHandle, ifo)
	if err != nil {
		return
	}
	status = proto.OpOk
	return
}

func (mp *metaPartition) inodeInTx(inode uint64) (status uint8, err error) {
	inTx, txId, err := mp.txProcessor.txResource.isInodeInTransction(NewInode(inode, 0))
	if err != nil {
		return
	}
	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, inode %d, txId %s", inode, txId)
		status = proto.OpTxConflictErr
		return
	}
	status = proto.OpOk
	return
}

func (mp *metaPartition) dentryInTx(parIno uint64, name string) (status uint8, err error) {
	inTx, txId, err := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})
	if err != nil {
		return
	}
	if inTx {
		log.LogWarnf("inodeInTx: inode is in transaction, parent inode %d, name %s, txId %s", parIno, name, txId)
		status = proto.OpTxConflictErr
		return
	}
	status = proto.OpOk
	return
}

func (mp *metaPartition) txInodeInRb(inode uint64, newTxId string) (rbInode *TxRollbackInode, err error) {

	rbIno, err := mp.txProcessor.txResource.getTxRbInode(inode)
	if err != nil {
		return
	}
	if rbIno != nil && rbIno.txInodeInfo.TxID == newTxId {
		return
	}

	return
}

func (mp *metaPartition) txDentryInRb(parIno uint64, name, newTxId string) (inTx bool, err error) {
	inTx, txId, err := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})
	if err != nil {
		return
	}
	inTx = inTx && txId == newTxId
	return
}
