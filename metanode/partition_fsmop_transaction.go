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
	"context"

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) fsmTxRollback(ctx context.Context, txID string) (status uint8) {
	status = mp.txProcessor.txManager.rollbackTxInfo(ctx, txID)
	return
}

func (mp *metaPartition) fsmTxDelete(ctx context.Context, txID string) (status uint8) {
	status = mp.txProcessor.txManager.deleteTxInfo(ctx, txID)
	return
}

func (mp *metaPartition) fsmTxInodeRollback(ctx context.Context, req *proto.TxInodeApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackInode(ctx, req)
	return
}

func (mp *metaPartition) fsmTxDentryRollback(ctx context.Context, req *proto.TxDentryApplyRequest) (status uint8) {
	status, _ = mp.txProcessor.txResource.rollbackDentry(ctx, req)
	return
}

func (mp *metaPartition) fsmTxSetState(ctx context.Context, req *proto.TxSetStateRequest) (status uint8) {
	status, _ = mp.txProcessor.txManager.txSetState(ctx, req)
	return
}

func (mp *metaPartition) fsmTxInit(ctx context.Context, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	err := mp.txProcessor.txManager.registerTransaction(ctx, txInfo)
	if err != nil {
		getSpan(ctx).Errorf("fsmTxInit: register transaction failed, txInfo %s, err %s", txInfo.String(), err.Error())
		return proto.OpTxInternalErr
	}
	return
}

func (mp *metaPartition) fsmTxCommit(ctx context.Context, txID string) (status uint8) {
	status, _ = mp.txProcessor.txManager.commitTxInfo(ctx, txID)
	return
}

func (mp *metaPartition) fsmTxInodeCommit(ctx context.Context, txID string, inode uint64) (status uint8) {
	// var err error
	status, _ = mp.txProcessor.txResource.commitInode(ctx, txID, inode)
	return
}

func (mp *metaPartition) fsmTxDentryCommit(ctx context.Context, txID string, pId uint64, name string) (status uint8) {
	// var err error
	status, _ = mp.txProcessor.txResource.commitDentry(ctx, txID, pId, name)
	return
}

func (mp *metaPartition) fsmTxCommitRM(ctx context.Context, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if ifo == nil || ifo.Finish() {
		getSpan(ctx).Warnf("fsmTxCommitRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
		return
	}

	mpId := mp.config.PartitionId
	for _, ifo := range txInfo.TxInodeInfos {
		if ifo.MpID != mpId {
			continue
		}
		mp.fsmTxInodeCommit(ctx, ifo.TxID, ifo.Ino)
	}

	for _, ifo := range txInfo.TxDentryInfos {
		if ifo.MpID != mpId {
			continue
		}
		mp.fsmTxDentryCommit(ctx, ifo.TxID, ifo.ParentId, ifo.Name)
	}

	ifo.SetFinish()
	return proto.OpOk
}

func (mp *metaPartition) fsmTxRollbackRM(ctx context.Context, txInfo *proto.TransactionInfo) (status uint8) {
	status = proto.OpOk
	ifo := mp.txProcessor.txManager.copyGetTx(txInfo.TxID)
	if ifo == nil || ifo.Finish() {
		getSpan(ctx).Warnf("fsmTxRollbackRM: tx already commit or rollback before, tx %v, ifo %v", txInfo, ifo)
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
		mp.fsmTxInodeRollback(ctx, req)
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
		mp.fsmTxDentryRollback(ctx, req)
	}

	ifo.SetFinish()
	return proto.OpOk
}

func (mp *metaPartition) inodeInTx(ctx context.Context, inode uint64) uint8 {
	inTx, txId := mp.txProcessor.txResource.isInodeInTransction(NewInode(inode, 0))
	if inTx {
		getSpan(ctx).Warnf("inodeInTx: inode is in transaction, inode %d, txId %s", inode, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}

func (mp *metaPartition) dentryInTx(ctx context.Context, parIno uint64, name string) uint8 {
	inTx, txId := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})

	if inTx {
		getSpan(ctx).Warnf("dentryInTx: inode is in transaction, parent inode %d, name %s, txId %s", parIno, name, txId)
		return proto.OpTxConflictErr
	}
	return proto.OpOk
}

func (mp *metaPartition) txInodeInRb(inode uint64, newTxId string) (rbInode *TxRollbackInode) {
	rbIno := mp.txProcessor.txResource.getTxRbInode(inode)
	if rbIno != nil && rbIno.txInodeInfo.TxID == newTxId {
		return rbIno
	}
	return nil
}

func (mp *metaPartition) txDentryInRb(parIno uint64, name, newTxId string) bool {
	inTx, txId := mp.txProcessor.txResource.isDentryInTransction(&Dentry{
		ParentId: parIno,
		Name:     name,
	})
	return inTx && txId == newTxId
}
