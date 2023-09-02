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

package meta

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync/atomic"
)

var txId uint64 = 1

func genTransactionId(clientId uint64) string {
	return fmt.Sprintf("%d_%d", clientId, atomic.AddUint64(&txId, 1))
}

func getMembersFromMp(parentMp *MetaPartition) string {
	var members = parentMp.LeaderAddr
	for _, addr := range parentMp.Members {
		if addr == parentMp.LeaderAddr {
			continue
		}
		if members == "" {
			members += addr
		} else {
			members += "," + addr
		}
	}
	return members
}

func NewCreateTransaction(parentMp, inoMp *MetaPartition, parentID uint64, name string, txTimeout int64, txType uint32) (tx *Transaction, err error) {
	//tx = NewTransaction(txTimeout, proto.TxTypeCreate)
	tx = NewTransaction(txTimeout, txType)

	members := getMembersFromMp(parentMp)
	if members == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	inoMembers := getMembersFromMp(inoMp)

	txDentryInfo := proto.NewTxDentryInfo(members, parentID, name, parentMp.PartitionID)
	txParInoInfo := proto.NewTxInodeInfo(inoMembers, 0, inoMp.PartitionID)
	if err = tx.AddDentry(txDentryInfo); err != nil {
		return nil, err
	}
	if err = tx.AddInode(txParInoInfo); err != nil {
		return nil, err
	}
	if log.EnableDebug() {
		log.LogDebugf("NewCreateTransaction: txInfo(%v) parentMp", tx.txInfo)
	}
	return tx, nil
}

func NewDeleteTransaction(
	denMp *MetaPartition, parentID uint64, name string,
	inoMp *MetaPartition, ino uint64, txTimeout int64) (tx *Transaction, err error) {
	tx = NewTransaction(txTimeout, proto.TxTypeRemove)

	denMembers := getMembersFromMp(denMp)
	if denMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	inoMembers := getMembersFromMp(inoMp)
	if inoMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	txInoInfo := proto.NewTxInodeInfo(inoMembers, ino, inoMp.PartitionID)
	txDentryInfo := proto.NewTxDentryInfo(denMembers, parentID, name, denMp.PartitionID)
	if err = tx.AddInode(txInoInfo); err != nil {
		return nil, err
	}
	if err = tx.AddDentry(txDentryInfo); err != nil {
		return nil, err
	}
	if log.EnableDebug() {
		log.LogDebugf("NewDeleteTransaction: tx(%v)", tx)
	}
	return tx, nil
}

func NewRenameTransaction(srcMp *MetaPartition, srcDenParentID uint64, srcName string,
	dstMp *MetaPartition, dstDenParentID uint64, dstName string, txTimeout int64) (tx *Transaction, err error) {
	tx = NewTransaction(txTimeout, proto.TxTypeRename)

	srcMembers := getMembersFromMp(srcMp)
	if srcMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	dstMembers := getMembersFromMp(dstMp)
	if dstMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	txSrcDentryInfo := proto.NewTxDentryInfo(srcMembers, srcDenParentID, srcName, srcMp.PartitionID)
	txDstDentryInfo := proto.NewTxDentryInfo(dstMembers, dstDenParentID, dstName, dstMp.PartitionID)
	if err = tx.AddDentry(txSrcDentryInfo); err != nil {
		return nil, err
	}
	if err = tx.AddDentry(txDstDentryInfo); err != nil {
		return nil, err
	}

	if log.EnableDebug() {
		log.LogDebugf("NewRenameTransaction: txInfo(%v)", tx.txInfo)
	}
	return tx, nil
}

func RenameTxReplaceInode(tx *Transaction, inoMp *MetaPartition, ino uint64) (err error) {
	inoMembers := getMembersFromMp(inoMp)
	if inoMembers == "" {
		return fmt.Errorf("invalid parent metapartition")
	}
	txInoInfo := proto.NewTxInodeInfo(inoMembers, ino, inoMp.PartitionID)
	_ = tx.AddInode(txInoInfo)
	log.LogDebugf("RenameTxReplaceInode: txInfo(%v)", tx.txInfo)
	return nil
}

func NewLinkTransaction(
	denMp *MetaPartition, parentID uint64, name string,
	inoMp *MetaPartition, ino uint64, txTimeout int64) (tx *Transaction, err error) {
	tx = NewTransaction(txTimeout, proto.TxTypeLink)

	denMembers := getMembersFromMp(denMp)
	if denMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	inoMembers := getMembersFromMp(inoMp)
	if inoMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	txInoInfo := proto.NewTxInodeInfo(inoMembers, ino, inoMp.PartitionID)
	txDentryInfo := proto.NewTxDentryInfo(denMembers, parentID, name, denMp.PartitionID)
	if err = tx.AddInode(txInoInfo); err != nil {
		return nil, err
	}
	if err = tx.AddDentry(txDentryInfo); err != nil {
		return nil, err
	}
	if log.EnableDebug() {
		log.LogDebugf("NewLinkTransaction: tx(%v)", tx)
	}
	return tx, nil
}
