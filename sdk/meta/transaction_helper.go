package meta

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func getMembersFromMp(parentMp *MetaPartition) string {
	var members string

	if parentMp.LeaderAddr != "" {
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

		if members == "" {
			members = parentMp.LeaderAddr
		} else {
			members = parentMp.LeaderAddr + "," + members
		}
	} else {
		for _, addr := range parentMp.Members {
			if members == "" {
				members += addr
			} else {
				members += "," + addr
			}
		}
	}

	return members
}

func NewCreateTransaction(parentMp *MetaPartition, parentID uint64, name string, txTimeout int64) (tx *Transaction, err error) {
	tx = NewTransaction(txTimeout, proto.TxTypeCreate)
	//txInoInfo := proto.NewTxInodeInfo(mpID)

	/*mpAddr := parentMp.LeaderAddr
	if mpAddr == "" {
		log.LogDebugf("NewCreateTransaction: empty LeaderAddr parentMp:(%v)", parentMp)
		num := len(parentMp.Members)
		if num == 0 {
			return nil, fmt.Errorf("invalid parent metapartition")
		} else {
			rand.Seed(time.Now().UnixNano())
			mpAddr = parentMp.Members[rand.Intn(num)]
		}
	}*/

	members := getMembersFromMp(parentMp)
	if members == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	txDentryInfo := proto.NewTxDentryInfo(members, parentID, name, parentMp.PartitionID)
	//if err = tx.AddInode(txInoInfo); err != nil {
	//	return nil, err
	//}
	if err = tx.AddDentry(txDentryInfo); err != nil {
		return nil, err
	}
	log.LogDebugf("NewCreateTransaction: txInfo(%v) parentMp", tx.txInfo)
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
	log.LogDebugf("NewDeleteTransaction: tx(%v)", tx)
	return tx, nil
}

/*func NewDeleteTransaction(
	denMp *MetaPartition, parentID uint64, name string, txTimeout uint32) (tx *Transaction, err error) {
	tx = NewTransaction(txTimeout, proto.TxTypeRemove)

	denMembers := getMembersFromMp(denMp)
	if denMembers == "" {
		return nil, fmt.Errorf("invalid parent metapartition")
	}

	txDentryInfo := proto.NewTxDentryInfo(denMembers, parentID, name, denMp.PartitionID)

	if err = tx.AddDentry(txDentryInfo); err != nil {
		return nil, err
	}
	log.LogDebugf("NewDeleteTransaction: tx(%v)", tx)
	return tx, nil
}*/

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
	log.LogDebugf("NewRenameTransaction: txInfo(%v)", tx.txInfo)
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
	log.LogDebugf("NewLinkTransaction: tx(%v)", tx)
	return tx, nil
}
