package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestMemSnapShot_RangeWithScope(t *testing.T) {
	// 构造测试用的 InodeBTree 和 DentryBTree
	inodeTree := &InodeBTree{NewBtree()}
	dentryTree := &DentryBTree{NewBtree()}

	// 插入测试数据
	inode1 := &Inode{Inode: 1}
	inode2 := &Inode{Inode: 2}
	inodeTree.ReplaceOrInsert(nil, inode1, true)
	inodeTree.ReplaceOrInsert(nil, inode2, true)
	dentry1 := &Dentry{ParentId: 1, Name: "a", Inode: 100}
	dentry2 := &Dentry{ParentId: 1, Name: "b", Inode: 101}
	dentryTree.ReplaceOrInsert(nil, dentry1, true)
	dentryTree.ReplaceOrInsert(nil, dentry2, true)

	// 测试 ExtendType
	extendTree := &ExtendBTree{NewBtree()}
	extend1 := &Extend{inode: 1, dataMap: map[string][]byte{"k1": []byte("v1")}}
	extend2 := &Extend{inode: 2, dataMap: map[string][]byte{"k2": []byte("v2")}}
	extendTree.ReplaceOrInsert(nil, extend1, true)
	extendTree.ReplaceOrInsert(nil, extend2, true)

	// MultipartType
	multipartTree := &MultipartBTree{NewBtree()}
	multipart1 := &Multipart{id: "id1", key: "key1"}
	multipart2 := &Multipart{id: "id2", key: "key2"}
	multipartTree.ReplaceOrInsert(nil, multipart1, true)
	multipartTree.ReplaceOrInsert(nil, multipart2, true)

	// TransactionType
	transactionTree := &TransactionBTree{NewBtree()}
	tx1 := proto.NewTransactionInfo(0, 0)
	tx1.TxID = "1"
	tx2 := proto.NewTransactionInfo(0, 0)
	tx2.TxID = "2"
	transactionTree.ReplaceOrInsert(nil, tx1, true)
	transactionTree.ReplaceOrInsert(nil, tx2, true)

	// TransactionRollbackInodeType
	txRbInodeTree := &TransactionRollbackInodeBTree{NewBtree()}
	txRbInodeTree.ReplaceOrInsert(nil, NewTxRollbackInode(inode1, []uint32{}, proto.NewTxInodeInfo("tx1", 1, 1), 0), true)
	txRbInodeTree.ReplaceOrInsert(nil, NewTxRollbackInode(inode2, []uint32{}, proto.NewTxInodeInfo("tx2", 2, 2), 0), true)

	// TransactionRollbackDentryType
	txRbDentryTree := &TransactionRollbackDentryBTree{NewBtree()}
	txRbDentryTree.ReplaceOrInsert(nil, NewTxRollbackDentry(dentry1, proto.NewTxDentryInfo("tx1", 1, "dentry1", 1), 0), true)
	txRbDentryTree.ReplaceOrInsert(nil, NewTxRollbackDentry(dentry2, proto.NewTxDentryInfo("tx2", 2, "dentry2", 2), 0), true)

	snap := &MemSnapShot{
		inode:               inodeTree,
		dentry:              dentryTree,
		extend:              extendTree,
		multipart:           multipartTree,
		transaction:         transactionTree,
		transactionRbInode:  txRbInodeTree,
		transactionRbDentry: txRbDentryTree,
	}

	// 测试 InodeType
	var inodes []*Inode
	err := snap.RangeWithScope(InodeType, nil, nil, func(item interface{}) bool {
		inodes = append(inodes, item.(*Inode))
		return true
	})
	require.NoError(t, err)
	require.Len(t, inodes, 2)
	require.Equal(t, uint64(1), inodes[0].Inode)
	require.Equal(t, uint64(2), inodes[1].Inode)

	// 测试 DentryType
	var dentries []*Dentry
	err = snap.RangeWithScope(DentryType, nil, nil, func(item interface{}) bool {
		dentries = append(dentries, item.(*Dentry))
		return true
	})
	require.NoError(t, err)
	require.Len(t, dentries, 2)
	require.Equal(t, "a", dentries[0].Name)
	require.Equal(t, "b", dentries[1].Name)

	// ExtendType
	var extends []*Extend
	err = snap.RangeWithScope(ExtendType, nil, nil, func(item interface{}) bool {
		extends = append(extends, item.(*Extend))
		return true
	})
	require.NoError(t, err)
	require.Len(t, extends, 2)

	// MultipartType
	var multiparts []*Multipart
	err = snap.RangeWithScope(MultipartType, nil, nil, func(item interface{}) bool {
		multiparts = append(multiparts, item.(*Multipart))
		return true
	})
	require.NoError(t, err)
	require.Len(t, multiparts, 2)

	// TransactionType
	var txs []*proto.TransactionInfo
	err = snap.RangeWithScope(TransactionType, nil, nil, func(item interface{}) bool {
		txs = append(txs, item.(*proto.TransactionInfo))
		return true
	})
	require.NoError(t, err)
	require.Len(t, txs, 2)

	// TransactionRollbackInodeType
	var txRbInodes []*TxRollbackInode
	err = snap.RangeWithScope(TransactionRollbackInodeType, nil, nil, func(item interface{}) bool {
		txRbInodes = append(txRbInodes, item.(*TxRollbackInode))
		return true
	})
	require.NoError(t, err)
	require.Len(t, txRbInodes, 2)

	// TransactionRollbackDentryType
	var txRbDentries []*TxRollbackDentry
	err = snap.RangeWithScope(TransactionRollbackDentryType, nil, nil, func(item interface{}) bool {
		txRbDentries = append(txRbDentries, item.(*TxRollbackDentry))
		return true
	})
	require.NoError(t, err)
	require.Len(t, txRbDentries, 2)
}
