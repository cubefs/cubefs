package metanode

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestInodeKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	inodeId := uint64(67890)

	// Build a tree with a fixed partition ID so both encoding paths use the same prefix.
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path: write the partition/table prefix first, then append the inode ID.
	keyBuf := tree.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := inodeEncodingKey(keyBuf, inodeId)

	// Legacy encoding path: build the table-local key, then wrap it with the partition prefix.
	keyV0 := inodeEncodingKeyV0(inodeId)
	key2 := tree.warpKeyV0(keyV0)

	// The optimized encoding must remain byte-for-byte compatible with the legacy format.
	if !bytes.Equal(key1, key2) {
		t.Errorf("key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestDentryKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	parentId := uint64(67890)
	name := "test_dentry"

	// Build a tree with a fixed partition ID so both encoding paths use the same prefix.
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path: write the partition/table prefix before appending parent and name.
	keyBuf := tree.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := dentryEncodingKey(keyBuf, parentId, name)

	// Legacy encoding path: build the table-local dentry key and wrap it with the partition.
	keyV0 := dentryEncodingKeyV0(parentId, name)
	key2 := tree.warpKeyV0(keyV0)

	// Dentry key layout must stay compatible for data written by older versions.
	if !bytes.Equal(key1, key2) {
		t.Errorf("DentryTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestExtendKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	ino := uint64(67890)
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path appends the extend inode ID to the prebuilt RocksDB key prefix.
	keyBuf := tree.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := extendEncodingKey(keyBuf, ino)

	// Legacy encoding path wraps the old table-local extend key with the partition prefix.
	keyV0 := extendEncodingKeyV0(ino)
	key2 := tree.warpKeyV0(keyV0)

	// The extend key format must remain stable across encoding implementations.
	if !bytes.Equal(key1, key2) {
		t.Errorf("ExtendTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestMultipartKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	keyStr := "multipart_key"
	id := "part_id"
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path appends multipart object key and upload ID to the long key prefix.
	keyBuf := tree.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := multipartEncodingKey(keyBuf, keyStr, id)

	// Legacy encoding path creates the table-local multipart key before partition wrapping.
	keyV0 := multipartEncodingKeyV0(keyStr, id)
	key2 := tree.warpKeyV0(keyV0)

	// Multipart key compatibility preserves access to existing uploads after upgrades.
	if !bytes.Equal(key1, key2) {
		t.Errorf("MultipartTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	txId := "tx_abc"
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path appends the transaction ID to the transaction table prefix.
	keyBuf := tree.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := transactionEncodingKey(keyBuf, txId)

	// Legacy encoding path wraps the old transaction key with the same partition prefix.
	keyV0 := transactionEncodingKeyV0(txId)
	key2 := tree.warpKeyV0(keyV0)

	// Transaction keys must remain compatible with data created before the encoding change.
	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionRollbackInodeKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	ino := uint64(67890)
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path appends the rollback inode ID to the rollback inode table prefix.
	keyBuf := tree.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := transactionRollbackInodeEncodingKey(keyBuf, ino)

	// Legacy encoding path builds the old rollback inode key and wraps it by partition.
	keyV0 := transactionRollbackInodeEncodingKeyV0(ino)
	key2 := tree.warpKeyV0(keyV0)

	// Rollback inode keys must remain readable after switching encoding implementations.
	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionRollbackInodeTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionRollbackDentryKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	parentId := uint64(67890)
	name := "tx_dentry"
	tree := &RocksTree{partitionId: partitionId}

	// New encoding path appends rollback dentry fields to the long key prefix.
	keyBuf := tree.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := transactionRollbackDentryEncodingKey(keyBuf, parentId, name)

	// Legacy encoding path builds the table-local rollback dentry key before wrapping.
	keyV0 := transactionRollbackDentryEncodingKeyV0(parentId, name)
	key2 := tree.warpKeyV0(keyV0)

	// Rollback dentry key layout must remain stable for transaction recovery data.
	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionRollbackDentryTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestBaseInfoKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	tree := &RocksTree{partitionId: partitionId}

	// Base info keys contain only the partition prefix and table type.
	keyBuf := tree.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := keyBuf.Bytes()

	// Compare the new prefix builder against the legacy wrapping helper.
	key2 := tree.warpKeyV0([]byte{byte(BaseInfoType)})

	// Base info must stay at the same key so existing metadata can be loaded.
	if !bytes.Equal(key1, key2) {
		t.Errorf("BaseInfoTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestRocksBaseInfoMarshalCompatibility(t *testing.T) {
	// Populate every field included in the persisted base info payload.
	info := &RocksBaseInfo{
		version:           1,
		length:            128,
		applyId:           1001,
		inodeCnt:          2002,
		dentryCnt:         3003,
		extendCnt:         4004,
		multiCnt:          5005,
		persistentApplyId: 1001,
		cursor:            6006,
		txCnt:             7007,
		txRbInodeCnt:      8008,
		txRbDentryCnt:     9009,
		txId:              1010,
		uniqID:            2020,
	}

	// Marshal through both the legacy binary writer and the optimized ByteBuf path.
	dataV0, err := info.MarshalV0()
	if err != nil {
		t.Fatalf("MarshalV0 failed: %v", err)
	}
	data, err := info.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// The optimized marshal output must be byte-for-byte compatible with the legacy output.
	if !bytes.Equal(dataV0, data) {
		t.Errorf("MarshalV0 and Marshal result not equal\nMarshalV0: %v\nMarshal: %v", dataV0, data)
	}
}

func TestRocksBaseInfoMarshalWithoutApplyIDCompatibility(t *testing.T) {
	// Populate the fields used when persisting base info without the volatile apply ID.
	info := &RocksBaseInfo{
		version:           2,
		length:            256,
		persistentApplyId: 1002,
		inodeCnt:          2003,
		dentryCnt:         3004,
		extendCnt:         4005,
		multiCnt:          5006,
		cursor:            6007,
		txCnt:             7008,
		txRbInodeCnt:      8009,
		txRbDentryCnt:     9010,
		txId:              1011,
		uniqID:            2021,
	}

	// Compare the legacy and optimized encoders for the no-apply-ID payload.
	dataV0, err := info.MarshalWithoutApplyIDV0()
	if err != nil {
		t.Fatalf("MarshalWithoutApplyIDV0 failed: %v", err)
	}
	data, err := info.MarshalWithoutApplyID()
	if err != nil {
		t.Fatalf("MarshalWithoutApplyID failed: %v", err)
	}

	// Persisting without apply ID must keep the same binary layout for compatibility.
	if !bytes.Equal(dataV0, data) {
		t.Errorf("MarshalWithoutApplyIDV0 and MarshalWithoutApplyID result not equal\nMarshalWithoutApplyIDV0: %v\nMarshalWithoutApplyID: %v", dataV0, data)
	}
}

func TestRocksBaseInfoMarshalUnmarshalRoundTrip(t *testing.T) {
	// Build a base info value with every persisted counter populated.
	info := &RocksBaseInfo{
		version:       3,
		length:        512,
		applyId:       11,
		inodeCnt:      22,
		dentryCnt:     33,
		extendCnt:     44,
		multiCnt:      55,
		cursor:        66,
		txCnt:         77,
		txRbInodeCnt:  88,
		txRbDentryCnt: 99,
		txId:          111,
		uniqID:        222,
	}

	// Marshal and unmarshal through the production encoding path.
	data, err := info.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded RocksBaseInfo
	if err = decoded.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify that all persisted fields survive the round trip.
	if decoded.version != info.version ||
		decoded.length != info.length ||
		decoded.applyId != info.applyId ||
		decoded.persistentApplyId != info.applyId ||
		decoded.inodeCnt != info.inodeCnt ||
		decoded.dentryCnt != info.dentryCnt ||
		decoded.extendCnt != info.extendCnt ||
		decoded.multiCnt != info.multiCnt ||
		decoded.cursor != info.cursor ||
		decoded.txCnt != info.txCnt ||
		decoded.txRbInodeCnt != info.txRbInodeCnt ||
		decoded.txRbDentryCnt != info.txRbDentryCnt ||
		decoded.txId != info.txId ||
		decoded.uniqID != info.uniqID {
		t.Fatalf("decoded base info mismatch\ndecoded: %+v\nsource: %+v", decoded, info)
	}
}

func TestRocksBaseInfoUnmarshalReturnsErrorForShortInput(t *testing.T) {
	// A truncated payload should fail before partially initialized data is trusted.
	info := &RocksBaseInfo{}
	if err := info.Unmarshal([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for short base info input")
	}
}

func TestRocksTreeSettersAndMonotonicFields(t *testing.T) {
	tree := &RocksTree{}

	// Apply ID is directly stored and should be readable immediately.
	tree.SetApplyID(10)
	if got := tree.GetApplyID(); got != 10 {
		t.Fatalf("GetApplyID = %d, want 10", got)
	}

	// Transaction ID only moves forward and ignores smaller values.
	tree.SetTxId(7)
	tree.SetTxId(3)
	if got := tree.GetTxId(); got != 7 {
		t.Fatalf("GetTxId = %d, want 7", got)
	}

	// Cursor follows the same monotonic update rule.
	tree.SetCursor(9)
	tree.SetCursor(4)
	if got := tree.GetCursor(); got != 9 {
		t.Fatalf("GetCursor = %d, want 9", got)
	}

	// Unique ID is stored directly because callers control its lifecycle.
	tree.SetUniqID(12)
	if got := tree.GetUniqID(); got != 12 {
		t.Fatalf("GetUniqID = %d, want 12", got)
	}
}

func TestRocksTreeKeyBufferLayout(t *testing.T) {
	const partitionID = uint64(0x0102030405060708)
	tree := &RocksTree{partitionId: partitionID}

	// Normal keys start with the partition ID followed by the table type.
	normalKey := tree.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(normalKey)
	normalBytes := normalKey.Bytes()
	if len(normalBytes) != RocksdbTypeIndex+1 {
		t.Fatalf("normal key len = %d, want %d", len(normalBytes), RocksdbTypeIndex+1)
	}
	if got := binary.BigEndian.Uint64(normalBytes[:RocksdbTypeIndex]); got != partitionID {
		t.Fatalf("normal key partition = %d, want %d", got, partitionID)
	}
	if got := normalBytes[RocksdbTypeIndex]; got != byte(InodeTable) {
		t.Fatalf("normal key table = %d, want %d", got, byte(InodeTable))
	}

	// Long keys share the same fixed prefix layout as normal keys.
	longKey := tree.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(longKey)
	longBytes := longKey.Bytes()
	if len(longBytes) != RocksdbTypeIndex+1 {
		t.Fatalf("long key len = %d, want %d", len(longBytes), RocksdbTypeIndex+1)
	}
	if got := binary.BigEndian.Uint64(longBytes[:RocksdbTypeIndex]); got != partitionID {
		t.Fatalf("long key partition = %d, want %d", got, partitionID)
	}
	if got := longBytes[RocksdbTypeIndex]; got != byte(DentryTable) {
		t.Fatalf("long key table = %d, want %d", got, byte(DentryTable))
	}
}

func TestEncodingPrefixes(t *testing.T) {
	tree := &RocksTree{partitionId: 12345}

	// Dentry prefixes should match every key under the same parent.
	dentryPrefixBuf := tree.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(dentryPrefixBuf)
	dentryPrefix := append([]byte(nil), dentryEncodingPrefix(dentryPrefixBuf, 100, "")...)

	dentryKeyBuf := tree.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(dentryKeyBuf)
	dentryKey := dentryEncodingKey(dentryKeyBuf, 100, "child")
	if !bytes.HasPrefix(dentryKey, dentryPrefix) {
		t.Fatalf("dentry key %v does not have prefix %v", dentryKey, dentryPrefix)
	}

	// Multipart prefixes should match all upload IDs for the same object key.
	multipartPrefixBuf := tree.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(multipartPrefixBuf)
	multipartPrefix := append([]byte(nil), multipartEncodingPrefix(multipartPrefixBuf, "object", "")...)

	multipartKeyBuf := tree.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(multipartKeyBuf)
	multipartKey := multipartEncodingKey(multipartKeyBuf, "object", "upload")
	if !bytes.HasPrefix(multipartKey, multipartPrefix) {
		t.Fatalf("multipart key %v does not have prefix %v", multipartKey, multipartPrefix)
	}

	// Rollback dentry prefixes should match rollback keys for the same parent.
	rollbackPrefixBuf := tree.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(rollbackPrefixBuf)
	rollbackPrefix := append([]byte(nil), transactionRollbackDentryEncodingPrefix(rollbackPrefixBuf, 200, "")...)

	rollbackKeyBuf := tree.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(rollbackKeyBuf)
	rollbackKey := transactionRollbackDentryEncodingKey(rollbackKeyBuf, 200, "name")
	if !bytes.HasPrefix(rollbackKey, rollbackPrefix) {
		t.Fatalf("rollback dentry key %v does not have prefix %v", rollbackKey, rollbackPrefix)
	}
}

func TestRocksCountAndLenAccessors(t *testing.T) {
	// Seed the shared base info counters used by the typed Rocks wrappers.
	tree := &RocksTree{baseInfo: RocksBaseInfo{
		inodeCnt:      1,
		dentryCnt:     2,
		extendCnt:     3,
		multiCnt:      4,
		txCnt:         5,
		txRbInodeCnt:  6,
		txRbDentryCnt: 7,
	}}

	// Len delegates to Count for metadata-backed trees.
	if got := (&InodeRocks{RocksTree: tree}).Len(); got != 1 {
		t.Fatalf("inode len = %d, want 1", got)
	}
	if got := (&DentryRocks{RocksTree: tree}).Len(); got != 2 {
		t.Fatalf("dentry len = %d, want 2", got)
	}
	if got := (&ExtendRocks{RocksTree: tree}).Len(); got != 3 {
		t.Fatalf("extend len = %d, want 3", got)
	}
	if got := (&MultipartRocks{RocksTree: tree}).Len(); got != 4 {
		t.Fatalf("multipart len = %d, want 4", got)
	}
	if got := (&TransactionRocks{RocksTree: tree}).Len(); got != 5 {
		t.Fatalf("transaction len = %d, want 5", got)
	}
	if got := (&TransactionRollbackInodeRocks{RocksTree: tree}).Len(); got != 6 {
		t.Fatalf("transaction rollback inode len = %d, want 6", got)
	}
	if got := (&TransactionRollbackDentryRocks{RocksTree: tree}).Len(); got != 7 {
		t.Fatalf("transaction rollback dentry len = %d, want 7", got)
	}

	// Deleted extent trees do not maintain persisted counters.
	if got := (&DeletedExtentsRocks{RocksTree: tree}).Len(); got != 0 {
		t.Fatalf("deleted extents len = %d, want 0", got)
	}
	if got := (&DeletedObjExtentsRocks{RocksTree: tree}).Len(); got != 0 {
		t.Fatalf("deleted obj extents len = %d, want 0", got)
	}
}
