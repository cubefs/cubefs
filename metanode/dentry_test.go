package metanode

import (
	"bytes"
	"io/fs"
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dentryDqual(d1, d2 *Dentry) bool {
	if d1.Name != d2.Name || d1.ParentId != d2.ParentId || d1.Inode != d2.Inode || d1.Type != d2.Type {
		return false
	}

	if d1.multiSnap == nil && d2.multiSnap == nil {
		return true
	}

	if d1.multiSnap.VerSeq == d2.multiSnap.VerSeq {
		return true
	}

	if len(d1.multiSnap.dentryList) != len(d2.multiSnap.dentryList) {
		return false
	}

	for i, dd1 := range d1.multiSnap.dentryList {
		dd2 := d2.multiSnap.dentryList[i]
		if !dentryDqual(dd1, dd2) {
			return false
		}
	}

	return true
}

// Helper function to create a simple dentry without multiSnap
func createSimpleDentry(parentId, inode uint64, name string, fileType uint32) *Dentry {
	return &Dentry{
		ParentId: parentId,
		Inode:    inode,
		Name:     name,
		Type:     fileType,
	}
}

// Helper function to compare dentries (simplified version without multiSnap)
func dentryEqual(d1, d2 *Dentry) bool {
	if d1.Name != d2.Name || d1.ParentId != d2.ParentId || d1.Inode != d2.Inode || d1.Type != d2.Type {
		return false
	}

	// For simple dentries without multiSnap, both should be nil
	if d1.multiSnap == nil && d2.multiSnap == nil {
		return true
	}

	// If one has multiSnap and the other doesn't, they're not equal
	if (d1.multiSnap == nil) != (d2.multiSnap == nil) {
		return false
	}

	// If both have multiSnap, compare basic properties
	if d1.multiSnap != nil && d2.multiSnap != nil {
		return d1.multiSnap.VerSeq == d2.multiSnap.VerSeq
	}

	return true
}

func TestDentryItemMarshalCompitable(t *testing.T) {
	snap := NewDentrySnap(1024)
	snap.dentryList = append(snap.dentryList, &Dentry{
		Name:  "old_name",
		Inode: 1035,
	})

	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Inode:     102,
		Type:      uint32(fs.ModeDir),
		multiSnap: snap,
	}

	// data is dentry d marshald byte by version 3.5.0
	data := []byte{0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 1, 116, 101, 115, 116, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 102, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	d2 := &Dentry{}
	err := d2.Unmarshal(data)
	if err != nil {
		t.Fail()
	}

	if !dentryDqual(d, d2) {
		t.Fail()
	}
}

func TestDentryItemMarshal(t *testing.T) {
	snap := NewDentrySnap(1024)
	snap.dentryList = append(snap.dentryList,
		&Dentry{
			Name:      "old_name",
			Inode:     1035,
			multiSnap: NewDentrySnap(1025),
		},
		&Dentry{
			Name:      "test_2",
			Inode:     1040,
			multiSnap: NewDentrySnap(1025),
		},
	)

	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Type:      uint32(fs.ModeDir),
		Inode:     1024,
		multiSnap: snap,
	}

	buf1 := GetDentryBuf()
	defer PutDentryBuf(buf1)

	err := d.MarshalV2(buf1)
	if err != nil {
		panic(err)
	}

	data2, err := d.Marshal()
	if err != nil || !bytes.Equal(buf1.Bytes(), data2) {
		t.Fail()
	}

	d3 := &Dentry{}
	err = d3.Unmarshal(buf1.Bytes())
	if err != nil {
		t.Fail()
	}

	if !dentryDqual(d, d3) {
		t.Fail()
	}
}

func TestDentryItemMarshalValue(t *testing.T) {
	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Type:      uint32(fs.ModeDir),
		Inode:     1024,
		multiSnap: NewDentrySnap(1024),
	}

	buf1 := GetDentryBuf()
	defer PutDentryBuf(buf1)

	// marshalValue & marshalValueV2
	d.MarshalValueV2(buf1)
	data1 := buf1.Bytes()

	data2 := d.MarshalValue()
	if !bytes.Equal(data1, data2) {
		t.Fail()
	}

	// marshalKey & marshalKeyV2
	buf2 := GetDentryBuf()
	defer PutDentryBuf(buf2)

	d.MarshalKeyV2(buf2)
	data1 = buf2.Bytes()

	data2 = d.MarshalKey()
	if !bytes.Equal(data1, data2) {
		t.Fail()
	}
}

func TestDentryItemBasicProperties(t *testing.T) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))

	assert.Equal(t, uint64(1), d.ParentId)
	assert.Equal(t, uint64(100), d.Inode)
	assert.Equal(t, "testfile", d.Name)
	assert.Equal(t, uint32(os.ModeDir), d.Type)
	assert.Nil(t, d.multiSnap)
}

func TestDentryItemString(t *testing.T) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))
	str := d.String()

	assert.Contains(t, str, "testfile")
	assert.Contains(t, str, "parentId:[1]")
	assert.Contains(t, str, "inode:[100]")
	assert.Contains(t, str, "dentryList_len[0]")
}

func TestDentryItemLess(t *testing.T) {
	d1 := createSimpleDentry(1, 100, "afile", uint32(os.ModeDir))
	d2 := createSimpleDentry(1, 101, "bfile", uint32(os.ModeDir))
	d3 := createSimpleDentry(2, 102, "afile", uint32(os.ModeDir))

	// Same parent, different names
	assert.True(t, d1.Less(d2))
	assert.False(t, d2.Less(d1))

	// Different parents
	assert.True(t, d1.Less(d3))
	assert.False(t, d3.Less(d1))

	// Same dentry
	assert.False(t, d1.Less(d1))
}

func TestDentryItemCopyDirectly(t *testing.T) {
	original := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))
	copied := original.CopyDirectly().(*Dentry)

	assert.Equal(t, original.ParentId, copied.ParentId)
	assert.Equal(t, original.Inode, copied.Inode)
	assert.Equal(t, original.Name, copied.Name)
	assert.Equal(t, original.Type, copied.Type)
	assert.Nil(t, copied.multiSnap)

	// Ensure it's a different instance
	assert.False(t, original == copied)
}

func TestDentryItemCopy(t *testing.T) {
	original := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))
	copied := original.Copy().(*Dentry)

	assert.Equal(t, original.ParentId, copied.ParentId)
	assert.Equal(t, original.Inode, copied.Inode)
	assert.Equal(t, original.Name, copied.Name)
	assert.Equal(t, original.Type, copied.Type)

	// Ensure it's a different instance
	assert.False(t, original == copied)
}

func TestDentryItemMarshalKey(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	key := d.MarshalKey()
	assert.NotEmpty(t, key)
	assert.Len(t, key, 8+len("testfile")) // 8 bytes for ParentId + name length

	// Test unmarshaling
	d2 := &Dentry{}
	err := d2.UnmarshalKey(key)
	require.NoError(t, err)

	assert.Equal(t, d.ParentId, d2.ParentId)
	assert.Equal(t, d.Name, d2.Name)
}

func TestDentryItemMarshalKeyV2(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	d.MarshalKeyV2(buf)
	key := buf.Bytes()

	assert.NotEmpty(t, key)
	assert.Len(t, key, 8+len("testfile"))

	// Test unmarshaling
	d2 := &Dentry{}
	err := d2.UnmarshalKey(key)
	require.NoError(t, err)

	assert.Equal(t, d.ParentId, d2.ParentId)
	assert.Equal(t, d.Name, d2.Name)
}

func TestDentryItemMarshalValueSimple(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	value := d.MarshalValue()
	assert.NotEmpty(t, value)
	assert.Len(t, value, 12) // 8 bytes for Inode + 4 bytes for Type

	// Test unmarshaling
	d2 := &Dentry{}
	err := d2.UnmarshalValue(value)
	require.NoError(t, err)

	assert.Equal(t, d.Inode, d2.Inode)
	assert.Equal(t, d.Type, d2.Type)
}

func TestDentryItemMarshalValueV2Simple(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	d.MarshalValueV2(buf)
	value := buf.Bytes()

	assert.NotEmpty(t, value)
	assert.Len(t, value, 12)

	// Test unmarshaling
	d2 := &Dentry{}
	err := d2.UnmarshalValue(value)
	require.NoError(t, err)

	assert.Equal(t, d.Inode, d2.Inode)
	assert.Equal(t, d.Type, d2.Type)
}

func TestDentryItemMarshalSimple(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	data, err := d.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemMarshalV2Simple(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	err := d.MarshalV2(buf)
	require.NoError(t, err)

	data := buf.Bytes()
	assert.NotEmpty(t, data)

	// Test unmarshaling
	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemMarshalCompatibility(t *testing.T) {
	// Test that Marshal and MarshalV2 produce compatible results
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	data1, err := d.Marshal()
	require.NoError(t, err)

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	err = d.MarshalV2(buf)
	require.NoError(t, err)
	data2 := buf.Bytes()

	assert.Equal(t, data1, data2)
}

func TestDentryItemBatchMarshal(t *testing.T) {
	batch := DentryBatch{
		createSimpleDentry(1, 100, "file1", uint32(os.ModeDir)),
		createSimpleDentry(1, 101, "file2", uint32(os.ModeDir)),
		createSimpleDentry(2, 102, "file3", uint32(fs.ModeDir)),
	}

	data, err := batch.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	batch2, err := DentryBatchUnmarshal(data)
	require.NoError(t, err)

	assert.Len(t, batch2, 3)
	for i, d := range batch {
		assert.True(t, dentryEqual(d, batch2[i]))
	}
}

func TestDentryItemBatchUnmarshal(t *testing.T) {
	// Test empty batch
	emptyData := []byte{0, 0, 0, 0} // length = 0
	batch, err := DentryBatchUnmarshal(emptyData)
	require.NoError(t, err)
	assert.Len(t, batch, 0)

	// Test single dentry batch
	singleDentry := createSimpleDentry(1, 100, "single", uint32(os.ModeDir))
	singleData, err := DentryBatch{singleDentry}.Marshal()
	require.NoError(t, err)

	batch, err = DentryBatchUnmarshal(singleData)
	require.NoError(t, err)
	assert.Len(t, batch, 1)
	assert.True(t, dentryEqual(singleDentry, batch[0]))
}

func TestDentryItemTxDentryCreation(t *testing.T) {
	parentInode := &Inode{}
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)

	txDentry := NewTxDentry(1, "testfile", 100, uint32(os.ModeDir), parentInode, txInfo)

	assert.NotNil(t, txDentry.Dentry)
	assert.NotNil(t, txDentry.TxInfo)
	assert.Equal(t, uint64(1), txDentry.Dentry.ParentId)
	assert.Equal(t, "testfile", txDentry.Dentry.Name)
	assert.Equal(t, uint64(100), txDentry.Dentry.Inode)
	assert.Equal(t, uint32(os.ModeDir), txDentry.Dentry.Type)
}

func TestDentryItemTxDentryMarshal(t *testing.T) {
	parentInode := &Inode{}
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)
	txDentry := NewTxDentry(1, "testfile", 100, uint32(os.ModeDir), parentInode, txInfo)

	data, err := txDentry.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	txDentry2 := &TxDentry{}
	err = txDentry2.Unmarshal(data)
	require.NoError(t, err)

	assert.True(t, dentryEqual(txDentry.Dentry, txDentry2.Dentry))
	assert.Equal(t, txDentry.TxInfo.TxID, txDentry2.TxInfo.TxID)
}

func TestDentryItemTxDentryUnmarshal(t *testing.T) {
	parentInode := &Inode{}
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)
	txDentry := NewTxDentry(1, "testfile", 100, uint32(os.ModeDir), parentInode, txInfo)

	data, err := txDentry.Marshal()
	require.NoError(t, err)

	txDentry2 := &TxDentry{}
	err = txDentry2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, txDentry.Dentry.ParentId, txDentry2.Dentry.ParentId)
	assert.Equal(t, txDentry.Dentry.Name, txDentry2.Dentry.Name)
	assert.Equal(t, txDentry.Dentry.Inode, txDentry2.Dentry.Inode)
	assert.Equal(t, txDentry.Dentry.Type, txDentry2.Dentry.Type)
}

func TestDentryItemTxUpdateDentryCreation(t *testing.T) {
	oldDentry := createSimpleDentry(1, 100, "oldfile", uint32(os.ModeDir))
	newDentry := createSimpleDentry(1, 101, "newfile", uint32(os.ModeDir))
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)

	txUpdateDentry := NewTxUpdateDentry(oldDentry, newDentry, txInfo)

	assert.NotNil(t, txUpdateDentry.OldDentry)
	assert.NotNil(t, txUpdateDentry.NewDentry)
	assert.NotNil(t, txUpdateDentry.TxInfo)
	assert.True(t, dentryEqual(oldDentry, txUpdateDentry.OldDentry))
	assert.True(t, dentryEqual(newDentry, txUpdateDentry.NewDentry))
}

func TestDentryItemTxUpdateDentryMarshal(t *testing.T) {
	oldDentry := createSimpleDentry(1, 100, "oldfile", uint32(os.ModeDir))
	newDentry := createSimpleDentry(1, 101, "newfile", uint32(os.ModeDir))
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)
	txUpdateDentry := NewTxUpdateDentry(oldDentry, newDentry, txInfo)

	data, err := txUpdateDentry.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	txUpdateDentry2 := &TxUpdateDentry{}
	err = txUpdateDentry2.Unmarshal(data)
	require.NoError(t, err)

	assert.True(t, dentryEqual(txUpdateDentry.OldDentry, txUpdateDentry2.OldDentry))
	assert.True(t, dentryEqual(txUpdateDentry.NewDentry, txUpdateDentry2.NewDentry))
	assert.Equal(t, txUpdateDentry.TxInfo.TxID, txUpdateDentry2.TxInfo.TxID)
}

func TestDentryItemTxUpdateDentryUnmarshal(t *testing.T) {
	oldDentry := createSimpleDentry(1, 100, "oldfile", uint32(os.ModeDir))
	newDentry := createSimpleDentry(1, 101, "newfile", uint32(os.ModeDir))
	txInfo := proto.NewTransactionInfo(1000, proto.TxTypeUndefined)
	txUpdateDentry := NewTxUpdateDentry(oldDentry, newDentry, txInfo)

	data, err := txUpdateDentry.Marshal()
	require.NoError(t, err)

	txUpdateDentry2 := &TxUpdateDentry{}
	err = txUpdateDentry2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, txUpdateDentry.OldDentry.ParentId, txUpdateDentry2.OldDentry.ParentId)
	assert.Equal(t, txUpdateDentry.OldDentry.Name, txUpdateDentry2.OldDentry.Name)
	assert.Equal(t, txUpdateDentry.NewDentry.ParentId, txUpdateDentry2.NewDentry.ParentId)
	assert.Equal(t, txUpdateDentry.NewDentry.Name, txUpdateDentry2.NewDentry.Name)
}

func TestDentryItemUnmarshalKeyError(t *testing.T) {
	d := &Dentry{}

	// Test with insufficient data
	err := d.UnmarshalKey([]byte{1, 2, 3}) // Less than 8 bytes
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dentry key length less than 8")
}

func TestDentryItemMarshalKeyConsistency(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	// Test that MarshalKey and MarshalKeyV2 produce the same result
	key1 := d.MarshalKey()

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	d.MarshalKeyV2(buf)
	key2 := buf.Bytes()

	assert.Equal(t, key1, key2)
}

func TestDentryItemMarshalValueConsistency(t *testing.T) {
	d := createSimpleDentry(12345, 67890, "testfile", uint32(os.ModeDir))

	// Test that MarshalValue and MarshalValueV2 produce the same result
	value1 := d.MarshalValue()

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	d.MarshalValueV2(buf)
	value2 := buf.Bytes()

	assert.Equal(t, value1, value2)
}

func TestDentryItemWithDifferentTypes(t *testing.T) {
	testCases := []struct {
		name     string
		fileType uint32
	}{
		{"regular_file", uint32(os.ModeDir)},
		{"directory", uint32(fs.ModeDir)},
		{"symlink", uint32(fs.ModeSymlink)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := createSimpleDentry(1, 100, tc.name, tc.fileType)

			data, err := d.Marshal()
			require.NoError(t, err)

			d2 := &Dentry{}
			err = d2.Unmarshal(data)
			require.NoError(t, err)

			assert.Equal(t, tc.fileType, d2.Type)
			assert.True(t, dentryEqual(d, d2))
		})
	}
}

func TestDentryItemWithLongNames(t *testing.T) {
	longName := "this_is_a_very_long_filename_that_might_cause_issues_with_serialization_and_deserialization"
	d := createSimpleDentry(1, 100, longName, uint32(os.ModeDir))

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, longName, d2.Name)
	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemWithSpecialCharacters(t *testing.T) {
	specialName := "file with spaces & special chars!@#$%^&*()"
	d := createSimpleDentry(1, 100, specialName, uint32(os.ModeDir))

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, specialName, d2.Name)
	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemWithUnicodeNames(t *testing.T) {
	unicodeName := "文件名称_测试_中文"
	d := createSimpleDentry(1, 100, unicodeName, uint32(os.ModeDir))

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, unicodeName, d2.Name)
	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemWithEmptyName(t *testing.T) {
	// Test edge case with empty name
	d := createSimpleDentry(1, 100, "", uint32(os.ModeDir))

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, "", d2.Name)
	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemWithZeroValues(t *testing.T) {
	// Test edge case with zero values
	d := createSimpleDentry(0, 0, "test", 0)

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, uint64(0), d2.ParentId)
	assert.Equal(t, uint64(0), d2.Inode)
	assert.Equal(t, uint32(0), d2.Type)
	assert.True(t, dentryEqual(d, d2))
}

func TestDentryItemWithMaxValues(t *testing.T) {
	// Test edge case with maximum values
	d := createSimpleDentry(^uint64(0), ^uint64(0), "test", ^uint32(0))

	data, err := d.Marshal()
	require.NoError(t, err)

	d2 := &Dentry{}
	err = d2.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, ^uint64(0), d2.ParentId)
	assert.Equal(t, ^uint64(0), d2.Inode)
	assert.Equal(t, ^uint32(0), d2.Type)
	assert.True(t, dentryEqual(d, d2))
}

func BenchmarkDentryMarshal(b *testing.B) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := d.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDentryUnmarshal(b *testing.B) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))
	data, err := d.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d2 := &Dentry{}
		err := d2.Unmarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDentryMarshalV2(b *testing.B) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))
	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := d.MarshalV2(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDentryBatchMarshal(b *testing.B) {
	batch := make(DentryBatch, 100)
	for i := 0; i < 100; i++ {
		batch[i] = createSimpleDentry(uint64(i), uint64(i+1000), "file"+string(rune(i)), uint32(os.ModeDir))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := batch.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDentryBatchUnmarshal(b *testing.B) {
	batch := make(DentryBatch, 100)
	for i := 0; i < 100; i++ {
		batch[i] = createSimpleDentry(uint64(i), uint64(i+1000), "file"+string(rune(i)), uint32(os.ModeDir))
	}

	data, err := batch.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DentryBatchUnmarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDentryLess(b *testing.B) {
	d1 := createSimpleDentry(1, 100, "afile", uint32(os.ModeDir))
	d2 := createSimpleDentry(1, 101, "bfile", uint32(os.ModeDir))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = d1.Less(d2)
	}
}

func BenchmarkDentryCopy(b *testing.B) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = d.Copy()
	}
}

func BenchmarkDentryCopyDirectly(b *testing.B) {
	d := createSimpleDentry(1, 100, "testfile", uint32(os.ModeDir))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = d.CopyDirectly()
	}
}
