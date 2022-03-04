package metanode

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func newTestRocksTree(dir string) (rocksTree *RocksTree) {
	err := os.RemoveAll(dir)
	if err != nil {
		fmt.Printf("remove db dir(%s) failed\n", dir)
		os.Exit(1)
	}
	rocksdbHandle := NewRocksDb()
	if err = rocksdbHandle.OpenDb(dir); err != nil {
		fmt.Printf("open db without exist dir(%s) failed:%v\n", dir, err)
		os.Exit(1)
	}
	rocksTree, err = DefaultRocksTree(rocksdbHandle)
	if err != nil {
		fmt.Printf("new rocks tree in dir(%s) failed:%v\n", dir, err)
		os.Exit(1)
	}
	return
}

func mockTree(rocksTree *RocksTree, treeType TreeType) (memModeTree, rocksModeTree interface{}) {
	switch treeType {
	case InodeType:
		memModeTree = &InodeBTree{NewBtree()}
		rocksModeTree, _ = NewInodeRocks(rocksTree)
	case DentryType:
		memModeTree = &DentryBTree{NewBtree()}
		rocksModeTree, _ = NewDentryRocks(rocksTree)
	case MultipartType:
		memModeTree = &MultipartBTree{NewBtree()}
		rocksModeTree, _ = NewMultipartRocks(rocksTree)
	case ExtendType:
		memModeTree = &ExtendBTree{NewBtree()}
		rocksModeTree, _ = NewExtendRocks(rocksTree)
	case DelDentryType:
		memModeTree = &DeletedDentryBTree{NewBtree()}
		rocksModeTree, _ = NewDeletedDentryRocks(rocksTree)
	case DelInodeType:
		memModeTree = &DeletedInodeBTree{NewBtree()}
		rocksModeTree, _ = NewDeletedInodeRocks(rocksTree)
	default:
		fmt.Printf("error tree type(%v)\n", treeType)
		os.Exit(1)
	}
	return
}

func InitInodeTree(rocksTree *RocksTree) (memInodeTree, rocksInodeTree InodeTree) {
	if rocksTree == nil {
		fmt.Printf("rocksTree is nil\n")
		os.Exit(1)
	}
	memItem, rocksItem := mockTree(rocksTree, InodeType)
	memInodeTree = memItem.(InodeTree)
	rocksInodeTree = rocksItem.(InodeTree)
	return
}

func InintDentryTree(rocksTree *RocksTree) (memDentryTree, rocksDentryTree DentryTree) {
	if rocksTree == nil {
		fmt.Printf("rocksTree is nil\n")
		os.Exit(1)
	}
	memItem, rocksItem := mockTree(rocksTree, DentryType)
	memDentryTree = memItem.(DentryTree)
	rocksDentryTree = rocksItem.(DentryTree)
	return
}

func InitDeletedDentryTree(rocksTree *RocksTree) (memDeletedDentryTree, rocksDeletedDentryTree DeletedDentryTree) {
	if rocksTree == nil {
		fmt.Printf("rocksTree(%v) is nil", rocksTree)
		os.Exit(1)
	}
	memItem, rocksItem := mockTree(rocksTree, DelDentryType)
	memDeletedDentryTree = memItem.(DeletedDentryTree)
	rocksDeletedDentryTree = rocksItem.(DeletedDentryTree)
	return
}

func TestInodeTreeCreate(t *testing.T) {
	//check
	var errForMem, errForRocks error

	rocksDir := "./test_inode_tree_create"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	inode := NewInode(1000, 0)
	errForMem = memInodeTree.Create(inode, true)
	errForRocks = rocksInodeTree.Create(inode, true)
	if errForMem != errForRocks || errForMem != nil {
		t.Fatalf("Test Create: error different or mismatch, expect:nil, actual[errorInMem:%v errorInRocks:%v]", errForMem, errForRocks)
	}

	errForMem = memInodeTree.Create(inode, false)
	errForRocks = rocksInodeTree.Create(inode, false)
	if errForMem != errForRocks || errForMem != existsError {
		t.Fatalf("Test Create: error different or error mismatch, expect:existError, actual[errorInMem:%v errorInRocksd:%v]", errForMem, errForRocks)
	}

	errForMem = memInodeTree.Create(inode, true)
	errForRocks = rocksInodeTree.Create(inode, true)
	if errForMem != errForRocks || errForMem != nil {
		t.Fatalf("Test Create: error different or mismatch, expect:mil, actual[errorInMem:%v errorInRocks:%v]", errForMem, errForRocks)
	}

	if memInodeTree.Count() != rocksInodeTree.Count() || memInodeTree.Count() != 1 {
		t.Fatalf("Test Create: inode count different or mismatch, expect:1, actual:[mem:%v, rocks:%v]", memInodeTree.Count(), rocksInodeTree.Count())
	}

	memInodeTree.Delete(inode.Inode)
	rocksInodeTree.Delete(inode.Inode)
	if memInodeTree.Count() != rocksInodeTree.Count() || memInodeTree.Count() != 0 {
		t.Fatalf("Test Create: inode count different or mismatch, expect:0, actual:[mem:%v, rocks:%v]", memInodeTree.Count(), rocksInodeTree.Count())
	}
}

func TestInodeTreeGet(t *testing.T) {
	var (
		errForMem   error
		errForRocks error
	)

	rocksDir := "./test_inode_tree_get"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	for index := 1; index <= 100; index++ {
		inode := NewInode(uint64(index), 0)
		_ = memInodeTree.Create(inode, true)
		_ = rocksInodeTree.Create(inode, true)
	}
	defer func() {
		for index := 1; index <= 100; index++ {
			_, _ = memInodeTree.Delete(uint64(index))
			_, _ = rocksInodeTree.Delete(uint64(index))
		}
	}()

	if memInodeTree.Count() != rocksInodeTree.Count() || memInodeTree.Count() != 100 {
		t.Fatalf("Test Get: inode count mismatch, expect:100, actual:%v", memInodeTree.Count())
	}

	if rocksInodeTree.RealCount() != memInodeTree.RealCount() || rocksInodeTree.RealCount() != 100 {
		t.Fatalf("Test Get: inode count mismatch, expect:100, actual:%v", rocksInodeTree.RealCount())
	}

	var getIno *Inode
	getIno, errForMem = memInodeTree.Get(30)
	if errForMem != nil || getIno == nil {
		t.Fatalf("Test Get: result mismatch, expect[err:nil, inode:not nil] actual[err:%v, inode:%v]", errForMem, getIno)
	}

	if getIno.Inode != 30 {
		t.Fatalf("Test Get: inode number mismatch, expect:30, actual:%v", getIno.Inode)
	}

	getIno, errForRocks = rocksInodeTree.Get(30)
	if errForRocks != nil || getIno == nil {
		t.Fatalf("Test Get: result mismatch, expect[err:nil, inode:30] actual[err:%v, inode:%v]", errForRocks, getIno)
	}

	if getIno.Inode != 30 {
		t.Fatalf("Test Get: inode number mismatch, expect:30, actual:%v", getIno.Inode)
	}

	//get not exist inode
	getIno, errForMem = memInodeTree.Get(1000)
	if errForMem != nil {
		t.Fatalf("Test Get: result mismatch, expect[err:nil] actual[err:%v]", errForMem)
	}

	if getIno != nil {
		t.Fatalf("Test Get: result mismatch, inode get expect:nil, actual:%v", getIno)
	}

	getIno, errForRocks = rocksInodeTree.Get(1000)
	if errForRocks != nil && errForRocks != rocksdbError {
		t.Fatalf("Test Get: result mismatch, expect[err:nil] actual[err:%v]", errForRocks)
	}

	if getIno != nil {
		t.Fatalf("Test Get: result mismatch, inode get expect:nil, actual:%v", getIno)
	}
	return
}

func TestInodeTreeGetMaxInode(t *testing.T) {
	var (
		errForRocks error
		inodeCount  = 1000
	)
	rocksDir := "./test_inode_tree_getMaxInode"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	for index := 1; index <= inodeCount; index++ {
		inode := NewInode(uint64(index), 0)
		_ = memInodeTree.Create(inode, true)
		_ = rocksInodeTree.Create(inode, true)
	}
	defer func() {
		_ = memInodeTree.Clear()
		_ = rocksInodeTree.Clear()
	}()

	maxIno, _ := memInodeTree.GetMaxInode()
	if maxIno != uint64(inodeCount) {
		t.Fatalf("Test GetMaxInode: result mismatch, expect:10000, actual:%v", maxIno)
	}

	maxIno, errForRocks = rocksInodeTree.GetMaxInode()
	if errForRocks != nil {
		t.Fatalf("Test GetMaxInode: get error:%v", errForRocks)
	}
	if maxIno != uint64(inodeCount) {
		t.Fatalf("Test GetMaxInode: result mismatch, expect:10000, actual:%v", maxIno)
	}
	return
}

func TestInodeTreeClear(t *testing.T) {
	var (
		errForRocks error
		inodeCount  = 12345
	)
	rocksDir := "./test_inode_tree_clear"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	for index := 1; index <= inodeCount; index++ {
		inode := NewInode(uint64(index), 0)
		_ = memInodeTree.Create(inode, true)
		_ = rocksInodeTree.Create(inode, true)
	}
	defer func() {
		for index := 1; index <= inodeCount; index++ {
			_, _ = memInodeTree.Delete(uint64(index))
			_, _ = rocksInodeTree.Delete(uint64(index))
		}
	}()
	if memInodeTree.Count() != rocksInodeTree.Count() || memInodeTree.Count() != uint64(inodeCount) {
		t.Fatalf("Test Clear: inode count mismatch, expect:%v actual[mem:%v, rocks:%v]", inodeCount, memInodeTree.Count(), rocksInodeTree.Count())
	}

	if memInodeTree.RealCount() != rocksInodeTree.RealCount() || memInodeTree.RealCount() != uint64(inodeCount) {
		t.Fatalf("Test Clear: inode real count mismatch, expect:%v actual[mem:%v, rocks:%v]", inodeCount, memInodeTree.Count(), rocksInodeTree.Count())
	}
	_ = memInodeTree.Clear()
	errForRocks = rocksInodeTree.Clear()
	if errForRocks != nil {
		t.Fatalf("Test Clear: get error:%v", errForRocks)
	}

	if memInodeTree.Count() != 0 || rocksInodeTree.Count() != 0 {
		t.Fatalf("Test Clear: inode count mismatch, expect:0 actual[mem:%v, rocks:%v]", memInodeTree.Count(), rocksInodeTree.Count())
	}
	if memInodeTree.RealCount() != 0 || rocksInodeTree.RealCount() != 0 {
		t.Fatalf("Test Clear: inode real count mismatch, expect:0 actual[mem:%v, rocks:%v]", memInodeTree.Count(), rocksInodeTree.Count())
	}
	return
}

func TestInodeTreeRange(t *testing.T) {
	inodeCount := 123
	rocksDir := "./test_inode_tree_range"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	for index := 1; index <= inodeCount; index++ {
		inode := NewInode(uint64(index), 0)
		_ = memInodeTree.Create(inode, true)
		_ = rocksInodeTree.Create(inode, true)
	}
	defer func() {
		_ = memInodeTree.Clear()
		_ = rocksInodeTree.Clear()
	}()

	startInode := 43
	index := 0
	memInodeTree.Range(NewInode(43, 0), nil, func(data []byte) (bool, error) {
		inode := NewInode(0, 0)
		_ = inode.Unmarshal(context.Background(), data)
		if inode.Inode != uint64(startInode+index) {
			t.Fatalf("Test Range: inode mismatch, expect:%v, actual:%v", startInode+index, inode.Inode)
		}
		index++
		return true, nil
	})
	if index != 81 {
		t.Fatalf("Test Range: inode count mismatch, expect:81, actual:%v", index)
	}

	index = 0
	rocksInodeTree.Range(NewInode(43, 0), nil, func(data []byte) (bool, error) {
		inode := NewInode(0, 0)
		_ = inode.Unmarshal(context.Background(), data)
		if inode.Inode != uint64(startInode+index) {
			t.Fatalf("Test Range: inode mismatch, expect:%v, actual:%v", startInode+index, inode.Inode)
		}
		index++
		return true, nil
	})
	if index != 81 {
		t.Fatalf("Test Range: inode count mismatch, expect:81, actual:%v", index)
	}
}

func TestInodeTreeMaxItem(t *testing.T) {
	var (
		inodeCount = 1234
	)
	rocksDir := "./test_inode_tree_MaxItem"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memInodeTree, rocksInodeTree := InitInodeTree(rocksTree)
	//create
	for index := 1; index <= inodeCount; index++ {
		inode := NewInode(uint64(index), 0)
		_ = memInodeTree.Create(inode, true)
		_ = rocksInodeTree.Create(inode, true)
	}

	maxInode := memInodeTree.MaxItem()
	if maxInode == nil || maxInode.Inode != uint64(inodeCount) {
		t.Fatalf("Test GetMaxInode: mem mode result mismatch, expect MaxInodeId:%v, actual MaxInode:%v", inodeCount, maxInode)
	}

	maxInode = rocksInodeTree.MaxItem()
	if maxInode == nil || maxInode.Inode != uint64(inodeCount) {
		t.Fatalf("Test GetMaxInode: rocks mode result mismatch, expect MaxInodeId:%v, actual MaxInode:%v", inodeCount, maxInode)
	}
	return
}

func TestDeletedDentryRange(t *testing.T) {
	rocksDir := "./test_deleted_dentry_tree_range"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memDelDentryTree, rocksDelDentryTree := InitDeletedDentryTree(rocksTree)

	curTimestamp := time.Now().Unix()
	delDentry := newPrimaryDeletedDentry(1, "test_01", curTimestamp, 1000)
	_ = memDelDentryTree.Create(delDentry, true)
	_ = rocksDelDentryTree.Create(delDentry, true)

	delDentry = newPrimaryDeletedDentry(1, "a", curTimestamp+100, 1000)
	_ = memDelDentryTree.Create(delDentry, true)
	_ = rocksDelDentryTree.Create(delDentry, true)

	delDentry = newPrimaryDeletedDentry(1, "test_01", curTimestamp+1000, 1004)
	_ = memDelDentryTree.Create(delDentry, true)
	_ = rocksDelDentryTree.Create(delDentry, true)

	delDentry = newPrimaryDeletedDentry(2, "test_02", curTimestamp, 2000)
	_ = memDelDentryTree.Create(delDentry, true)
	_ = rocksDelDentryTree.Create(delDentry, true)

	delDentry = newPrimaryDeletedDentry(3, "test_03", curTimestamp, 3000)
	_ = memDelDentryTree.Create(delDentry, true)
	_ = rocksDelDentryTree.Create(delDentry, true)

	defer func() {
		_ = memDelDentryTree.Clear()
		_ = rocksDelDentryTree.Clear()
	}()

	fmt.Printf("start run testRange01\n")
	expectResult := []*DeletedDentry{
		{Dentry{ParentId: 1, Name: "a", Inode: 1000}, curTimestamp + 100, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1000}, curTimestamp, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1004}, curTimestamp + 1000, ""},
		{Dentry{ParentId: 2, Name: "test_02", Inode: 2000}, curTimestamp, ""},
		{Dentry{ParentId: 3, Name: "test_03", Inode: 3000}, curTimestamp, ""},
	}
	actualResult := make([]*DeletedDentry, 0, 5)
	memDelDentryTree.Range(nil, nil, func(data []byte) (bool, error) {
		delDen := new(DeletedDentry)
		if err := delDen.Unmarshal(data); err != nil {
			t.Errorf("unmarshal failed:%v", err)
			return false, err
		}
		actualResult = append(actualResult, delDen)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*DeletedDentry, 0, 5)
	rocksDelDentryTree.Range(nil, nil, func(data []byte) (bool, error) {
		delDen := new(DeletedDentry)
		if err := delDen.Unmarshal(data); err != nil {
			t.Errorf("unmarshal failed:%v", err)
			return false, err
		}
		actualResult = append(actualResult, delDen)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	fmt.Printf("start run testRange02\n")
	expectResult = []*DeletedDentry{
		{Dentry{ParentId: 1, Name: "a", Inode: 1000}, curTimestamp + 100, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1000}, curTimestamp, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1004}, curTimestamp + 1000, ""},
	}
	actualResult = make([]*DeletedDentry, 0, 5)
	memDelDentryTree.Range(newPrimaryDeletedDentry(1, "", 0, 0), newPrimaryDeletedDentry(2, "", 0, 0), func(data []byte) (bool, error) {
		delDen := new(DeletedDentry)
		if err := delDen.Unmarshal(data); err != nil {
			t.Errorf("unmarshal failed:%v", err)
			return false, err
		}
		actualResult = append(actualResult, delDen)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*DeletedDentry, 0, 5)
	rocksDelDentryTree.Range(newPrimaryDeletedDentry(1, "", 0, 0), newPrimaryDeletedDentry(2, "", 0, 0), func(data []byte) (bool, error) {
		delDen := new(DeletedDentry)
		if err := delDen.Unmarshal(data); err != nil {
			t.Errorf("unmarshal failed:%v", err)
			return false, err
		}
		actualResult = append(actualResult, delDen)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	fmt.Printf("start run testRange03\n")
	expectResult = []*DeletedDentry{
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1000}, curTimestamp, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1004}, curTimestamp + 1000, ""},
	}
	actualResult = make([]*DeletedDentry, 0, 5)
	memDelDentryTree.Range(newPrimaryDeletedDentry(1, "test_01", curTimestamp, 0),
		newPrimaryDeletedDentry(1, "test_01", curTimestamp+2000, 0), func(data []byte) (bool, error) {
			delDen := new(DeletedDentry)
			if err := delDen.Unmarshal(data); err != nil {
				t.Errorf("unmarshal failed:%v", err)
				return false, err
			}
			actualResult = append(actualResult, delDen)
			return true, nil
		})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*DeletedDentry, 0, 5)
	rocksDelDentryTree.Range(newPrimaryDeletedDentry(1, "test_01", curTimestamp, 0),
		newPrimaryDeletedDentry(1, "test_01", curTimestamp+2000, 0), func(data []byte) (bool, error) {
			delDen := new(DeletedDentry)
			if err := delDen.Unmarshal(data); err != nil {
				t.Errorf("unmarshal failed:%v", err)
				return false, err
			}
			actualResult = append(actualResult, delDen)
			return true, nil
		})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	fmt.Printf("start run testRange04\n")
	expectResult = []*DeletedDentry{
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1000}, curTimestamp, ""},
		{Dentry{ParentId: 1, Name: "test_01", Inode: 1004}, curTimestamp + 1000, ""},
	}
	actualResult = make([]*DeletedDentry, 0, 5)
	memDelDentryTree.Range(newPrimaryDeletedDentry(1, "test_01", curTimestamp, 0),
		newPrimaryDeletedDentry(2, "", 0, 0), func(data []byte) (bool, error) {
			delDen := new(DeletedDentry)
			if err := delDen.Unmarshal(data); err != nil {
				t.Errorf("unmarshal failed:%v", err)
				return false, err
			}
			actualResult = append(actualResult, delDen)
			return true, nil
		})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*DeletedDentry, 0, 5)
	rocksDelDentryTree.Range(newPrimaryDeletedDentry(1, "test_01", curTimestamp, 0),
		newPrimaryDeletedDentry(2, "", 0, 0), func(data []byte) (bool, error) {
			delDen := new(DeletedDentry)
			if err := delDen.Unmarshal(data); err != nil {
				t.Errorf("unmarshal failed:%v", err)
				return false, err
			}
			actualResult = append(actualResult, delDen)
			return true, nil
		})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%v, actual：%v", len(expectResult), len(actualResult))
	}
	for index, result := range expectResult {
		if !reflect.DeepEqual(result, actualResult[index]) {
			t.Fatalf("Test DeletedDentry Range: delDentry count mismatch, expect:%s, actual：%s", result.String(), actualResult[index].String())
		}
	}
}

func TestDentryTreeRange(t *testing.T) {
	rocksDir := "./test_dentry_tree_range"
	rocksTree := newTestRocksTree(rocksDir)
	defer func() {
		rocksTree.Release()
		_ = os.RemoveAll(rocksDir)
	}()
	memDentryTree, rocksDentryTree := InintDentryTree(rocksTree)
	dentry := &Dentry{ParentId: 1, Name: "test_01", Inode: 1001, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 1, Name: "test_02", Inode: 1002, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 1, Name: "test_03", Inode: 1003, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 1, Name: "test_04", Inode: 1004, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 1, Name: "abc", Inode: 1005, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 1, Name: "def", Inode: 1006, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	dentry = &Dentry{ParentId: 2, Name: "hig", Inode: 1007, Type: 0}
	_ = memDentryTree.Create(dentry, true)
	_ = rocksDentryTree.Create(dentry, true)

	expectResult := []*Dentry{
		{ParentId: 1, Name: "abc", Inode: 1005, Type: 0},
		{ParentId: 1, Name: "def", Inode: 1006, Type: 0},
		{ParentId: 1, Name: "test_01", Inode: 1001, Type: 0},
		{ParentId: 1, Name: "test_02", Inode: 1002, Type: 0},
		{ParentId: 1, Name: "test_03", Inode: 1003, Type: 0},
		{ParentId: 1, Name: "test_04", Inode: 1004, Type: 0},
	}
	actualResult := make([]*Dentry, 0, 6)
	memDentryTree.Range(&Dentry{ParentId: 1}, &Dentry{ParentId: 2}, func(data []byte) (bool, error) {
		den := &Dentry{}
		_ = den.Unmarshal(data)
		actualResult = append(actualResult, den)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test Dentry Range in mem: result cnt mismatch, expect:%v, actual:%v", len(expectResult), len(actualResult))
	}
	for index, den := range expectResult {
		if !reflect.DeepEqual(den, actualResult[index]) {
			t.Fatalf("Test Dentry Range in mem: result mismatch, expect:%s, actual:%s", den.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*Dentry, 0, 6)
	rocksDentryTree.Range(&Dentry{ParentId: 1}, &Dentry{ParentId: 2}, func(data []byte) (bool, error) {
		den := &Dentry{}
		_ = den.Unmarshal(data)
		actualResult = append(actualResult, den)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test Dentry Range in rocks: result cnt mismatch, expect:%v, actual:%v", len(expectResult), len(actualResult))
	}
	for index, den := range expectResult {
		if !reflect.DeepEqual(den, actualResult[index]) {
			t.Fatalf("Test Dentry Range in rocksdb: result mismatch, expect:%s, actual:%s", den.String(), actualResult[index].String())
		}
	}

	expectResult = []*Dentry{
		{ParentId: 1, Name: "test_01", Inode: 1001, Type: 0},
		{ParentId: 1, Name: "test_02", Inode: 1002, Type: 0},
		{ParentId: 1, Name: "test_03", Inode: 1003, Type: 0},
		{ParentId: 1, Name: "test_04", Inode: 1004, Type: 0},
	}
	actualResult = make([]*Dentry, 0, 4)
	memDentryTree.Range(&Dentry{ParentId: 1, Name: "test"}, &Dentry{ParentId: 2}, func(data []byte) (bool, error) {
		den := &Dentry{}
		_ = den.Unmarshal(data)
		actualResult = append(actualResult, den)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test Dentry Range in mem: result cnt mismatch, expect:%v, actual:%v", len(expectResult), len(actualResult))
	}
	for index, den := range expectResult {
		if !reflect.DeepEqual(den, actualResult[index]) {
			t.Fatalf("Test Dentry Range in mem: result mismatch, expect:%s, actual:%s", den.String(), actualResult[index].String())
		}
	}

	actualResult = make([]*Dentry, 0, 6)
	rocksDentryTree.Range(&Dentry{ParentId: 1, Name: "test"}, &Dentry{ParentId: 2}, func(data []byte) (bool, error) {
		den := &Dentry{}
		_ = den.Unmarshal(data)
		actualResult = append(actualResult, den)
		return true, nil
	})
	if len(expectResult) != len(actualResult) {
		t.Fatalf("Test Dentry Range in rocks: result cnt mismatch, expect:%v, actual:%v", len(expectResult), len(actualResult))
	}
	for index, den := range expectResult {
		if !reflect.DeepEqual(den, actualResult[index]) {
			t.Fatalf("Test Dentry Range in rocksdb: result mismatch, expect:%s, actual:%s", den.String(), actualResult[index].String())
		}
	}
}
