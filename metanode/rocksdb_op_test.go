package metanode

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"
)

const (
	MultiCountPerOp = 1000
	TestCont 		= 1000000
)

func TestOpenDb(t *testing.T) {
	db := NewRocksDb()
	os.RemoveAll("./db")
	if err := db.OpenDb("./db"); err != nil {
		t.Errorf("open db without exist dir failed")
	}

	db.CloseDb()
	db.ReleaseRocksDb()
	_, err := os.Stat("./db")
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("os error, rm dir[db] failed")
		return
	}

	os.MkdirAll("./db", 0755)
	_, err = os.Stat("./db")
	if err != nil && !os.IsExist(err) {
		t.Errorf("os error, mk dir[db] failed:%v", err)
		return
	}

	if err = db.OpenDb("./db"); err != nil {
		t.Errorf("open db without exist dir failed")
	}

	db.CloseDb()
	db.ReleaseRocksDb()
}


func TestReopneDb(t *testing.T) {
	db := NewRocksDb()
	os.RemoveAll("./db")
	if err := db.OpenDb("./db"); err != nil {
		t.Errorf("open db without exist dir failed")
	}

	db.CloseDb()
	db.ReleaseRocksDb()
	_, err := os.Stat("./db")
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("os error, rm dir[db] failed")
		return
	}

	os.MkdirAll("./db", 0755)
	_, err = os.Stat("./db")
	if err != nil && !os.IsExist(err) {
		t.Errorf("os error, mk dir[db] failed:%v", err)
		return
	}

	if err = db.ReOpenDb("./db"); err != nil {
		t.Errorf("open db without exist dir failed")
	}

	db.CloseDb()
	db.ReleaseRocksDb()
}

func insertByItem(t *testing.T, db *RocksDbInfo) {
	i 	  := 0
	key   := make([]byte, dbExtentKeySize)
	value := make([]byte, 1)

	updateKeyToNow(key)

	for ; i < TestCont; i++ {
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(i))
		copy(key[8:], id)
		if err := db.Put(key, value); err != nil {
			t.Errorf("insert item failed:%s", err.Error())
			return
		}
	}

	return
}

func insertByMultiItem(t *testing.T, db *RocksDbInfo) {
	i 	  := 0
	key   := make([]byte, dbExtentKeySize)
	value := make([]byte, 1)
	keys  := make([]byte, dbExtentKeySize * MultiCountPerOp)
	handle, err := db.CreateBatchHandler()

	if err != nil {
		t.Errorf("insert multi failed:%s", err.Error())
	}

	updateKeyToNow(key)
	key[0] = byte(ExtentDelTable)

	for ; i < TestCont; i++ {
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(i))
		copy(key[8:], id)

		if i != 0 && i % MultiCountPerOp == 0 {
			if err = db.CommitBatchAndRelease(handle); err != nil {
				t.Errorf("insert multi failed:commit failed:%s", err.Error())
				return
			}
			if handle, err = db.CreateBatchHandler(); err != nil {
				t.Errorf("insert multi failed:create batch handle failed:%s", err.Error())
				return
			}
		}
		offset := (i % MultiCountPerOp) * dbExtentKeySize
		copy(keys[offset : offset + dbExtentKeySize], key)
		db.AddItemToBatch(handle, keys[offset : offset + dbExtentKeySize], value)
	}

	if err != nil{
		db.ReleaseBatchHandle(handle)
		return
	}

	db.CommitBatchAndRelease(handle)

	return
}

func rangeTest(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(0)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	db.Range(stKey, endKey, func(k, v[]byte)(bool, error){
		if k[0] != byte(ExtentDelTable) {
			t.Errorf("get out of range, cur key:%v\n",k)
			return false, fmt.Errorf("get out of range, cur key:%v\n",k)
		}
		index := binary.BigEndian.Uint64(k[8:16])
		if i != index {
			t.Errorf("range failed, want:%d, now:%d, key:%v", i, index, k)
			return false, fmt.Errorf("store data err")
		}
		i++
		return true, nil
	})

	if i > TestCont {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, i)
	}
}

func snapRangeTest(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(0)
	snap := db.OpenSnap()
	start := time.Now()
	defer db.ReleaseSnap(snap)


	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	db.RangeWithSnap(stKey, endKey, snap, func(k, v[]byte)(bool, error){
		if k[0] != byte(ExtentDelTable) {
			t.Errorf("get out of range, cur key:%v\n",k)
			return false, fmt.Errorf("get out of range, cur key:%v\n",k)
		}
		index := binary.BigEndian.Uint64(k[8:16])
		if i != index {
			t.Errorf("range failed, want:%d, now:%d, key:%v", i, index, k)
			return false, fmt.Errorf("store data err")
		}
		i++
		return true, nil
	})

	if i > TestCont {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, i)
	}
	t.Logf("snap range %v items cost:%v", i, time.Since(start))
}

func descRangeTest(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(TestCont - 1)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	db.DescRange(stKey, endKey, func(k, v[]byte)(bool, error){
		if k[0] != byte(ExtentDelTable) {
			t.Errorf("get out of range, cur key:%v\n",k)
			return false, fmt.Errorf("get out of range, cur key:%v\n",k)
		}
		index := binary.BigEndian.Uint64(k[8:16])
		if i != index {
			t.Errorf("range failed, want:%d, now:%d, key:%v", i, index, k)
			return false, fmt.Errorf("store data err")
		}
		i--
		return true, nil
	})

	if i != math.MaxUint64 {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, TestCont + (math.MaxUint64 - i))
	}
}

func snapDescRangeTest(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)
	i := uint64(TestCont - 1)
	snap := db.OpenSnap()
	defer db.ReleaseSnap(snap)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)

	db.DescRangeWithSnap(stKey, endKey, snap, func(k, v[]byte)(bool, error){
		if k[0] != byte(ExtentDelTable) {
			t.Errorf("get out of range, cur key:%v\n",k)
			return false, fmt.Errorf("get out of range, cur key:%v\n",k)
		}
		index := binary.BigEndian.Uint64(k[8:16])
		if i != index {
			t.Errorf("range failed, want:%d, now:%d, key:%v", i, index, k)
			return false, fmt.Errorf("store data err")
		}
		i--
		return true, nil
	})

	if i != math.MaxUint64 {
		t.Errorf("range failed, total record:%d, but read:%d", TestCont, TestCont + (math.MaxUint64 - i))
	}
}

func genenerData(t *testing.T, db *RocksDbInfo) {
	start := time.Now()
	insertByMultiItem(t,db)
	t.Logf("insert by multi items cost:%v", time.Since(start))

	return
}

func deleteDataByItem(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	start := time.Now()
	var i = 0

	err := db.Range(stKey, endKey, func(k, v []byte)(bool, error) {

		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}
		i++
		db.Del(k)
		return true, nil
	})

	t.Logf("********range %v items delete used:%v , err:%v\n", i, time.Since(start), err)
	return
}

func deleteDataByMultiItems(t *testing.T, db *RocksDbInfo) {
	stKey  := make([]byte, 1)
	endKey := make([]byte, 1)

	keys := make([]byte, dbExtentKeySize * 1000)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	start := time.Now()
	var i = 0
	handle, _ := db.CreateBatchHandler()

	err := db.Range(stKey, endKey, func(k, v []byte)(bool, error) {
		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}
		index := i % 1000
		if i % 1000 == 0{
			if i != 0 {
				_ = db.CommitBatchAndRelease(handle)
				handle, _ = db.CreateBatchHandler()
			}
		}
		i++
		copy(keys[index * dbExtentKeySize : (index+1) *dbExtentKeySize], k)
		db.DelItemToBatch(handle, keys[index * dbExtentKeySize : (index+1) *dbExtentKeySize])
		return true, nil
	})

	if handle != nil {
		db.CommitBatchAndRelease(handle)
	}

	t.Logf("********delete %v items by multi used:%v , err:%v\n", i, time.Since(start), err)
	return
}



func TestOps(t *testing.T) {

	os.RemoveAll("db")
	db := NewRocksDb()
	_ = db.OpenDb("db")

	start := time.Now()
	insertByItem(t, db)
	t.Logf("insert by item cost:%v", time.Since(start))

	start = time.Now()
	rangeTest(t, db)
	t.Logf("range test by item cost:%v", time.Since(start))

	start = time.Now()
	rangeTest(t, db)
	t.Logf("range test by item cost:%v", time.Since(start))

	runtime.GC()
	time.Sleep(time.Second * 3)
	start = time.Now()
	descRangeTest(t, db)
	t.Logf("desc range test by item cost:%v", time.Since(start))
	deleteDataByMultiItems(t, db)
	deleteDataByItem(t, db)



	genenerData(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	t.Logf("delete multi items begin\n")
	start = time.Now()
	deleteDataByItem(t, db)
	t.Logf("del by item cost:%v", time.Since(start))

	deleteDataByItem(t, db)
	genenerData(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	t.Logf("delete multi items begin\n")
	start = time.Now()
	deleteDataByMultiItems(t, db)
	t.Logf("del by multi item cost:%v", time.Since(start))


	deleteDataByItem(t, db)
	runtime.GC()
	time.Sleep(time.Second * 3)
	db2 := NewRocksDb()
	_ = db2.OpenDb("db2")
	genenerData(t, db2)
	start = time.Now()

	db2.CloseDb()
	_ = db.CloseDb()
	db.ReleaseRocksDb()
	os.Rename("db2", "db")
	db.ReOpenDb("db")
	t.Logf("reopen db delete used:%v \n", time.Since(start))
	rangeTest(t, db)
	deleteDataByMultiItems(t, db)
	db.CloseDb()
	db.ReleaseRocksDb()

	return
}

func batchAbortTest(t *testing.T, db *RocksDbInfo) {
	i 	  := 0
	key   := make([]byte, dbExtentKeySize)
	value := make([]byte, 1)
	keys  := make([]byte, dbExtentKeySize * MultiCountPerOp)
	var (
		handle interface{}
		err error
	)

	handle = nil
	db.AddItemToBatch(handle, key, value)
	db.DelItemToBatch(handle, key)
	db.CommitBatchAndRelease(handle)
	db.ReleaseBatchHandle(handle)

	handle, err = db.CreateBatchHandler()

	if err != nil {
		t.Errorf("insert multi failed:%s", err.Error())
	}

	updateKeyToNow(key)
	key[0] = byte(ExtentDelTable)

	for i = TestCont ; i < TestCont + TestCont; i++ {
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(i))
		copy(key[8:], id)

		if i != 0 && i % MultiCountPerOp == 0 {
			if err = db.ReleaseBatchHandle(handle); err != nil {
				t.Errorf("insert multi failed:commit failed:%s", err.Error())
				return
			}
			if handle, err = db.CreateBatchHandler(); err != nil {
				t.Errorf("insert multi failed:create batch handle failed:%s", err.Error())
				return
			}
		}
		offset := (i % MultiCountPerOp) * dbExtentKeySize
		copy(keys[offset : offset + dbExtentKeySize], key)
		db.AddItemToBatch(handle, keys[offset : offset + dbExtentKeySize], value)
	}

	db.ReleaseBatchHandle(handle)
	t.Logf("********batchAbortTest  finished")
}

func TestAbortOps(t *testing.T) {
	t.Logf("************ abort test begin *************")
	os.RemoveAll("db")
	db := NewRocksDb()
	_ = db.OpenDb("db")
	genenerData(t, db)
	rangeTest(t, db)

	batchAbortTest(t, db)
	rangeTest(t, db)

	go snapRangeTest(t, db)
	go deleteDataByMultiItems(t, db)
	time.Sleep(time.Millisecond * 50)
	db.CloseDb()
	_ = db.OpenSnap()
	db.ReleaseRocksDb()
}
