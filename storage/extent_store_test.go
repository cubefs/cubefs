package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/async"
	"github.com/chubaofs/chubaofs/util/testutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func createFile(name string, data []byte) (err error) {
	os.RemoveAll(name)
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return
	}
	fp.Write(data)
	fp.Close()
	return
}

func removeFile(name string) {
	os.RemoveAll(name)
}

func computeMd5(data []byte) string {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func compareMd5(s *ExtentStore, extent, offset, size uint64, expectMD5 string) (err error) {
	actualMD5, err := s.ComputeMd5Sum(extent, offset, size)
	if err != nil {
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v),"+
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)", extent, offset, size, expectMD5, actualMD5, err)
	}
	if actualMD5 != expectMD5 {
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v),"+
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)", extent, offset, size, expectMD5, actualMD5, err)
	}
	return nil
}

func TestExtentStore_ComputeMd5Sum(t *testing.T) {
	allData := []byte(RandStringRunes(100 * 1024))
	allMd5 := computeMd5(allData)
	dataPath := "/tmp"
	extentID := 3675
	err := createFile(path.Join(dataPath, strconv.Itoa(extentID)), allData)
	if err != nil {
		t.Logf("createFile failed %v", err)
		t.FailNow()
	}
	s := new(ExtentStore)
	s.dataPath = dataPath
	err = compareMd5(s, uint64(extentID), 0, 0, allMd5)
	if err != nil {
		t.Logf("compareMd5 failed %v", err)
		t.FailNow()
	}

	for i := 0; i < 100; i++ {
		data := allData[i*1024 : (i+1)*1024]
		expectMD5 := computeMd5(data)
		err = compareMd5(s, uint64(extentID), uint64(i*1024), 1024, expectMD5)
		if err != nil {
			t.Logf("compareMd5 failed %v", err)
			t.FailNow()
		}
	}
	removeFile(path.Join(dataPath, strconv.Itoa(extentID)))

}

func TestExtentStore_PlaybackTinyDelete(t *testing.T) {
	const (
		testStoreSize          int    = 128849018880 // 120GB
		testStoreCacheCapacity int    = 1
		testPartitionID        uint64 = 1
		testTinyExtentID       uint64 = 1
		testTinyFileCount      int    = 1024
		testTinyFileSize       int    = PageSize
	)
	var baseTestPath = testutil.InitTempTestPath(t)
	t.Log(baseTestPath.Path())
	defer func() {
		baseTestPath.Cleanup()
	}()

	var (
		err error
	)

	var store *ExtentStore
	if store, err = NewExtentStore(baseTestPath.Path(), testPartitionID, testStoreSize, testStoreCacheCapacity, nil, false); err != nil {
		t.Fatalf("init test store failed: %v", err)
	}
	// 准备小文件数据，向TinyExtent 1写入1024个小文件数据, 每个小文件size为1024.
	var (
		tinyFileData    = make([]byte, testTinyFileSize)
		testFileDataCrc = crc32.ChecksumIEEE(tinyFileData[:testTinyFileSize])
		holes           = make([]*proto.TinyExtentHole, 0)
	)
	for i := 0; i < testTinyFileCount; i++ {
		off := int64(i * PageSize)
		size := int64(testTinyFileSize)
		if err = store.Write(context.Background(), testTinyExtentID, off, size,
			tinyFileData[:testTinyFileSize], testFileDataCrc, AppendWriteType, false); err != nil {
			t.Fatalf("prepare tiny data [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		if i%2 == 0 {
			continue
		}
		if err = store.RecordTinyDelete(testTinyExtentID, off, size); err != nil {
			t.Fatalf("write tiny delete record [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		holes = append(holes, &proto.TinyExtentHole{
			Offset: uint64(off),
			Size:   uint64(size),
		})
	}
	// 执行TinyDelete回放
	if err = store.PlaybackTinyDelete(); err != nil {
		t.Fatalf("playback tiny delete failed: %v", err)
	}
	// 验证回放后测试TinyExtent的洞是否可预期一致
	var (
		testTinyExtent *Extent
		newOffset      int64
	)
	if testTinyExtent, err = store.extentWithHeaderByExtentID(testTinyExtentID); err != nil {
		t.Fatalf("load test tiny extent %v failed: %v", testTinyExtentID, err)
	}
	for _, hole := range holes {
		newOffset, _, err = testTinyExtent.tinyExtentAvaliOffset(int64(hole.Offset))
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			newOffset = testTinyExtent.dataSize
			err = nil
		}
		if err != nil {
			t.Fatalf("check tiny extent avali offset failed: %v", err)
		}
		if hole.Offset+hole.Size != uint64(newOffset) {
			t.Fatalf("punch hole record [offset: %v, size: %v] not applied to extent.", hole.Offset, hole.Size)
		}
	}
}

func TestExtentStore_UsageOnConcurrentModification(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()

	const (
		partitionID       uint64 = 1
		storageSize              = 1 * 1024 * 1024
		cacheCapacity            = 10
		writeWorkers             = 10
		dataSizePreWrite         = 16
		executionDuration        = 30 * time.Second
	)

	var testPartitionPath = path.Join(testPath.Path(), fmt.Sprintf("datapartition_%d", partitionID))
	var storage *ExtentStore
	var err error
	if storage, err = NewExtentStore(testPartitionPath, partitionID, storageSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, true); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}

	storage.AsyncLoadExtentSize()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}

	var futures = make(map[string]*async.Future, 0)
	var dataSize = int64(dataSizePreWrite)
	var data = make([]byte, dataSize)
	var dataCRC = crc32.ChecksumIEEE(data)
	var ctx, cancel = context.WithCancel(context.Background())
	// Start workers to write extents
	for i := 0; i < writeWorkers; i++ {
		var future = async.NewFuture()
		go func(future *async.Future, ctx context.Context, cancelFunc context.CancelFunc) {
			var err error
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var extentID uint64
				if extentID, err = storage.NextExtentID(); err != nil {
					return
				}
				if err = storage.Create(extentID, true); err != nil {
					return
				}
				var offset int64 = 0
				for offset+int64(dataSize) <= 64*1024*1024 {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err = storage.Write(context.Background(), extentID, offset, dataSize, data, dataCRC, AppendWriteType, false); err != nil {
						err = nil
						break
					}
					offset += int64(dataSize)
				}
			}
		}(future, ctx, cancel)
		futures[fmt.Sprintf("WriteWorker-%d", i)] = future
	}

	// Start extents delete worker
	{
		var future = async.NewFuture()
		go func(future *async.Future, ctx context.Context, cancelFunc context.CancelFunc) {
			var err error
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			var batchDeleteTicker = time.NewTicker(1 * time.Second)
			for {
				select {
				case <-batchDeleteTicker.C:
				case <-ctx.Done():
					return
				}
				var eibs []ExtentInfoBlock
				if eibs, err = storage.GetAllWatermarks(proto.NormalExtentType, nil); err != nil {
					return
				}
				if _, err = storage.GetAllExtentInfoWithByteArr(ExtentFilterForValidateCRC()); err != nil {
					return
				}
				for _, eib := range eibs {
					_ = storage.MarkDelete(eib[FileID], 0, 0)
				}
			}
		}(future, ctx, cancel)
		futures["DeleteWorker"] = future
	}

	// Start control worker
	{
		var future = async.NewFuture()
		go func(future *async.Future, ctx context.Context, cancelFunc context.CancelFunc) {
			defer func() {
				future.Respond(nil, nil)
				var startTime = time.Now()
				var stopTimer = time.NewTimer(executionDuration)
				var displayTicker = time.NewTicker(time.Second * 10)
				for {
					select {
					case <-stopTimer.C:
						t.Logf("Execution finish.")
						cancelFunc()
						return
					case <-displayTicker.C:
						t.Logf("Execution time: %.0fs.", time.Now().Sub(startTime).Seconds())
					case <-ctx.Done():
						t.Logf("Execution aborted.")
						return
					}
				}

			}()
		}(future, ctx, cancel)
		futures["ControlWorker"] = future
	}

	for name, future := range futures {
		if _, err = future.Response(); err != nil {
			t.Fatalf("%v respond error: %v", name, err)
		}
	}

	// 结果检查
	{
		// 计算本地文件系统中实际Size总和
		var actualNormalExtentTotalUsed int64
		var files []os.FileInfo
		var extentFileRegexp = regexp.MustCompile("^(\\d)+$")
		if files, err = ioutil.ReadDir(testPartitionPath); err != nil {
			t.Fatalf("Stat test partition path %v failed, error message: %v", testPartitionPath, err)
		}
		for _, file := range files {
			if extentFileRegexp.Match([]byte(file.Name())) {
				actualNormalExtentTotalUsed += file.Size()
			}
		}

		var eibs []ExtentInfoBlock
		if eibs, err = storage.GetAllWatermarks(proto.AllExtentType, nil); err != nil {
			t.Fatalf("Get extent info from storage failed, error message: %v", err)
		}
		var eibTotalUsed int64
		for _, eib := range eibs {
			eibTotalUsed += int64(eib[Size])
		}

		var storeUsedSize = storage.GetStoreUsedSize() // 存储引擎统计的使用Size总和

		if !((actualNormalExtentTotalUsed == eibTotalUsed) && (eibTotalUsed == storeUsedSize)) {
			t.Fatalf("Used size validation failed, actual total used %v, store used size %v, extent info total used %v", actualNormalExtentTotalUsed, storeUsedSize, eibTotalUsed)
		}
	}

	storage.Close()
}
