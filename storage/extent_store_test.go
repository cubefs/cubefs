package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"hash/crc32"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
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

	var (
		err         error
		testBaseDir = path.Join(os.TempDir(), t.Name())
	)

	if err = os.MkdirAll(testBaseDir, os.ModePerm); err != nil {
		t.Fatalf("prepare test dir %v failed: %v", testBaseDir, err)
	}
	defer func() {
		// 结束清理测试数据
		_ = os.RemoveAll(testBaseDir)
	}()

	var store *ExtentStore
	if store, err = NewExtentStore(testBaseDir, testPartitionID, testStoreSize, testStoreCacheCapacity, nil, false); err != nil {
		t.Fatalf("init test store failed: %v", err)
	}
	// 准备小文件数据，向TinyExtent 1写入1024个小文件数据, 每个小文件size为1024.
	var (
		tinyFileData       = make([]byte, testTinyFileSize)
		testFileDataCrc    = crc32.ChecksumIEEE(tinyFileData[:testTinyFileSize])
		holes        = make([]*proto.TinyExtentHole, 0)
	)
	for i := 0; i < testTinyFileCount; i++ {
		off := int64(i * PageSize)
		size := int64(testTinyFileSize)
		if err = store.Write(context.Background(), testTinyExtentID, off, size,
			tinyFileData[:testTinyFileSize], testFileDataCrc, AppendWriteType, false); err != nil {
			t.Fatalf("prepare tiny data [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		if i % 2 == 0 {
			continue
		}
		if err = store.RecordTinyDelete(testTinyExtentID, off, size); err != nil {
			t.Fatalf("write tiny delete record [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		holes = append(holes, &proto.TinyExtentHole{
			Offset: uint64(off),
			Size: uint64(size),
		})
	}
	// 执行TinyDelete回放
	if err = store.PlaybackTinyDelete(); err != nil {
		t.Fatalf("playback tiny delete failed: %v", err)
	}
	// 验证回放后测试TinyExtent的洞是否可预期一致
	var (
		testTinyExtent *Extent
		newOffset int64
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
		if hole.Offset + hole.Size != uint64(newOffset) {
			t.Fatalf("punch hole record [offset: %v, size: %v] not applied to extent.", hole.Offset, hole.Size)
		}
	}
}
