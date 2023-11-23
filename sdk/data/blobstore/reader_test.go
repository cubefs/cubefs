// Copyright 2022 The CubeFS Authors.
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
// permissions and limitations under the License.

package blobstore

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"io"
	"math/rand"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/brahma-adshonor/gohook"
	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewReader(t *testing.T) {
	mockConfig := ClientConfig{
		VolName:         "cfs",
		VolType:         0,
		BlockSize:       0,
		Ino:             2,
		Bc:              nil,
		Mw:              nil,
		Ec:              nil,
		Ebsc:            nil,
		EnableBcache:    false,
		WConcurrency:    0,
		ReadConcurrency: 0,
		CacheAction:     0,
		FileCache:       false,
		FileSize:        0,
		CacheThreshold:  0,
	}
	ec := &stream.ExtentClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	mockConfig.Ec = ec

	reader := NewReader(mockConfig)
	assert.NotEmpty(t, reader, nil)
}

func TestBuildExtentKey(t *testing.T) {

	testCase := []struct {
		eks      []proto.ExtentKey
		expectEk proto.ExtentKey
	}{
		{nil, proto.ExtentKey{}},
		{[]proto.ExtentKey{
			{FileOffset: uint64(0), Size: uint32(100)},
			{FileOffset: uint64(100), Size: uint32(100)},
			{FileOffset: uint64(200), Size: uint32(100)},
			{FileOffset: uint64(300), Size: uint32(100)},
		}, proto.ExtentKey{FileOffset: uint64(100), Size: uint32(100)}},
		{[]proto.ExtentKey{
			{FileOffset: uint64(0), Size: uint32(1)},
			{FileOffset: uint64(1), Size: uint32(1)},
			{FileOffset: uint64(2), Size: uint32(1)},
			{FileOffset: uint64(3), Size: uint32(1)},
		}, proto.ExtentKey{}},
	}

	rs := &rwSlice{}
	rs.objExtentKey = proto.ObjExtentKey{FileOffset: 100, Size: 100}
	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.extentKeys = tc.eks
		reader.buildExtentKey(rs)
		assert.Equal(t, tc.expectEk, rs.extentKey)
	}
}

func TestFileSize(t *testing.T) {
	testCase := []struct {
		valid      bool
		objEks     []proto.ObjExtentKey
		expectSize uint64
		expectOk   bool
	}{
		{false, nil, 0, false},
		{true, nil, 0, true},
		{true, []proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}}, 200, true},
	}

	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.valid = tc.valid
		reader.objExtentKeys = tc.objEks
		gotSize, gotOk := reader.fileSize()
		assert.Equal(t, tc.expectSize, gotSize)
		assert.Equal(t, tc.expectOk, gotOk)
	}

	//// mock objExtentKey
	//objEks := make([]proto.ObjExtentKey, 0)
	//objEkLen := rand.Intn(20)
	//expectedFileSize := 0
	//for i := 0; i < objEkLen; i++ {
	//	size := rand.Intn(1000)
	//	objEks = append(objEks, proto.ObjExtentKey{Size: uint64(size), FileOffset: uint64(expectedFileSize)})
	//	expectedFileSize += size
	//}
	//
	//// mock reader
	//mockConfig := ClientConfig{
	//	VolName:         "cfs",
	//	VolType:         0,
	//	BlockSize:       0,
	//	Ino:             2,
	//	Bc:              nil,
	//	Mw:              nil,
	//	Ec:              nil,
	//	Bsc:            nil,
	//	EnableBcache:    false,
	//	WConcurrency:    0,
	//	ReadConcurrency: 0,
	//	CacheAction:     0,
	//	FileCache:       false,
	//	FileSize:        0,
	//	CacheThreshold:  0,
	//}
	//reader := NewReader(mockConfig)
	//reader.valid = true
	//reader.objExtentKeys = objEks
	//
	//got, ok := reader.fileSize()
	//assert.True(t, true, ok)
	//assert.Equal(t, expectedFileSize, int(got))
	//
	//ctx := context.Background()
	//reader.Close(ctx)
}

func TestRefreshEbsExtents(t *testing.T) {
	testCase := []struct {
		getObjFunc  func(*meta.MetaWrapper, uint64) (uint64, uint64, []proto.ExtentKey, []proto.ObjExtentKey, error)
		expectValid bool
	}{
		{MockGetObjExtentsTrue, true},
		{MockGetObjExtentsFalse, false},
	}

	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		mw := &meta.MetaWrapper{}
		err := gohook.HookMethod(mw, "GetObjExtents", tc.getObjFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		reader.mw = mw
		reader.refreshEbsExtents()
		assert.Equal(t, reader.valid, tc.expectValid)
	}
}

func TestPrepareEbsSlice(t *testing.T) {
	testCase := []struct {
		getObjFunc  func(*meta.MetaWrapper, uint64) (uint64, uint64, []proto.ExtentKey, []proto.ObjExtentKey, error)
		offset      int
		size        uint32
		expectError error
	}{
		{nil, -1, 100, syscall.EIO},
		{MockGetObjExtentsTrue, 0, 100, nil},
		{MockGetObjExtentsTrue, 501, 100, io.EOF},
		{MockGetObjExtentsTrue, 400, 101, nil},
		{MockGetObjExtentsFalse, 0, 100, syscall.EIO},
		{MockGetObjExtentsFalse, 501, 100, syscall.EIO},
		{MockGetObjExtentsFalse, 400, 101, syscall.EIO},
	}

	for _, tc := range testCase {
		mw := &meta.MetaWrapper{}
		err := gohook.HookMethod(mw, "GetObjExtents", tc.getObjFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.mw = mw
		_, got := reader.prepareEbsSlice(tc.offset, tc.size)
		assert.Equal(t, tc.expectError, got)
	}
}

func TestRead(t *testing.T) {
	testCase := []struct {
		close            bool
		readConcurrency  int
		getObjFunc       func(*meta.MetaWrapper, uint64) (uint64, uint64, []proto.ExtentKey, []proto.ObjExtentKey, error)
		bcacheGetFunc    func(*bcache.BcacheClient, string, []byte, uint64, uint32) (int, error)
		checkDpExistFunc func(*stream.ExtentClient, uint64) error
		readExtentFunc   func(*stream.ExtentClient, uint64, *proto.ExtentKey, []byte, int, int) (int, error, bool)
		ebsReadFunc      func(*BlobStoreClient, context.Context, string, []byte, uint64, uint64, proto.ObjExtentKey) (int, error)
		expectError      error
	}{
		{true, 2, MockGetObjExtentsTrue, MockGetTrue, MockCheckDataPartitionExistTrue, MockReadExtentTrue, MockEbscReadTrue, os.ErrInvalid},
		{false, 2, MockGetObjExtentsFalse, MockGetTrue, MockCheckDataPartitionExistTrue, MockReadExtentTrue, MockEbscReadTrue, syscall.EIO},
		{false, 2, MockGetObjExtentsTrue, MockGetTrue, MockCheckDataPartitionExistTrue, MockReadExtentTrue, MockEbscReadFalse, syscall.EIO},
		{false, 2, MockGetObjExtentsTrue, MockGetTrue, MockCheckDataPartitionExistTrue, MockReadExtentTrue, MockEbscReadTrue, nil},
	}

	for _, tc := range testCase {
		reader := &Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.close = tc.close
		reader.readConcurrency = tc.readConcurrency

		mw := &meta.MetaWrapper{}
		ebsc := &BlobStoreClient{}
		bc := &bcache.BcacheClient{}
		ec := &stream.ExtentClient{}
		err := gohook.HookMethod(mw, "GetObjExtents", tc.getObjFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(ec, "CheckDataPartitionExsit", tc.checkDpExistFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(ec, "ReadExtent", tc.readExtentFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(ebsc, "Read", tc.ebsReadFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(bc, "Get", tc.bcacheGetFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		reader.mw = mw
		reader.ebs = ebsc
		reader.bc = bc
		reader.ec = ec

		ctx := context.Background()
		buf := make([]byte, 500)
		_, gotError := reader.Read(ctx, buf, 0, 100)
		assert.Equal(t, tc.expectError, gotError)
	}
}

func TestAsyncCache(t *testing.T) {
	ebsc := &BlobStoreClient{}
	ec := &stream.ExtentClient{}
	bc := &bcache.BcacheClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	err = gohook.HookMethod(bc, "Put", MockPutTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}

	testCase := []struct {
		ebsReadFunc  func(*BlobStoreClient, context.Context, string, []byte, uint64, uint64, proto.ObjExtentKey) (int, error)
		cacheAction  int
		enableBcache bool
		fileSize     uint64
	}{
		{MockEbscReadTrue, proto.NoCache, true, rand.Uint64() % 1000},
		{MockEbscReadTrue, proto.RCache, true, rand.Uint64() % 1000},
		{MockEbscReadTrue, proto.RWCache, true, rand.Uint64() % 1000},
		{MockEbscReadTrue, proto.NoCache, false, rand.Uint64() % 1000},
		{MockEbscReadTrue, proto.RCache, false, rand.Uint64() % 1000},
		{MockEbscReadTrue, proto.RWCache, false, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.NoCache, true, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.RCache, true, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.RWCache, true, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.NoCache, false, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.RCache, false, rand.Uint64() % 1000},
		{MockEbscReadFalse, proto.RWCache, false, rand.Uint64() % 1000},
	}

	size := rand.Intn(1000)
	objEk := proto.ObjExtentKey{
		Cid:        0,
		CodeMode:   0,
		BlobSize:   0,
		BlobsLen:   0,
		Size:       uint64(size),
		Blobs:      nil,
		FileOffset: 0,
		Crc:        0,
	}

	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.cacheThreshold = 1000
		ctx := context.Background()
		err := gohook.HookMethod(ebsc, "Read", tc.ebsReadFunc, nil)
		if err != nil {
			t.Fatal(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		reader.fileLength = tc.fileSize
		reader.cacheAction = tc.cacheAction
		reader.asyncCache(ctx, "cacheKey", objEk)
	}

}

func TestNeedCacheL2(t *testing.T) {
	testCase := []struct {
		cacheAction    int
		fileLength     uint64
		cacheThreshold int
		fileCache      bool
		expectCache    bool
	}{
		{proto.NoCache, 10, 100, false, false},
		{proto.NoCache, 10, 100, true, true},
		{proto.NoCache, 101, 100, true, true},
		{proto.NoCache, 101, 100, false, false},
		{proto.RCache, 10, 100, false, true},
		{proto.RCache, 10, 100, true, true},
		{proto.RCache, 101, 100, false, false},
		{proto.RCache, 101, 100, true, true},
		{proto.RWCache, 10, 100, false, true},
		{proto.RWCache, 10, 100, true, true},
		{proto.RWCache, 101, 100, false, false},
		{proto.RWCache, 101, 100, true, true},
	}

	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.cacheAction = tc.cacheAction
		reader.fileLength = tc.fileLength
		reader.cacheThreshold = tc.cacheThreshold
		reader.fileCache = tc.fileCache
		got := reader.needCacheL2()
		assert.Equal(t, tc.expectCache, got)
	}
}

func TestNeedCacheL1(t *testing.T) {
	testCase := []struct {
		enableCache bool
		expectCache bool
	}{
		{true, true},
		{false, false},
	}

	for _, tc := range testCase {
		reader := Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		reader.enableBcache = tc.enableCache
		got := reader.needCacheL1()
		assert.Equal(t, tc.expectCache, got)
	}
}

func TestReadSliceRange(t *testing.T) {
	testCase := []struct {
		enableBcache     bool
		extentKey        proto.ExtentKey
		bcacheGetFunc    func(*bcache.BcacheClient, string, []byte, uint64, uint32) (int, error)
		checkDpExistFunc func(*stream.ExtentClient, uint64) error
		readExtentFunc   func(*stream.ExtentClient, uint64, *proto.ExtentKey, []byte, int, int) (int, error, bool)
		ebsReadFunc      func(*BlobStoreClient, context.Context, string, []byte, uint64, uint64, proto.ObjExtentKey) (int, error)
		expectError      error
	}{
		{false, proto.ExtentKey{}, MockGetTrue, MockCheckDataPartitionExistTrue,
			MockReadExtentTrue, MockEbscReadTrue, nil},
		{false, proto.ExtentKey{}, MockGetTrue, MockCheckDataPartitionExistTrue,
			MockReadExtentTrue, MockEbscReadFalse, syscall.EIO},
		{true, proto.ExtentKey{}, MockGetTrue, MockCheckDataPartitionExistTrue,
			MockReadExtentTrue, MockEbscReadFalse, nil},
		{true, proto.ExtentKey{}, MockGetFalse, MockCheckDataPartitionExistTrue,
			MockReadExtentTrue, MockEbscReadFalse, syscall.EIO},
		{true, proto.ExtentKey{}, MockGetFalse, MockCheckDataPartitionExistTrue,
			MockReadExtentTrue, MockEbscReadTrue, nil},
	}

	for _, tc := range testCase {
		reader := &Reader{}
		reader.limitManager = manager.NewLimitManager(nil)
		ebsc := &BlobStoreClient{}
		bc := &bcache.BcacheClient{}
		ec := &stream.ExtentClient{}
		reader.volName = "cfs"
		reader.ino = 12407
		reader.fileLength = 10
		reader.cacheThreshold = 100
		reader.err = make(chan error)
		rs := &rwSlice{}
		rs.rSize = uint32(len("Hello world"))
		rs.Data = make([]byte, len("Hello world"))
		rs.extentKey = tc.extentKey

		reader.enableBcache = tc.enableBcache
		err := gohook.HookMethod(ebsc, "Read", tc.ebsReadFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(bc, "Get", tc.bcacheGetFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(ec, "CheckDataPartitionExsit", tc.checkDpExistFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		err = gohook.HookMethod(ec, "ReadExtent", tc.readExtentFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		reader.ebs = ebsc
		reader.ec = ec
		reader.bc = bc

		ctx := context.Background()
		reader.wg.Add(1)
		go func() {
			<-reader.err
		}()
		gotError := reader.readSliceRange(ctx, rs)
		assert.Equal(t, tc.expectError, gotError)
	}
}

func MockGetObjExtentsTrue(m *meta.MetaWrapper, inode uint64) (gen uint64, size uint64,
	extents []proto.ExtentKey, objExtents []proto.ObjExtentKey, err error) {
	objEks := make([]proto.ObjExtentKey, 0)
	objEkLen := 5
	expectedFileSize := 0
	for i := 0; i < objEkLen; i++ {
		size := 100
		objEks = append(objEks, proto.ObjExtentKey{Size: uint64(100), FileOffset: uint64(expectedFileSize)})
		expectedFileSize += size
	}
	return 1, 1, nil, objEks, nil
}

func MockGetObjExtentsFalse(m *meta.MetaWrapper, inode uint64) (gen uint64, size uint64,
	extents []proto.ExtentKey, objExtents []proto.ObjExtentKey, err error) {
	return 1, 1, nil, nil, errors.New("Get objEks failed")
}

func MockEbscReadTrue(ebsc *BlobStoreClient, ctx context.Context, volName string,
	buf []byte, offset uint64, size uint64,
	oek proto.ObjExtentKey) (readN int, err error) {
	reader := strings.NewReader("Hello world.")
	readN, err = io.ReadFull(reader, buf)
	return readN, nil
}

func MockEbscReadFalse(ebsc *BlobStoreClient, ctx context.Context, volName string,
	buf []byte, offset uint64, size uint64,
	oek proto.ObjExtentKey) (readN int, err error) {
	return 0, syscall.EIO
}

func MockReadExtentTrue(client *stream.ExtentClient, inode uint64, ek *proto.ExtentKey,
	data []byte, offset int, size int) (read int, err error, b bool) {
	return len("Hello world"), nil, true
}

func MockReadExtentFalse(client *stream.ExtentClient, inode uint64, ek *proto.ExtentKey,
	data []byte, offset int, size int) (read int, err error) {
	return 0, errors.New("Read extent failed")
}

func MockCheckDataPartitionExistTrue(client *stream.ExtentClient, partitionID uint64) error {
	return nil
}

func MockCheckDataPartitionExistFalse(client *stream.ExtentClient, partitionID uint64) error {
	return errors.New("CheckDataPartitionExist failed")
}

func MockWriteTrue(client *stream.ExtentClient, inode uint64, offset int, data []byte,
	flags int, checkFunc func() error) (write int, err error) {
	return len(data), nil
}

func MockWriteFalse(client *stream.ExtentClient, inode uint64, offset int, data []byte,
	flags int) (write int, err error) {
	return 0, errors.New("Write failed")
}

func MockPutTrue(bc *bcache.BcacheClient, key string, buf []byte) error {
	return nil
}
func MockPutFalse(bc *bcache.BcacheClient, key string, buf []byte) error {
	return errors.New("Bcache put failed")
}

func MockGetTrue(bc *bcache.BcacheClient, key string, buf []byte, offset uint64, size uint32) (int, error) {
	return int(size), nil
}

func MockGetFalse(bc *bcache.BcacheClient, key string, buf []byte, offset uint64, size uint32) (int, error) {
	return 0, errors.New("Bcache get failed")
}
