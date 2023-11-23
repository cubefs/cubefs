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
	"reflect"
	"syscall"
	"testing"

	"github.com/brahma-adshonor/gohook"
	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/buf"
)

var (
	writer *Writer
)

func init() {

	//start ebs mock service
	mockServer := NewMockEbsService()
	cfg := access.Config{
		ConnMode: access.QuickConnMode,
		Consul: access.ConsulConfig{
			Address: mockServer.service.URL[7:],
		},
		PriorityAddrs:  []string{mockServer.service.URL},
		MaxSizePutOnce: 1 << 20,
	}

	blobStoreClient, _ := NewEbsClient(cfg)

	config := ClientConfig{
		VolName:         "testVolume",
		VolType:         1,
		BlockSize:       1 << 23,
		Ino:             1000,
		Bc:              nil,
		Mw:              nil,
		Ec:              nil,
		Ebsc:            blobStoreClient,
		EnableBcache:    false,
		WConcurrency:    10,
		ReadConcurrency: 10,
		CacheAction:     2,
		FileCache:       false,
		FileSize:        0,
		CacheThreshold:  0,
	}
	ec := &stream.ExtentClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	config.Ec = ec

	buf.InitCachePool(8388608)
	config.Ec.LimitManager = manager.NewLimitManager(nil)
	writer = NewWriter(config)
}

func newNilWriter() (writer *Writer) {
	return nil
}

func TestNotInstanceWriter_Write(t *testing.T) {
	writer := newNilWriter()
	ctx := context.Background()
	data := []byte{1, 2, 3}
	var flag int
	flag |= proto.FlagsAppend
	_, err := writer.Write(ctx, 0, data, flag)
	//expect err is not nil
	if err == nil {
		t.Fatalf("write is called by not instance writer.")
	}
}

func TestWriter_doBufferWrite_(t *testing.T) {
	//write data to buffer,not write to ebs when len(buffer)<BlockSize
	ctx := context.Background()
	var testCases = []struct {
		offset int
		data   []byte
		n      int
	}{
		{0, make([]byte, 1), 1},
		{1, make([]byte, 10), 10},
		{11, make([]byte, 100), 100},
		{111, make([]byte, 1000), 1000},
		{1111, make([]byte, 10000), 10000},
		{11111, make([]byte, 100000), 100000},
		{111111, make([]byte, 1000000), 1000000},
		{1111111, make([]byte, 5000000), 5000000},
	}
	var flag int
	flag |= proto.FlagsAppend
	var fileSize int
	for _, tc := range testCases {
		n, err := writer.Write(ctx, tc.offset, tc.data, flag)
		if n != tc.n || err != nil {
			t.Fatalf("write fail. write n(%v),expect n(%v) err(%v)", n, tc.n, err)
		}
		fileSize += tc.n
	}
	if writer.CacheFileSize() != fileSize {
		t.Fatalf("write fail. fileSize is correct. fileSize:(%v),expect:(%v)", writer.CacheFileSize(), fileSize)
	}
}

func TestWriter_prepareWriteSlice(t *testing.T) {
	var testCases = []struct {
		offset             int
		dataLen            int
		expectSlices       int
		expectSliceDateLen []int
	}{
		{0, 100, 1, []int{100}},
		{0, 1<<23 - 1, 1, []int{1<<23 - 1}},
		{0, 1 << 23, 1, []int{1 << 23}},
		{0, 1<<23 + 1, 2, []int{1 << 23, 1}},
		{0, 1 << 24, 2, []int{1 << 23, 1 << 23}},
	}
	for _, tc := range testCases {
		data := make([]byte, tc.dataLen)
		wSlices := writer.prepareWriteSlice(tc.offset, data)
		var actualSlices = len(wSlices)
		var actualSliceDateLen = make([]int, 0)
		for _, wSlice := range wSlices {
			actualSliceDateLen = append(actualSliceDateLen, len(wSlice.Data))
		}
		if actualSlices != tc.expectSlices || !reflect.DeepEqual(actualSliceDateLen, tc.expectSliceDateLen) {
			t.Fatalf("prepareWriteSlice fail. actualSlices(%v) expectSlices(%v) "+
				"actualSliceDateLen(%v) expectSliceDateLen(%v)",
				actualSlices, tc.expectSlices, actualSliceDateLen, tc.expectSliceDateLen)
		}
	}
}

func TestPrepareWriteSlice(t *testing.T) {
	testCase := []struct {
		data             []byte
		blockSize        int
		expectSliceCount int
	}{
		{[]byte("hello world"), 10, 2},
		{[]byte("hello world"), 100, 1},
		{[]byte("0123456789012345678901234567890123456789"), 5, 8},
		{[]byte("0123456789012345678901234567890123456789"), 15, 3},
	}
	for _, tc := range testCase {
		writer.blockSize = tc.blockSize
		sliceGot := writer.prepareWriteSlice(0, tc.data)
		assert.Equal(t, tc.expectSliceCount, len(sliceGot))
	}
}

func TestCacheFileSize(t *testing.T) {
	testCase := []struct {
		fileSize   uint64
		expectSize uint64
	}{
		{1, 1},
		{10, 10},
		{100, 100},
		{1000, 1000},
	}
	for _, tc := range testCase {
		writer.fileSize = tc.fileSize
		gotSize := uint64(writer.CacheFileSize())
		assert.Equal(t, tc.expectSize, gotSize)
	}
}

func TestParallelWrite(t *testing.T) {
	ctx := context.Background()
	data := []byte("Hello world")
	offset := 0

	mw := &meta.MetaWrapper{}
	err := gohook.HookMethod(mw, "AppendObjExtentKeys", MockAppendObjExtentKeysTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	writer.mw = mw

	writer.doParallelWrite(ctx, data, offset)
}

func TestCacheL2(t *testing.T) {
	testCase := []struct {
		cacheAction    int
		fileOffset     uint64
		cacheThreshold int
		fileCache      bool
	}{
		{proto.NoCache, 0, 10000, false},
		{proto.NoCache, 0, 10000, true},
		{proto.RCache, 0, 10000, false},
		{proto.RWCache, 0, 10000, false},
		{proto.RCache, 10001, 10000, false},
		{proto.RWCache, 10001, 10000, false},
		{proto.RCache, 10001, 10000, true},
		{proto.RWCache, 10001, 10000, true},
	}

	wSlice := &rwSlice{
		index:        0,
		fileOffset:   0,
		size:         1000,
		rOffset:      0,
		rSize:        1000,
		read:         0,
		Data:         make([]byte, 1000),
		extentKey:    proto.ExtentKey{},
		objExtentKey: proto.ObjExtentKey{},
	}

	ec := &stream.ExtentClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	writer.ec = ec

	for _, tc := range testCase {
		writer.cacheAction = tc.cacheAction
		writer.cacheThreshold = tc.cacheThreshold
		writer.fileCache = tc.fileCache
		wSlice.fileOffset = tc.fileOffset
		writer.cacheLevel2(wSlice)
	}
}

func TestWriterAsyncCache(t *testing.T) {
	testCase := []struct {
		ino    uint64
		offset int
		data   []byte
	}{
		{1, 0, []byte("hello world")},
		{2, 10, []byte("hello world")},
	}

	ec := &stream.ExtentClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	writer.ec = ec
	for _, tc := range testCase {
		writer.asyncCache(tc.ino, tc.offset, tc.data)
	}
}

func TestFlush(t *testing.T) {
	testCase := []struct {
		buf             []byte
		dirty           bool
		flushFlag       bool
		appendObjEkFunc func(*meta.MetaWrapper, uint64, []proto.ObjExtentKey) error
		expectError     error
	}{
		{make([]byte, 0), true, true, MockAppendObjExtentKeysTrue, nil},
		{make([]byte, 0), false, true, MockAppendObjExtentKeysTrue, nil},
		{nil, true, true, MockAppendObjExtentKeysTrue, nil},
		{[]byte("hello world"), false, true, MockAppendObjExtentKeysTrue, nil},
		{[]byte("hello world"), false, true, MockAppendObjExtentKeysFalse, syscall.EIO},
		{[]byte("hello world"), false, false, MockAppendObjExtentKeysTrue, nil},
		{[]byte("hello world"), false, false, MockAppendObjExtentKeysFalse, syscall.EIO},
	}

	ec := &stream.ExtentClient{}
	err := gohook.HookMethod(ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	writer.ec = ec

	for _, tc := range testCase {
		mw := &meta.MetaWrapper{}
		err = gohook.HookMethod(mw, "AppendObjExtentKeys", tc.appendObjEkFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		writer.mw = mw
		writer.buf = tc.buf
		writer.dirty = tc.dirty
		ctx := context.Background()
		_ = writer.flush(1, ctx, tc.flushFlag)
		//assert.Equal(t, tc.expectError, gotError)
	}

}

func TestNewWriter(t *testing.T) {
	config := ClientConfig{
		VolName:         "cfs",
		VolType:         1,
		BlockSize:       1 << 23,
		Ino:             1000,
		Bc:              nil,
		Mw:              nil,
		Ec:              nil,
		Ebsc:            nil,
		EnableBcache:    false,
		WConcurrency:    10,
		ReadConcurrency: 10,
		CacheAction:     2,
		FileCache:       false,
		FileSize:        0,
		CacheThreshold:  0,
	}
	config.Ec = &stream.ExtentClient{}
	err := gohook.HookMethod(config.Ec, "Write", MockWriteTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	config.Ec.LimitManager = manager.NewLimitManager(nil)
	w := NewWriter(config)
	_ = w.String()
}

func TestBufferWrite(t *testing.T) {
	ctx := context.Background()
	data := []byte("Hello world")
	offset := 0

	mw := &meta.MetaWrapper{}
	err := gohook.HookMethod(mw, "AppendObjExtentKeys", MockAppendObjExtentKeysTrue, nil)
	if err != nil {
		panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
	}
	writer.mw = mw
	writer.blockSize = 8388608
	writer.buf = buf.CachePool.Get()

	writer.doBufferWrite(ctx, data, offset)
}

func TestWriteSlice(t *testing.T) {
	testCase := []struct {
		wg           bool
		ebsWriteFunc func(*BlobStoreClient, context.Context, string, []byte, uint32) (access.Location, error)
		expectError  error
	}{
		{false, MockEbscWriteTrue, nil},
		{false, MockEbscWriteFalse, syscall.EIO},
		{true, MockEbscWriteTrue, nil},
		{true, MockEbscWriteFalse, syscall.EIO},
	}

	for _, tc := range testCase {
		ebsc := &BlobStoreClient{}
		err := gohook.HookMethod(ebsc, "Write", tc.ebsWriteFunc, nil)
		if err != nil {
			panic(fmt.Sprintf("Hook advance instance method failed:%s", err.Error()))
		}
		writer.ebsc = ebsc
		wSlice := &rwSlice{
			index:        0,
			fileOffset:   0,
			size:         100,
			rOffset:      0,
			rSize:        100,
			read:         0,
			Data:         make([]byte, 100),
			extentKey:    proto.ExtentKey{},
			objExtentKey: proto.ObjExtentKey{},
		}

		ctx := context.Background()
		writer.err = make(chan *wSliceErr)
		writer.wg.Add(1)
		if tc.wg {
			go func() {
				<-writer.err
			}()
		}
		gotError := writer.writeSlice(ctx, wSlice, tc.wg)
		assert.Equal(t, tc.expectError, gotError)
	}
}

func MockEbscWriteTrue(ebs *BlobStoreClient, ctx context.Context, volName string, data []byte, l uint32) (location access.Location, err error) {
	loc := access.Location{
		ClusterID: 1,
		CodeMode:  0,
		Size:      100,
		BlobSize:  100,
		Crc:       1024,
		Blobs: []access.SliceInfo{
			{
				MinBid: 1,
				Vid:    1,
				Count:  1,
			},
		},
	}
	return loc, nil
}

func MockEbscWriteFalse(ebs *BlobStoreClient, ctx context.Context, volName string, data []byte, l uint32) (location access.Location, err error) {
	return access.Location{}, syscall.EIO
}

func MockAppendObjExtentKeysTrue(mw *meta.MetaWrapper, inode uint64, eks []proto.ObjExtentKey) error {
	return nil
}

func MockAppendObjExtentKeysFalse(mw *meta.MetaWrapper, inode uint64, eks []proto.ObjExtentKey) error {
	return syscall.EIO
}
