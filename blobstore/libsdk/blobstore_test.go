package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// backup for restore
var originalAccessAPIProvider = accessAPIProvider

func getValidBlobStoreHandle(tb testing.TB) (blobStoreHandle, *mocks.MockAccessAPI) {
	// Setup mock
	ctrl := gomock.NewController(tb)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockAccessAPI(ctrl)
	accessAPIProvider = func(path string) (access.API, error) {
		return mockAPI, nil
	}
	defer func() { accessAPIProvider = originalAccessAPIProvider }()

	handle, code := newBlobStoreHandle("/tmp/config.json")
	assert.Equal(tb, BS_ERR_OK, code, "Expected error code 0")
	assert.False(tb, handle.IsInValid(), "Expected valid handle")

	return handle, mockAPI
}

func TestNewBlobStoreHandle_InvalidConfigPath(t *testing.T) {
	expectedErr := -int(syscall.EINVAL)
	_, code := newBlobStoreHandle("")
	if code != expectedErr {
		assert.EqualValues(t, expectedErr, code, "Error codes do not match")
	}
}

func TestNewBlobStoreHandle_NilConfPointer(t *testing.T) {
	expectedErr := -int(syscall.EINVAL)

	_, code := newBlobStoreHandleWithNilConf()
	if code != expectedErr {
		assert.EqualValues(t, expectedErr, code, "Error codes do not match")
	}
}

func TestNewBlobStoreHandle_IgnoreErrorCode(t *testing.T) {
	// Setup
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockAccessAPI(ctrl)

	accessAPIProvider = func(path string) (access.API, error) {
		return mockAPI, nil
	}
	defer func() { accessAPIProvider = originalAccessAPIProvider }()

	handle, err := newBlobStoreHandleWithNilErrorCode("/tmp/config.json")
	assert.NoError(t, err, "Expected success, got error: %v", err)
	defer freeBlobStoreHandle(&handle)

	assert.False(t, handle.IsInValid(), "Expected valid handle")
}

func TestFreeBlobStoreHandle_Idempotent(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)

	// Free twice
	freeBlobStoreHandle(&handle)
	freeBlobStoreHandle(&handle)

	assert.True(t, handle.IsInValid(), "Expected handle to be invalid after double free")
}

func setFinalizerTrackerForTest(tracker *FinalizerTracker) {
	finalizerTracker = tracker
}

func ResetFinalizerTrackerForTest() {
	if finalizerTracker != nil {
		finalizerTracker.Reset()
	}
	finalizerTracker = nil
}

func TestNewBlobStoreHandle_NotGced(t *testing.T) {
	setFinalizerTrackerForTest(NewFinalizerTracker())
	defer ResetFinalizerTrackerForTest()

	handle, _ := getValidBlobStoreHandle(t)

	var err error
	fHandle, err := getFinHandle(handle.handle)
	assert.NoError(t, err, "Retrieve wrapper by handle failed")

	// Force GC multiple times
	for i := 0; i < 5; i++ {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
	}

	assert.False(t, finalizerTracker.WasFinalized(fHandle),
		"BlobStoreHandle was unexpectedly garbage collected")

	freeBlobStoreHandle(&handle)
}

func TestFreeBlobStoreHandle_CanBeGced(t *testing.T) {
	setFinalizerTrackerForTest(NewFinalizerTracker())
	defer ResetFinalizerTrackerForTest()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handle, _ := getValidBlobStoreHandle(t)

	var err error
	fHandle, err := getFinHandle(handle.handle)
	assert.NoError(t, err, "Failed to retrieve wrapper by handle")

	freeBlobStoreHandle(&handle)

	// Force GC multiple times and wait for the finalizer to execute
	for i := 0; i < 5 && !finalizerTracker.WasFinalized(fHandle); i++ {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, finalizerTracker.WasFinalized(fHandle),
		"BlobStoreHandle was not garbage collected as expected")
}

func TestBlobStorePutWithNilHandle(t *testing.T) {
	loc, hash, code := blobStorePutWithNilHandle([]byte("test"), access.HashAlgSHA256)
	assert.Less(t, code, 0, "Expected negative error code for nil handle")
	assert.Nil(t, loc, "Expected nil location")
	assert.Nil(t, hash, "Expected nil hash sum map")
}

func TestBlobStorePutWithNilPutArgs(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)

	loc, hash, code := blobStorePutWithNilPutArgs(handle)
	assert.Less(t, code, 0, "Expected negative error code for nil put args")
	assert.Nil(t, loc, "Expected nil location")
	assert.Nil(t, hash, "Expected nil hash sum map")
}

func TestBlobStorePutWithNilLocation(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)

	hash, code := blobStorePutWithNilLocation(handle, []byte("test"), access.HashAlgSHA256)
	assert.Equal(t, -int(syscall.EINVAL), code, "Expected -EINVAL error code")
	assert.Nil(t, hash, "Expected nil hash sum map")
}

func TestBlobStorePutWithNilHashSumMap(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	data := []byte("test")
	hashAlg := access.HashAlgSHA256

	// Expect Put call with the provided arguments and return a mock location
	mockLocation := access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      4,
		BlobSize:  4,
		Crc:       12345,
		Blobs:     []access.SliceInfo{{MinBid: proto.BlobID(1), Vid: proto.Vid(1), Count: 1}},
	}

	mockHashSumMap := access.HashSumMap{
		hashAlg: []byte("687b899aaa76b05547a0ea834e70ab38351ad463f2d0bd7d38dd163948135d85"),
	}

	mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(mockLocation, mockHashSumMap, nil)

	loc, code := blobStorePutWithNilHashSumMap(handle, data, hashAlg)
	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code")
	assert.NotNil(t, loc, "Expected non-nil location")

	assert.Equal(t, mockLocation.ClusterID, loc.ClusterID, "ClusterID mismatch")
	assert.Equal(t, mockLocation.Size, loc.Size, "Size mismatch")
}

func TestBlobStorePut_WithValidArgs(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	data := []byte("test-data")
	hashAlg := access.HashAlgSHA256

	mockLocation := access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(data)),
		BlobSize:  uint32(len(data)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	mockHashSumMap := access.HashSumMap{
		hashAlg: []byte("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
	}

	mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(mockLocation, mockHashSumMap, nil)

	loc, hashSumMap, code := blobStorePut(handle, data, hashAlg)
	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code")
	assert.NotNil(t, loc, "Expected non-nil location")
	assert.Equal(t, mockLocation.ClusterID, loc.ClusterID, "ClusterID mismatch")
	assert.Equal(t, mockLocation.Size, loc.Size, "Size mismatch")

	assert.Len(t, loc.Blobs, len(mockLocation.Blobs), "Blob count mismatch")
	assert.Equal(t, mockLocation.Blobs[0].MinBid, loc.Blobs[0].MinBid, "MinBid mismatch")

	assert.NotNil(t, hashSumMap, "Expected non-nil hash sum map")
	sum, ok := (*hashSumMap)[hashAlg]
	assert.True(t, ok, "Expected hash algorithm %d in hash sum map", hashAlg)
	assert.Equal(t, mockHashSumMap[hashAlg], sum, "Hash value mismatch")
}

func TestBlobStorePut_EmptyData(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)
	data := []byte{}
	hashAlg := access.HashAlgSHA256

	mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *access.PutArgs) (access.Location, access.HashSumMap, error) {
			if req.Size != 0 {
				t.Errorf("Expected empty data")
			}
			hashSumMap := req.Hashes.ToHashSumMap()
			for alg := range hashSumMap {
				hashSumMap[alg] = alg.ToHasher().Sum(nil)
			}
			return access.Location{Blobs: make([]access.SliceInfo, 0)}, hashSumMap, nil
		},
	)

	loc, hashSumMap, code := blobStorePut(handle, data, hashAlg)
	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code")

	assert.NotNil(t, loc, "Expected non-nil location even for empty data")
	assert.Empty(t, loc.Blobs, "Expected zero blobs for empty data")

	assert.NotNil(t, hashSumMap, "Expected non-nil hash sum map for empty data with hash algorithm set")

	emptyHash := hashAlg.ToHasher().Sum(nil)
	actualHash, ok := (*hashSumMap)[hashAlg]

	assert.True(t, ok, "Expected hash algorithm %v in hash sum map", hashAlg)
	assert.Equal(t, emptyHash, actualHash, "Expected empty hash value")
}

func TestBlobStorePut_MultipleBlobs(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)
	data := []byte("test-multi-blob")
	hashAlg := access.HashAlgSHA256

	mockLocation := access.Location{
		ClusterID: proto.ClusterID(2),
		CodeMode:  codemode.CodeMode(1),
		Size:      uint64(len(data)),
		BlobSize:  1024,
		Crc:       0x87654321,
		Blobs: []access.SliceInfo{
			{MinBid: proto.BlobID(1001), Vid: proto.Vid(2001), Count: 2},
			{MinBid: proto.BlobID(1003), Vid: proto.Vid(2002), Count: 3},
		},
	}

	mockHashSumMap := access.HashSumMap{
		hashAlg: []byte("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
	}

	mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(mockLocation, mockHashSumMap, nil)

	loc, hashSumMap, code := blobStorePut(handle, data, hashAlg)
	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code %d, got %d", BS_ERR_OK, code)
	assert.NotNil(t, loc, "Expected non-nil location")
	assert.Len(t, loc.Blobs, 2, "Expected 2 blobs, got %d", len(loc.Blobs))

	assert.Equal(t, proto.BlobID(1001), loc.Blobs[0].MinBid, "Unexpected first blob ID")
	assert.Equal(t, proto.BlobID(1003), loc.Blobs[1].MinBid, "Unexpected second blob ID")

	assert.NotNil(t, hashSumMap, "Expected non-nil hash sum map")
}

var internalErrTests = []struct {
	name         string
	mockErr      error
	expectedCode int
}{
	// access
	{
		name:         "BS_ERR_ACCESS_READ_REQUEST_BODY",
		mockErr:      errcode.Error(BS_ERR_ACCESS_READ_REQUEST_BODY),
		expectedCode: -BS_ERR_ACCESS_READ_REQUEST_BODY,
	},
	{
		name:         "BS_ERR_ACCESS_UNEXPECT",
		mockErr:      errcode.Error(BS_ERR_ACCESS_UNEXPECT),
		expectedCode: -BS_ERR_ACCESS_UNEXPECT,
	},
	{
		name:         "BS_ERR_ACCESS_SERVICE_DISCOVERY",
		mockErr:      errcode.Error(BS_ERR_ACCESS_SERVICE_DISCOVERY),
		expectedCode: -BS_ERR_ACCESS_SERVICE_DISCOVERY,
	},
	{
		name:         "BS_ERR_ACCESS_LIMITED",
		mockErr:      errcode.Error(BS_ERR_ACCESS_LIMITED),
		expectedCode: -BS_ERR_ACCESS_LIMITED,
	},
	{
		name:         "BS_ERR_ACCESS_EXCEED_SIZE",
		mockErr:      errcode.Error(BS_ERR_ACCESS_EXCEED_SIZE),
		expectedCode: -BS_ERR_ACCESS_EXCEED_SIZE,
	},
	// blobnode
	{
		name:         "BS_ERR_INVALID_PARAM",
		mockErr:      errcode.Error(BS_ERR_INVALID_PARAM),
		expectedCode: -BS_ERR_INVALID_PARAM,
	},
	{
		name:         "BS_ERR_ALREADY_EXIST",
		mockErr:      errcode.Error(BS_ERR_ALREADY_EXIST),
		expectedCode: -BS_ERR_ALREADY_EXIST,
	},
	{
		name:         "BS_ERR_OUT_OF_LIMIT",
		mockErr:      errcode.Error(BS_ERR_OUT_OF_LIMIT),
		expectedCode: -BS_ERR_OUT_OF_LIMIT,
	},
	{
		name:         "BS_ERR_INTERNAL",
		mockErr:      errcode.Error(BS_ERR_INTERNAL),
		expectedCode: -BS_ERR_INTERNAL,
	},
	{
		name:         "BS_ERR_OVERLOAD",
		mockErr:      errcode.Error(BS_ERR_OVERLOAD),
		expectedCode: -BS_ERR_OVERLOAD,
	},
	{
		name:         "BS_ERR_PATH_NOT_EXIST",
		mockErr:      errcode.Error(BS_ERR_PATH_NOT_EXIST),
		expectedCode: -BS_ERR_PATH_NOT_EXIST,
	},
	{
		name:         "BS_ERR_PATH_NOT_EMPTY",
		mockErr:      errcode.Error(BS_ERR_PATH_NOT_EMPTY),
		expectedCode: -BS_ERR_PATH_NOT_EMPTY,
	},
	{
		name:         "BS_ERR_PATH_FIND_ONLINE",
		mockErr:      errcode.Error(BS_ERR_PATH_FIND_ONLINE),
		expectedCode: -BS_ERR_PATH_FIND_ONLINE,
	},
	{
		name:         "BS_ERR_DISK_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_DISK_NOT_FOUND),
		expectedCode: -BS_ERR_DISK_NOT_FOUND,
	},
	{
		name:         "BS_ERR_DISK_BROKEN",
		mockErr:      errcode.Error(BS_ERR_DISK_BROKEN),
		expectedCode: -BS_ERR_DISK_BROKEN,
	},
	{
		name:         "BS_ERR_INVALID_DISK_ID",
		mockErr:      errcode.Error(BS_ERR_INVALID_DISK_ID),
		expectedCode: -BS_ERR_INVALID_DISK_ID,
	},
	{
		name:         "BS_ERR_DISK_NO_SPACE",
		mockErr:      errcode.Error(BS_ERR_DISK_NO_SPACE),
		expectedCode: -BS_ERR_DISK_NO_SPACE,
	},

	{
		name:         "BS_ERR_VUID_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_VUID_NOT_FOUND),
		expectedCode: -BS_ERR_VUID_NOT_FOUND,
	},
	{
		name:         "BS_ERR_VUID_READONLY",
		mockErr:      errcode.Error(BS_ERR_VUID_READONLY),
		expectedCode: -BS_ERR_VUID_READONLY,
	},
	{
		name:         "BS_ERR_VUID_RELEASE",
		mockErr:      errcode.Error(BS_ERR_VUID_RELEASE),
		expectedCode: -BS_ERR_VUID_RELEASE,
	},
	{
		name:         "BS_ERR_VUID_NOT_MATCH",
		mockErr:      errcode.Error(BS_ERR_VUID_NOT_MATCH),
		expectedCode: -BS_ERR_VUID_NOT_MATCH,
	},
	{
		name:         "BS_ERR_CHUNK_NOT_READONLY",
		mockErr:      errcode.Error(BS_ERR_CHUNK_NOT_READONLY),
		expectedCode: -BS_ERR_CHUNK_NOT_READONLY,
	},
	{
		name:         "BS_ERR_CHUNK_NOT_NORMAL",
		mockErr:      errcode.Error(BS_ERR_CHUNK_NOT_NORMAL),
		expectedCode: -BS_ERR_CHUNK_NOT_NORMAL,
	},
	{
		name:         "BS_ERR_CHUNK_NO_SPACE",
		mockErr:      errcode.Error(BS_ERR_CHUNK_NO_SPACE),
		expectedCode: -BS_ERR_CHUNK_NO_SPACE,
	},
	{
		name:         "BS_ERR_CHUNK_COMPACTING",
		mockErr:      errcode.Error(BS_ERR_CHUNK_COMPACTING),
		expectedCode: -BS_ERR_CHUNK_COMPACTING,
	},
	{
		name:         "BS_ERR_INVALID_CHUNK_ID",
		mockErr:      errcode.Error(BS_ERR_INVALID_CHUNK_ID),
		expectedCode: -BS_ERR_INVALID_CHUNK_ID,
	},
	{
		name:         "BS_ERR_TOO_MANY_CHUNKS",
		mockErr:      errcode.Error(BS_ERR_TOO_MANY_CHUNKS),
		expectedCode: -BS_ERR_TOO_MANY_CHUNKS,
	},
	{
		name:         "BS_ERR_CHUNK_INUSE",
		mockErr:      errcode.Error(BS_ERR_CHUNK_INUSE),
		expectedCode: -BS_ERR_CHUNK_INUSE,
	},
	{
		name:         "BS_ERR_SIZE_OVER_BURST",
		mockErr:      errcode.Error(BS_ERR_SIZE_OVER_BURST),
		expectedCode: -BS_ERR_SIZE_OVER_BURST,
	},

	{
		name:         "BS_ERR_BID_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_BID_NOT_FOUND),
		expectedCode: -BS_ERR_BID_NOT_FOUND,
	},
	{
		name:         "BS_ERR_SHARD_SIZE_TOO_LARGE",
		mockErr:      errcode.Error(BS_ERR_SHARD_SIZE_TOO_LARGE),
		expectedCode: -BS_ERR_SHARD_SIZE_TOO_LARGE,
	},
	{
		name:         "BS_ERR_SHARD_NOT_MARK_DELETE",
		mockErr:      errcode.Error(BS_ERR_SHARD_NOT_MARK_DELETE),
		expectedCode: -BS_ERR_SHARD_NOT_MARK_DELETE,
	},
	{
		name:         "BS_ERR_SHARD_MARK_DELETED",
		mockErr:      errcode.Error(BS_ERR_SHARD_MARK_DELETED),
		expectedCode: -BS_ERR_SHARD_MARK_DELETED,
	},
	{
		name:         "BS_ERR_SHARD_INVALID_OFFSET",
		mockErr:      errcode.Error(BS_ERR_SHARD_INVALID_OFFSET),
		expectedCode: -BS_ERR_SHARD_INVALID_OFFSET,
	},
	{
		name:         "BS_ERR_SHARD_LIST_EXCEED_LIMIT",
		mockErr:      errcode.Error(BS_ERR_SHARD_LIST_EXCEED_LIMIT),
		expectedCode: -BS_ERR_SHARD_LIST_EXCEED_LIMIT,
	},
	{
		name:         "BS_ERR_SHARD_INVALID_BID",
		mockErr:      errcode.Error(BS_ERR_SHARD_INVALID_BID),
		expectedCode: -BS_ERR_SHARD_INVALID_BID,
	},

	{
		name:         "BS_ERR_DEST_REPLICA_BAD",
		mockErr:      errcode.Error(BS_ERR_DEST_REPLICA_BAD),
		expectedCode: -BS_ERR_DEST_REPLICA_BAD,
	},
	{
		name:         "BS_ERR_ORPHAN_SHARD",
		mockErr:      errcode.Error(BS_ERR_ORPHAN_SHARD),
		expectedCode: -BS_ERR_ORPHAN_SHARD,
	},
	{
		name:         "BS_ERR_ILLEGAL_TASK",
		mockErr:      errcode.Error(BS_ERR_ILLEGAL_TASK),
		expectedCode: -BS_ERR_ILLEGAL_TASK,
	},
	{
		name:         "BS_ERR_REQUEST_LIMITED",
		mockErr:      errcode.Error(BS_ERR_REQUEST_LIMITED),
		expectedCode: -BS_ERR_REQUEST_LIMITED,
	},
	{
		name:         "BS_ERR_UNSUPPORTED_TASK_CODE_MODE",
		mockErr:      errcode.Error(BS_ERR_UNSUPPORTED_TASK_CODE_MODE),
		expectedCode: -BS_ERR_UNSUPPORTED_TASK_CODE_MODE,
	},
	{
		name:         "BS_ERR_PUT_SHARD_TIMEOUT",
		mockErr:      errcode.Error(BS_ERR_PUT_SHARD_TIMEOUT),
		expectedCode: -BS_ERR_PUT_SHARD_TIMEOUT,
	},

	// scheduler
	{
		name:         "BS_ERR_NOTHING_TODO",
		mockErr:      errcode.Error(BS_ERR_NOTHING_TODO),
		expectedCode: -BS_ERR_NOTHING_TODO,
	},
	{
		name:         "BS_ERR_UPDATE_VOL_CACHE_FREQ",
		mockErr:      errcode.Error(BS_ERR_UPDATE_VOL_CACHE_FREQ),
		expectedCode: -BS_ERR_UPDATE_VOL_CACHE_FREQ,
	},

	// proxy
	{
		name:         "BS_ERR_NO_CODEMODE_VOLUME",
		mockErr:      errcode.Error(BS_ERR_NO_CODEMODE_VOLUME),
		expectedCode: -BS_ERR_NO_CODEMODE_VOLUME,
	},
	{
		name:         "BS_ERR_ALLOC_BID_FROM_CM",
		mockErr:      errcode.Error(BS_ERR_ALLOC_BID_FROM_CM),
		expectedCode: -BS_ERR_ALLOC_BID_FROM_CM,
	},
	{
		name:         "BS_ERR_CLUSTER_ID_NOT_MATCH",
		mockErr:      errcode.Error(BS_ERR_CLUSTER_ID_NOT_MATCH),
		expectedCode: -BS_ERR_CLUSTER_ID_NOT_MATCH,
	},

	// clustermgr
	{
		name:         "BS_ERR_CM_UNEXPECT",
		mockErr:      errcode.Error(BS_ERR_CM_UNEXPECT),
		expectedCode: -BS_ERR_CM_UNEXPECT,
	},
	{
		name:         "BS_ERR_LOCK_NOT_ALLOW",
		mockErr:      errcode.Error(BS_ERR_LOCK_NOT_ALLOW),
		expectedCode: -BS_ERR_LOCK_NOT_ALLOW,
	},
	{
		name:         "BS_ERR_UNLOCK_NOT_ALLOW",
		mockErr:      errcode.Error(BS_ERR_UNLOCK_NOT_ALLOW),
		expectedCode: -BS_ERR_UNLOCK_NOT_ALLOW,
	},
	{
		name:         "BS_ERR_VOLUME_NOT_EXIST",
		mockErr:      errcode.Error(BS_ERR_VOLUME_NOT_EXIST),
		expectedCode: -BS_ERR_VOLUME_NOT_EXIST,
	},
	{
		name:         "BS_ERR_RAFT_PROPOSE",
		mockErr:      errcode.Error(BS_ERR_RAFT_PROPOSE),
		expectedCode: -BS_ERR_RAFT_PROPOSE,
	},
	{
		name:         "BS_ERR_NO_LEADER",
		mockErr:      errcode.Error(BS_ERR_NO_LEADER),
		expectedCode: -BS_ERR_NO_LEADER,
	},
	{
		name:         "BS_ERR_RAFT_READ_INDEX",
		mockErr:      errcode.Error(BS_ERR_RAFT_READ_INDEX),
		expectedCode: -BS_ERR_RAFT_READ_INDEX,
	},
	{
		name:         "BS_ERR_DUPLICATED_MEMBER_INFO",
		mockErr:      errcode.Error(BS_ERR_DUPLICATED_MEMBER_INFO),
		expectedCode: -BS_ERR_DUPLICATED_MEMBER_INFO,
	},
	{
		name:         "BS_ERR_CM_DISK_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_CM_DISK_NOT_FOUND),
		expectedCode: -BS_ERR_CM_DISK_NOT_FOUND,
	},
	{
		name:         "BS_ERR_INVALID_DISK_STATUS",
		mockErr:      errcode.Error(BS_ERR_INVALID_DISK_STATUS),
		expectedCode: -BS_ERR_INVALID_DISK_STATUS,
	},
	{
		name:         "BS_ERR_CHANGE_DISK_STATUS_NOT_ALLOW",
		mockErr:      errcode.Error(BS_ERR_CHANGE_DISK_STATUS_NOT_ALLOW),
		expectedCode: -BS_ERR_CHANGE_DISK_STATUS_NOT_ALLOW,
	},
	{
		name:         "BS_ERR_CONCURRENT_ALLOC_VOLUME_UNIT",
		mockErr:      errcode.Error(BS_ERR_CONCURRENT_ALLOC_VOLUME_UNIT),
		expectedCode: -BS_ERR_CONCURRENT_ALLOC_VOLUME_UNIT,
	},
	{
		name:         "BS_ERR_ALLOC_VOLUME_INVALID_PARAMS",
		mockErr:      errcode.Error(BS_ERR_ALLOC_VOLUME_INVALID_PARAMS),
		expectedCode: -BS_ERR_ALLOC_VOLUME_INVALID_PARAMS,
	},
	{
		name:         "BS_ERR_NO_AVAILABLE_VOLUME",
		mockErr:      errcode.Error(BS_ERR_NO_AVAILABLE_VOLUME),
		expectedCode: -BS_ERR_NO_AVAILABLE_VOLUME,
	},
	{
		name:         "BS_ERR_OLD_VUID_NOT_MATCH",
		mockErr:      errcode.Error(BS_ERR_OLD_VUID_NOT_MATCH),
		expectedCode: -BS_ERR_OLD_VUID_NOT_MATCH,
	},
	{
		name:         "BS_ERR_NEW_VUID_NOT_MATCH",
		mockErr:      errcode.Error(BS_ERR_NEW_VUID_NOT_MATCH),
		expectedCode: -BS_ERR_NEW_VUID_NOT_MATCH,
	},
	{
		name:         "BS_ERR_NEW_DISK_ID_NOT_MATCH",
		mockErr:      errcode.Error(BS_ERR_NEW_DISK_ID_NOT_MATCH),
		expectedCode: -BS_ERR_NEW_DISK_ID_NOT_MATCH,
	},
	{
		name:         "BS_ERR_CONFIG_ARGUMENT",
		mockErr:      errcode.Error(BS_ERR_CONFIG_ARGUMENT),
		expectedCode: -BS_ERR_CONFIG_ARGUMENT,
	},
	{
		name:         "BS_ERR_INVALID_CLUSTER_ID",
		mockErr:      errcode.Error(BS_ERR_INVALID_CLUSTER_ID),
		expectedCode: -BS_ERR_INVALID_CLUSTER_ID,
	},
	{
		name:         "BS_ERR_INVALID_IDC",
		mockErr:      errcode.Error(BS_ERR_INVALID_IDC),
		expectedCode: -BS_ERR_INVALID_IDC,
	},
	{
		name:         "BS_ERR_VOLUME_UNIT_NOT_EXIST",
		mockErr:      errcode.Error(BS_ERR_VOLUME_UNIT_NOT_EXIST),
		expectedCode: -BS_ERR_VOLUME_UNIT_NOT_EXIST,
	},
	{
		name:         "BS_ERR_REGISTER_SERVICE_INVALID_PARAMS",
		mockErr:      errcode.Error(BS_ERR_REGISTER_SERVICE_INVALID_PARAMS),
		expectedCode: -BS_ERR_REGISTER_SERVICE_INVALID_PARAMS,
	},
	{
		name:         "BS_ERR_DISK_ABNORMAL_OR_NOT_READONLY",
		mockErr:      errcode.Error(BS_ERR_DISK_ABNORMAL_OR_NOT_READONLY),
		expectedCode: -BS_ERR_DISK_ABNORMAL_OR_NOT_READONLY,
	},
	{
		name:         "BS_ERR_STAT_CHUNK_FAILED",
		mockErr:      errcode.Error(BS_ERR_STAT_CHUNK_FAILED),
		expectedCode: -BS_ERR_STAT_CHUNK_FAILED,
	},
	{
		name:         "BS_ERR_INVALID_CODE_MODE",
		mockErr:      errcode.Error(BS_ERR_INVALID_CODE_MODE),
		expectedCode: -BS_ERR_INVALID_CODE_MODE,
	},
	{
		name:         "BS_ERR_RETAIN_VOLUME_NOT_ALLOC",
		mockErr:      errcode.Error(BS_ERR_RETAIN_VOLUME_NOT_ALLOC),
		expectedCode: -BS_ERR_RETAIN_VOLUME_NOT_ALLOC,
	},
	{
		name:         "BS_ERR_DROPPED_DISK_HAS_VOLUME_UNIT",
		mockErr:      errcode.Error(BS_ERR_DROPPED_DISK_HAS_VOLUME_UNIT),
		expectedCode: -BS_ERR_DROPPED_DISK_HAS_VOLUME_UNIT,
	},
	{
		name:         "BS_ERR_NOT_SUPPORT_IDLE",
		mockErr:      errcode.Error(BS_ERR_NOT_SUPPORT_IDLE),
		expectedCode: -BS_ERR_NOT_SUPPORT_IDLE,
	},
	{
		name:         "BS_ERR_DISK_IS_DROPPING",
		mockErr:      errcode.Error(BS_ERR_DISK_IS_DROPPING),
		expectedCode: -BS_ERR_DISK_IS_DROPPING,
	},
	{
		name:         "BS_ERR_REJECT_DELETE_SYSTEM_CONFIG",
		mockErr:      errcode.Error(BS_ERR_REJECT_DELETE_SYSTEM_CONFIG),
		expectedCode: -BS_ERR_REJECT_DELETE_SYSTEM_CONFIG,
	},
	{
		name:         "BS_ERR_CM_NODE_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_CM_NODE_NOT_FOUND),
		expectedCode: -BS_ERR_CM_NODE_NOT_FOUND,
	},
	{
		name:         "BS_ERR_CM_HAS_DISK_NOT_DROPPED_OR_REPAIRED",
		mockErr:      errcode.Error(BS_ERR_CM_HAS_DISK_NOT_DROPPED_OR_REPAIRED),
		expectedCode: -BS_ERR_CM_HAS_DISK_NOT_DROPPED_OR_REPAIRED,
	},
	{
		name:         "BS_ERR_CM_NODE_SET_NOT_FOUND",
		mockErr:      errcode.Error(BS_ERR_CM_NODE_SET_NOT_FOUND),
		expectedCode: -BS_ERR_CM_NODE_SET_NOT_FOUND,
	},
}

func TestBlobStorePut_ReturnError(t *testing.T) {
	data := []byte("test-error")
	hashes := access.HashAlgSHA256

	for _, tt := range internalErrTests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)
			mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(access.Location{}, nil, tt.mockErr)

			loc, hashSumMap, code := blobStorePut(handle, data, hashes)
			assert.Equal(t, tt.expectedCode, code, "Expected error code %d, got %d", tt.expectedCode, code)
			assert.Nil(t, loc, "Expected nil location on error")
			assert.Nil(t, hashSumMap, "Expected nil hash sum map on error")
		})
	}
}

func TestBlobStorePut_ReturnStandardError(t *testing.T) {
	data := []byte("test-error")
	hashes := access.HashAlgSHA256

	// EHWPOISON, ref: /usr/include/asm-generic/errno.h
	errnoEnd := 133

	tests := make([]struct {
		name         string
		mockErr      error
		expectedCode int
	}, errnoEnd)

	for i := 1; i <= errnoEnd; i++ {
		testName := fmt.Sprintf("Standard_Error_%d", i)
		tests[i-1] = struct {
			name         string
			mockErr      error
			expectedCode int
		}{
			name:         testName,
			mockErr:      syscall.Errno(i),
			expectedCode: -i,
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)

			mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(access.Location{}, nil, tt.mockErr)

			loc, hashSumMap, code := blobStorePut(handle, data, hashes)
			assert.Equal(t, tt.expectedCode, code,
				"Expected error code %d, got %d for error %s", tt.expectedCode, code, tt.name)
			assert.Nil(t, loc,
				"Expected nil location on error: %s", tt.name)
			assert.Nil(t, hashSumMap,
				"Expected nil hash sum map on error: %s", tt.name)
		})
	}
}

func TestBlobStorePut_ReturnInternalServerError(t *testing.T) {
	data := []byte("test-error")
	hashes := access.HashAlgSHA256

	test := struct {
		name         string
		data         []byte
		hashes       access.HashAlgorithm
		mockErr      error
		expectedCode int
	}{
		name:         "InternalServerError",
		data:         data,
		hashes:       hashes,
		mockErr:      errors.New(http.StatusText(http.StatusInternalServerError)),
		expectedCode: -http.StatusInternalServerError,
	}

	t.Run(test.name, func(t *testing.T) {
		handle, mockAPI := getValidBlobStoreHandle(t)

		// Mock the Put method to return an internal server error
		mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).Return(access.Location{}, nil, test.mockErr)

		loc, hashSumMap, code := blobStorePut(handle, test.data, test.hashes)
		assert.Equal(t, test.expectedCode, code,
			"Expected error code %d, got %d", test.expectedCode, code)
		assert.Nil(t, loc, "Expected nil location on error")
		assert.Nil(t, hashSumMap, "Expected nil hash sum map on error")
	})
}

func TestBlobStoreGet_WithValidArgs(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	expectedData := []byte("test-data-to-get")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(expectedData)),
		BlobSize:  uint32(len(expectedData)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, args *access.GetArgs) (io.ReadCloser, error) {
			_, err := args.Writer.Write(expectedData)
			return io.NopCloser(bytes.NewReader([]byte{})), err
		},
	)

	readData, code := blobStoreGet(handle, mockLocation, 0, uint64(len(expectedData)), false)
	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code %d, got %d", BS_ERR_OK, code)
	assert.NotNil(t, readData, "Expected non-nil data")

	assert.Equal(t, expectedData, readData, "Expected data '%s', got '%s'", expectedData, readData)
}

func TestBlobStoreGet_WithNilHandle(t *testing.T) {
	_, mockAPI := getValidBlobStoreHandle(t)

	expectedData := []byte("test-data-to-get")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(expectedData)),
		BlobSize:  uint32(len(expectedData)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, args *access.GetArgs) (io.ReadCloser, error) {
			_, err := args.Writer.Write(expectedData)
			return io.NopCloser(bytes.NewReader([]byte{})), err
		},
	)

	readData, code := blobStoreGetWithNilHandle(mockLocation, 0, uint64(len(expectedData)))

	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
	assert.Nil(t, readData, "Expected nil data with invalid handle")
}

func TestBlobStoreGet_WithNilArgs(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)

	code := blobStoreGetWithNilArgs(handle)
	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
}

func TestBlobStoreGet_WithNilLocation(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)
	data := []byte("test-data-to-get")

	_, code := blobStoreGet(handle, nil, 0, uint64(len(data)), false)

	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
}

func TestBlobStoreGet_WithNilData(t *testing.T) {
	handle, _ := getValidBlobStoreHandle(t)

	expectedData := []byte("test-data-to-get")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(expectedData)),
		BlobSize:  uint32(len(expectedData)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	_, code := blobStoreGet(handle, mockLocation, 0, uint64(len(expectedData)), true)
	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
}

func TestBlobStoreGet_ReturnError(t *testing.T) {
	data := []byte("test-error")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(data)),
		BlobSize:  uint32(len(data)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	for _, tt := range internalErrTests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)
			mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, tt.mockErr)

			_, code := blobStoreGet(handle, mockLocation, 0, uint64(len(data)), false)
			assert.Equal(t, tt.expectedCode, code, "Error code mismatch")
		})
	}
}

func TestBlobStoreGet_ReturnStandardError(t *testing.T) {
	data := []byte("test-error")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(data)),
		BlobSize:  uint32(len(data)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	// EHWPOISON, ref: /usr/include/asm-generic/errno.h
	errnoEnd := 133

	tests := make([]struct {
		name         string
		mockErr      error
		expectedCode int
	}, errnoEnd)

	for i := 1; i <= errnoEnd; i++ {
		testName := fmt.Sprintf("Standard_Error_%d", i)
		tests[i-1] = struct {
			name         string
			mockErr      error
			expectedCode int
		}{
			name:         testName,
			mockErr:      syscall.Errno(i),
			expectedCode: -i,
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)
			mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, tt.mockErr)

			_, code := blobStoreGet(handle, mockLocation, 0, uint64(len(data)), false)
			assert.Equal(t, tt.expectedCode, code, "Error code mismatch")
		})
	}
}

func TestBlobStoreGet_ReturnInternalServerError(t *testing.T) {
	data := []byte("test-error")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(data)),
		BlobSize:  uint32(len(data)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	test := struct {
		name         string
		mockErr      error
		expectedCode int
	}{
		name:         "InternalServerError",
		mockErr:      errors.New(http.StatusText(http.StatusInternalServerError)),
		expectedCode: -http.StatusInternalServerError,
	}

	t.Run(test.name, func(t *testing.T) {
		handle, mockAPI := getValidBlobStoreHandle(t)
		mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, test.mockErr)

		_, code := blobStoreGet(handle, mockLocation, 0, uint64(len(data)), false)
		assert.Equal(t, test.expectedCode, code, "Error code mismatch")
	})
}

func TestExportGoLocation_CGO(t *testing.T) {
	gLoc1 := access.Location{
		ClusterID: 1,
		CodeMode:  0,
		Size:      1024,
		BlobSize:  512,
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{MinBid: 1001, Vid: 2001, Count: 1},
		},
	}

	ptr, code := exportGoLocationToCWrapper(&gLoc1)
	defer freeCLocationWrapper(ptr)

	assert.Equal(t, int(BS_ERR_OK), code)

	gLoc2 := convertCLocationToGoWrapper(ptr)

	assert.Equal(t, gLoc1.ClusterID, gLoc2.ClusterID)
	assert.Equal(t, gLoc1.CodeMode, gLoc2.CodeMode)
	assert.Equal(t, gLoc1.Size, gLoc2.Size)
	assert.Equal(t, gLoc1.BlobSize, gLoc2.BlobSize)
	assert.Equal(t, gLoc1.Crc, gLoc2.Crc)
	assert.Equal(t, len(gLoc1.Blobs), len(gLoc2.Blobs))

	for i := range gLoc1.Blobs {
		assert.Equal(t, gLoc1.Blobs[i].MinBid, gLoc2.Blobs[i].MinBid)
		assert.Equal(t, gLoc1.Blobs[i].Vid, gLoc2.Blobs[i].Vid)
		assert.Equal(t, gLoc1.Blobs[i].Count, gLoc2.Blobs[i].Count)
	}
}

func TestExportGoLocations_CGO(t *testing.T) {
	locations := []access.Location{
		{
			ClusterID: 1,
			CodeMode:  0,
			Size:      1024,
			BlobSize:  512,
			Crc:       0x12345678,
			Blobs: []access.SliceInfo{
				{MinBid: 1001, Vid: 2001, Count: 1},
			},
		},
		{
			ClusterID: 2,
			CodeMode:  1,
			Size:      2048,
			BlobSize:  1024,
			Crc:       0x9ABCDEF0,
			Blobs: []access.SliceInfo{
				{MinBid: 2002, Vid: 3002, Count: 2},
			},
		},
		{
			ClusterID: 3,
			CodeMode:  2,
			Size:      4096,
			BlobSize:  2048,
			Crc:       0x0,
			Blobs: []access.SliceInfo{
				{MinBid: 3003, Vid: 4003, Count: 3},
			},
		},
	}

	ptr, code := exportGoLocationsToCWrapper(locations)
	defer freeCLocationsWrapper(ptr)
	assert.Equal(t, int(BS_ERR_OK), code)

	goLocations := convertCLocationsToGoWrapper(ptr)

	assert.Equal(t, len(locations), len(goLocations))

	for i := range locations {
		assert.Equal(t, locations[i].ClusterID, goLocations[i].ClusterID)
		assert.Equal(t, locations[i].CodeMode, goLocations[i].CodeMode)
		assert.Equal(t, locations[i].Size, goLocations[i].Size)
		assert.Equal(t, locations[i].BlobSize, goLocations[i].BlobSize)
		assert.Equal(t, locations[i].Crc, goLocations[i].Crc)
		assert.Equal(t, len(locations[i].Blobs), len(goLocations[i].Blobs))

		for j := range locations[i].Blobs {
			assert.Equal(t, locations[i].Blobs[j].MinBid, goLocations[i].Blobs[j].MinBid)
			assert.Equal(t, locations[i].Blobs[j].Vid, goLocations[i].Blobs[j].Vid)
			assert.Equal(t, locations[i].Blobs[j].Count, goLocations[i].Blobs[j].Count)
		}
	}
}

func TestBlobStoreDelete_WithValidArgs(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	mockLocations := []access.Location{
		{
			ClusterID: proto.ClusterID(1),
			CodeMode:  codemode.CodeMode(0),
			Size:      1024,
			BlobSize:  512,
			Crc:       0x12345678,
			Blobs: []access.SliceInfo{
				{MinBid: proto.BlobID(1001), Vid: proto.Vid(2001), Count: 1},
			},
		},
		{
			ClusterID: proto.ClusterID(2),
			CodeMode:  codemode.CodeMode(1),
			Size:      2048,
			BlobSize:  1024,
			Crc:       0x23456789,
			Blobs: []access.SliceInfo{
				{MinBid: proto.BlobID(1002), Vid: proto.Vid(2002), Count: 1},
			},
		},
		{
			ClusterID: proto.ClusterID(3),
			CodeMode:  codemode.CodeMode(2),
			Size:      4096,
			BlobSize:  2048,
			Crc:       0x3456789A,
			Blobs: []access.SliceInfo{
				{MinBid: proto.BlobID(1003), Vid: proto.Vid(2003), Count: 1},
			},
		},
	}

	retainedLocations := []access.Location{
		mockLocations[1],
		mockLocations[2],
	}

	mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(retainedLocations, nil)

	resultLocs, code := blobStoreDelete(handle, mockLocations)

	assert.Equal(t, int(BS_ERR_OK), code, "Expected success code %d, got %d", BS_ERR_OK, code)
	assert.Len(t, resultLocs, 2, "Expected 2 result locations, got %d", len(resultLocs))

	for i, loc := range retainedLocations {
		result := resultLocs[i]
		expected := loc
		assert.Equal(t, expected.ClusterID, result.ClusterID, "Mismatch on ClusterID for location %d", i+1)
		assert.Equal(t, expected.CodeMode, result.CodeMode, "Mismatch on CodeMode for location %d", i+1)
		assert.Equal(t, expected.Crc, result.Crc, "Mismatch on Crc for location %d", i+1)
		assert.True(t, reflect.DeepEqual(expected.Blobs, result.Blobs), "Mismatch on Blobs for location %d", i+1)
	}
}

var mockLocations = []access.Location{
	{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      1024,
		BlobSize:  512,
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{MinBid: proto.BlobID(1001), Vid: proto.Vid(2001), Count: 1},
		},
	},
	{
		ClusterID: proto.ClusterID(2),
		CodeMode:  codemode.CodeMode(1),
		Size:      2048,
		BlobSize:  1024,
		Crc:       0x23456789,
		Blobs: []access.SliceInfo{
			{MinBid: proto.BlobID(1002), Vid: proto.Vid(2002), Count: 1},
		},
	},
	{
		ClusterID: proto.ClusterID(3),
		CodeMode:  codemode.CodeMode(2),
		Size:      4096,
		BlobSize:  2048,
		Crc:       0x3456789A,
		Blobs: []access.SliceInfo{
			{MinBid: proto.BlobID(1003), Vid: proto.Vid(2003), Count: 1},
		},
	},
}

func TestBlobStoreDelete_WithNilHandle(t *testing.T) {
	_, mockAPI := getValidBlobStoreHandle(t)

	mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, nil)

	resultLocs, code := blobStoreDeleteWithNilHandle(mockLocations)

	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
	assert.Nil(t, resultLocs, "Expected nil resultLocs with invalid handle")
}

func TestBlobStoreDelete_WithNilArgs(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, nil)

	resultLocs, code := blobStoreDeleteWithNilArgs(handle)

	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
	assert.Nil(t, resultLocs, "Expected nil resultLocs with invalid handle")
}

func TestBlobStoreDelete_WithNilResult(t *testing.T) {
	handle, mockAPI := getValidBlobStoreHandle(t)

	mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, nil)

	resultLocs, code := blobStoreDeleteWithNilResult(handle, mockLocations)

	assert.Equal(t, -int(syscall.EINVAL), code, "Expected EINVAL error code")
	assert.Nil(t, resultLocs, "Expected nil resultLocs with invalid handle")
}

func TestBlobStoreDelete_ReturnError(t *testing.T) {
	for _, tt := range internalErrTests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)
			mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, tt.mockErr)

			resultLocs, code := blobStoreDelete(handle, mockLocations)
			assert.Equal(t, tt.expectedCode, code, "Error code mismatch")
			assert.Nil(t, resultLocs, "Expected nil resultLocs with invalid handle")
		})
	}
}

func TestBlobStoreDelete_ReturnStandardError(t *testing.T) {
	// EHWPOISON, ref: /usr/include/asm-generic/errno.h
	errnoEnd := 133

	tests := make([]struct {
		name         string
		mockErr      error
		expectedCode int
	}, errnoEnd)

	for i := 1; i <= errnoEnd; i++ {
		testName := fmt.Sprintf("Standard_Error_%d", i)
		tests[i-1] = struct {
			name         string
			mockErr      error
			expectedCode int
		}{
			name:         testName,
			mockErr:      syscall.Errno(i),
			expectedCode: -i,
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle, mockAPI := getValidBlobStoreHandle(t)
			mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, tt.mockErr)

			resultLocs, code := blobStoreDelete(handle, mockLocations)
			assert.Equal(t, tt.expectedCode, code, "Error code mismatch")
			assert.Nil(t, resultLocs, "Expected nil resultLocs with invalid handle")
		})
	}
}

func BenchmarkBlobstorePut(b *testing.B) {
	handle, mockAPI := getValidBlobStoreHandle(b)
	data := []byte{}
	hashAlg := access.HashAlgDummy

	mockAPI.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *access.PutArgs) (access.Location, access.HashSumMap, error) {
			return access.Location{Blobs: make([]access.SliceInfo, 0)}, nil, nil
		},
	).AnyTimes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, code := blobStorePut(handle, data, hashAlg)
		if code != int(BS_ERR_OK) {
			b.Fatalf("Expected success code %d, got %d", BS_ERR_OK, code)
		}
	}
	b.StopTimer()
}

func BenchmarkBlobstoreGet(b *testing.B) {
	handle, mockAPI := getValidBlobStoreHandle(b)
	expectedData := []byte("1")

	mockLocation := &access.Location{
		ClusterID: proto.ClusterID(1),
		CodeMode:  codemode.CodeMode(0),
		Size:      uint64(len(expectedData)),
		BlobSize:  uint32(len(expectedData)),
		Crc:       0x12345678,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(1001),
				Vid:    proto.Vid(2001),
				Count:  1,
			},
		},
	}

	mockAPI.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, args *access.GetArgs) (io.ReadCloser, error) {
			_, err := args.Writer.Write(expectedData)
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(expectedData)), nil
		},
	).AnyTimes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		readData, code := blobStoreGet(handle, mockLocation, 0, uint64(len(expectedData)), false)

		if code != int(BS_ERR_OK) {
			b.Fatalf("Expected success code %d, got %d", BS_ERR_OK, code)
		}
		if !bytes.Equal(expectedData, readData) {
			b.Fatalf("Expected data '%s', got '%s'", expectedData, string(readData))
		}
	}

	b.StopTimer()
}

func BenchmarkBlobstoreDelete(b *testing.B) {
	handle, mockAPI := getValidBlobStoreHandle(b)

	mockLocations := []access.Location{
		{
			ClusterID: proto.ClusterID(1),
			CodeMode:  codemode.CodeMode(0),
			Size:      1024,
			BlobSize:  512,
			Crc:       0x12345678,
			Blobs: []access.SliceInfo{
				{MinBid: proto.BlobID(1001), Vid: proto.Vid(2001), Count: 1},
			},
		},
	}

	mockAPI.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, code := blobStoreDelete(handle, mockLocations)

		if code != int(BS_ERR_OK) {
			b.Fatalf("Expected success code %d, got %d", BS_ERR_OK, code)
		}
	}

	b.StopTimer()
}
