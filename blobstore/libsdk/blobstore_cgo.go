package main

/*
#include "libblobstore.h"
*/
import "C"

import (
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// BlobStore Error Codes (mirror of blobstore_errno.h)
const (
	// Success code
	BS_ERR_OK = 0

	// access
	BS_ERR_ACCESS_READ_REQUEST_BODY = 466
	BS_ERR_ACCESS_UNEXPECT          = 550
	BS_ERR_ACCESS_SERVICE_DISCOVERY = 551
	BS_ERR_ACCESS_LIMITED           = 429
	BS_ERR_ACCESS_EXCEED_SIZE       = 400

	// blobnode
	BS_ERR_INVALID_PARAM    = 600
	BS_ERR_ALREADY_EXIST    = 601
	BS_ERR_OUT_OF_LIMIT     = 602
	BS_ERR_INTERNAL         = 603
	BS_ERR_OVERLOAD         = 604
	BS_ERR_PATH_NOT_EXIST   = 605
	BS_ERR_PATH_NOT_EMPTY   = 606
	BS_ERR_PATH_FIND_ONLINE = 607

	BS_ERR_DISK_NOT_FOUND  = 611
	BS_ERR_DISK_BROKEN     = 613
	BS_ERR_INVALID_DISK_ID = 614
	BS_ERR_DISK_NO_SPACE   = 615

	BS_ERR_VUID_NOT_FOUND     = 621
	BS_ERR_VUID_READONLY      = 622
	BS_ERR_VUID_RELEASE       = 623
	BS_ERR_VUID_NOT_MATCH     = 624
	BS_ERR_CHUNK_NOT_READONLY = 625
	BS_ERR_CHUNK_NOT_NORMAL   = 626
	BS_ERR_CHUNK_NO_SPACE     = 627
	BS_ERR_CHUNK_COMPACTING   = 628
	BS_ERR_INVALID_CHUNK_ID   = 630
	BS_ERR_TOO_MANY_CHUNKS    = 632
	BS_ERR_CHUNK_INUSE        = 633
	BS_ERR_SIZE_OVER_BURST    = 634

	BS_ERR_BID_NOT_FOUND           = 651
	BS_ERR_SHARD_SIZE_TOO_LARGE    = 652
	BS_ERR_SHARD_NOT_MARK_DELETE   = 653
	BS_ERR_SHARD_MARK_DELETED      = 654
	BS_ERR_SHARD_INVALID_OFFSET    = 655
	BS_ERR_SHARD_LIST_EXCEED_LIMIT = 656
	BS_ERR_SHARD_INVALID_BID       = 657

	BS_ERR_DEST_REPLICA_BAD           = 670
	BS_ERR_ORPHAN_SHARD               = 671
	BS_ERR_ILLEGAL_TASK               = 672
	BS_ERR_REQUEST_LIMITED            = 673
	BS_ERR_UNSUPPORTED_TASK_CODE_MODE = 674
	BS_ERR_PUT_SHARD_TIMEOUT          = 675

	// scheduler
	BS_ERR_NOTHING_TODO          = 700
	BS_ERR_UPDATE_VOL_CACHE_FREQ = 701

	// proxy
	BS_ERR_NO_CODEMODE_VOLUME   = 801
	BS_ERR_ALLOC_BID_FROM_CM    = 802
	BS_ERR_CLUSTER_ID_NOT_MATCH = 803

	// clustermgr
	BS_ERR_CM_UNEXPECT                         = 900
	BS_ERR_LOCK_NOT_ALLOW                      = 902
	BS_ERR_UNLOCK_NOT_ALLOW                    = 903
	BS_ERR_VOLUME_NOT_EXIST                    = 904
	BS_ERR_RAFT_PROPOSE                        = 906
	BS_ERR_NO_LEADER                           = 907
	BS_ERR_RAFT_READ_INDEX                     = 908
	BS_ERR_DUPLICATED_MEMBER_INFO              = 910
	BS_ERR_CM_DISK_NOT_FOUND                   = 911
	BS_ERR_INVALID_DISK_STATUS                 = 912
	BS_ERR_CHANGE_DISK_STATUS_NOT_ALLOW        = 913
	BS_ERR_CONCURRENT_ALLOC_VOLUME_UNIT        = 914
	BS_ERR_ALLOC_VOLUME_INVALID_PARAMS         = 916
	BS_ERR_NO_AVAILABLE_VOLUME                 = 917
	BS_ERR_OLD_VUID_NOT_MATCH                  = 918
	BS_ERR_NEW_VUID_NOT_MATCH                  = 919
	BS_ERR_NEW_DISK_ID_NOT_MATCH               = 920
	BS_ERR_CONFIG_ARGUMENT                     = 921
	BS_ERR_INVALID_CLUSTER_ID                  = 922
	BS_ERR_INVALID_IDC                         = 923
	BS_ERR_VOLUME_UNIT_NOT_EXIST               = 924
	BS_ERR_REGISTER_SERVICE_INVALID_PARAMS     = 925
	BS_ERR_DISK_ABNORMAL_OR_NOT_READONLY       = 926
	BS_ERR_STAT_CHUNK_FAILED                   = 927
	BS_ERR_INVALID_CODE_MODE                   = 928
	BS_ERR_RETAIN_VOLUME_NOT_ALLOC             = 929
	BS_ERR_DROPPED_DISK_HAS_VOLUME_UNIT        = 930
	BS_ERR_NOT_SUPPORT_IDLE                    = 931
	BS_ERR_DISK_IS_DROPPING                    = 932
	BS_ERR_REJECT_DELETE_SYSTEM_CONFIG         = 933
	BS_ERR_CM_NODE_NOT_FOUND                   = 934
	BS_ERR_CM_HAS_DISK_NOT_DROPPED_OR_REPAIRED = 935
	BS_ERR_CM_NODE_SET_NOT_FOUND               = 936
)

type blobStoreHandle struct {
	handle C.BlobStoreHandle
}

func (h blobStoreHandle) IsInValid() bool {
	return h.handle == C.BLOBSTORE_INVALID_HANDLE
}

func getFinHandle(handle C.BlobStoreHandle) (uintptr, error) {
	h := cgo.Handle(handle)
	wrapper, ok := h.Value().(*blobStorageWrapper)
	if !ok {
		return 0, fmt.Errorf("wrapper not found")
	}

	fHandle := finalizerTracker.GetHandle(wrapper)
	return fHandle, nil
}

// newBlobStoreHandle creates a new BlobStoreHandle with the given config path.
func newBlobStoreHandle(confPath string) (blobStoreHandle, int) {
	var errorCode C.int
	cStr := C.CString(confPath)
	defer C.free(unsafe.Pointer(cStr))

	handle := C.new_blobstore_handle(cStr, &errorCode)
	return blobStoreHandle{handle: handle}, int(errorCode)
}

// newBlobStoreHandleWithNilConf is used in tests to simulate passing NULL to C.new_blobstore_handle.
func newBlobStoreHandleWithNilConf() (blobStoreHandle, int) {
	var errorCode C.int
	handle := C.new_blobstore_handle(nil, &errorCode)
	return blobStoreHandle{handle: handle}, int(errorCode)
}

// newBlobStoreHandleWithNilErrorCode is used in tests to simulate passing nil as errorCode to C.new_blobstore_handle.
func newBlobStoreHandleWithNilErrorCode(confPath string) (blobStoreHandle, error) {
	cStr := C.CString(confPath)
	defer C.free(unsafe.Pointer(cStr))

	handle := C.new_blobstore_handle(cStr, (*C.int)(nil))
	if handle == C.BLOBSTORE_INVALID_HANDLE {
		return blobStoreHandle{handle: handle}, fmt.Errorf("failed to create blobstore handle with nil errorCode")
	}
	return blobStoreHandle{handle: handle}, nil
}

// freeBlobStoreHandle frees the given BlobStoreHandle by calling the C function.
func freeBlobStoreHandle(handle *blobStoreHandle) {
	C.free_blobstore_handle(&handle.handle)
}

func newCBlobStorePutArgs(data []byte, hashes access.HashAlgorithm) (*C.BlobStorePutArgs, error) {
	args := C.malloc(C.sizeof_BlobStorePutArgs)
	if args == nil {
		return nil, fmt.Errorf("failed to allocate BlobStorePutArgs")
	}

	cData := C.CBytes(data)
	if cData == nil {
		return nil, fmt.Errorf("failed to allocate memory for data")
	}

	cArgs := (*C.BlobStorePutArgs)(args)

	cArgs.size = C.int64_t(len(data))
	cArgs.data = cData
	cArgs.hint = C.BlobStorePutHint(0)
	cArgs.hashes = C.BlobStoreHashAlgorithm(hashes)

	return cArgs, nil
}

func freeCBlobStorePutArgs(args *C.BlobStorePutArgs) {
	if args != nil {
		if args.data != nil {
			C.free(unsafe.Pointer(args.data))
		}
		C.free(unsafe.Pointer(args))
	}
}

func convertToGoLocation(cLoc *C.BlobStoreLocation) *access.Location {
	if cLoc == nil {
		return nil
	}

	goLoc := &access.Location{
		ClusterID: proto.ClusterID(cLoc.cluster_id),
		CodeMode:  codemode.CodeMode(cLoc.code_mode),
		Size:      uint64(cLoc.size),
		BlobSize:  uint32(cLoc.blob_size),
		Crc:       uint32(cLoc.crc),
	}

	// Convert blobs array
	blobsLen := int(cLoc.blobs_len)
	if blobsLen > 0 && cLoc.blobs != nil {
		goLoc.Blobs = make([]access.SliceInfo, blobsLen)
		for i := 0; i < blobsLen; i++ {
			cBlob := (*C.BlobStoreSliceInfo)(unsafe.Pointer(uintptr(unsafe.Pointer(cLoc.blobs)) + uintptr(i)*unsafe.Sizeof(C.BlobStoreSliceInfo{})))
			goLoc.Blobs[i] = access.SliceInfo{
				MinBid: proto.BlobID(cBlob.min_bid),
				Vid:    proto.Vid(cBlob.vid),
				Count:  uint32(cBlob.count),
			}
		}
	} else {
		goLoc.Blobs = []access.SliceInfo{}
	}

	return goLoc
}

func convertToGoHashSumMap(cHash *C.BlobStoreHashSumMap) *access.HashSumMap {
	if cHash == nil {
		return nil
	}

	goHashSumMap := make(access.HashSumMap)

	entries := (*[1 << 30]C.BlobStoreHashEntry)(unsafe.Pointer(cHash.entries))[:cHash.count:cHash.count]
	for _, entry := range entries {
		algorithm := access.HashAlgorithm(entry.algorithm)
		valueLen := int(entry.value_length)
		valuePtr := unsafe.Pointer(entry.value)
		valueBytes := C.GoBytes(valuePtr, C.int(valueLen))

		goHashSumMap[algorithm] = valueBytes
	}

	return &goHashSumMap
}

// wrapper of C.blobstore_put
func blobStorePut(handle blobStoreHandle, data []byte, hashes access.HashAlgorithm) (*access.Location, *access.HashSumMap, int) {
	cPutArgs, err := newCBlobStorePutArgs(data, hashes)
	if err != nil {
		return nil, nil, -1
	}

	defer freeCBlobStorePutArgs(cPutArgs)

	var cLoc *C.BlobStoreLocation = nil
	var cHash *C.BlobStoreHashSumMap = nil

	r := int(C.blobstore_put(handle.handle, cPutArgs, &cLoc, &cHash))

	defer C.free_blobstore_location(&cLoc)
	defer C.free_blobstore_hash_sum_map(&cHash)

	gLoc := convertToGoLocation(cLoc)
	gHash := convertToGoHashSumMap(cHash)

	return gLoc, gHash, r
}

// blobStorePutWithNilHandle calls blobstore_put with a nil handle.
func blobStorePutWithNilHandle(data []byte, hashes access.HashAlgorithm) (*access.Location, *access.HashSumMap, int) {
	cPutArgs, err := newCBlobStorePutArgs(data, hashes)
	if err != nil {
		return nil, nil, -1
	}
	defer freeCBlobStorePutArgs(cPutArgs)

	var cLoc *C.BlobStoreLocation = nil
	var cHash *C.BlobStoreHashSumMap = nil

	r := int(C.blobstore_put(C.BLOBSTORE_INVALID_HANDLE, cPutArgs, &cLoc, &cHash))

	defer C.free_blobstore_location(&cLoc)
	defer C.free_blobstore_hash_sum_map(&cHash)

	gLoc := convertToGoLocation(cLoc)
	gHash := convertToGoHashSumMap(cHash)

	return gLoc, gHash, r
}

// blobStorePutWithNilPutArgs calls blobstore_put with a nil put_args.
func blobStorePutWithNilPutArgs(handle blobStoreHandle) (*access.Location, *access.HashSumMap, int) {
	var cLoc *C.BlobStoreLocation = nil
	var cHash *C.BlobStoreHashSumMap = nil

	r := int(C.blobstore_put(handle.handle, nil, &cLoc, &cHash))

	defer C.free_blobstore_location(&cLoc)
	defer C.free_blobstore_hash_sum_map(&cHash)

	gLoc := convertToGoLocation(cLoc)
	gHash := convertToGoHashSumMap(cHash)

	return gLoc, gHash, r
}

// blobStorePutWithNilLocation calls blobstore_put with a nil out_location.
func blobStorePutWithNilLocation(handle blobStoreHandle, data []byte, hashes access.HashAlgorithm) (*access.HashSumMap, int) {
	cPutArgs, err := newCBlobStorePutArgs(data, hashes)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStorePutArgs(cPutArgs)

	var cHash *C.BlobStoreHashSumMap = nil

	r := int(C.blobstore_put(handle.handle, cPutArgs, nil, &cHash))

	defer C.free_blobstore_hash_sum_map(&cHash)

	gHash := convertToGoHashSumMap(cHash)

	return gHash, r
}

// blobStorePutWithNilHashSumMap calls blobstore_put with a nil out_hash_sum_map.
func blobStorePutWithNilHashSumMap(handle blobStoreHandle, data []byte, hashes access.HashAlgorithm) (*access.Location, int) {
	cPutArgs, err := newCBlobStorePutArgs(data, hashes)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStorePutArgs(cPutArgs)

	var cLoc *C.BlobStoreLocation = nil

	r := int(C.blobstore_put(handle.handle, cPutArgs, &cLoc, nil))

	defer C.free_blobstore_location(&cLoc)

	gLoc := convertToGoLocation(cLoc)

	return gLoc, r
}

// newCBlobStoreGetArgs allocates and initializes a C.BlobStoreGetArgs, location and data receive buffer
func newCBlobStoreGetArgs(loc *access.Location, offset int64, readSize int64, nilData bool) (*C.BlobStoreGetArgs, error) {
	// allocate memory for cArgs.data right after C.BlobStoreGetArgs
	totalSize := C.size_t(C.sizeof_BlobStoreGetArgs)
	if !nilData {
		totalSize += C.size_t(readSize)
	}
	args := C.malloc(totalSize)
	if args == nil {
		return nil, fmt.Errorf("failed to allocate BlobStoreGetArgs and data buffer")
	}

	cArgs := (*C.BlobStoreGetArgs)(args)
	cArgs.offset = C.uint64_t(offset)
	cArgs.read_size = C.uint64_t(readSize)
	cArgs.hint = C.BlobStoreGetHint(0) // reserved

	var outCLoc *C.BlobStoreLocation = nil
	if loc != nil {
		if ret := exportGoLocation(&outCLoc, loc, nil); ret != 0 {
			C.free(args)
			return nil, fmt.Errorf("failed to convert location: ret = %d", ret)
		}
	}

	cArgs.location = outCLoc
	if nilData {
		cArgs.data = nil
	} else {
		cArgs.data = unsafe.Pointer(uintptr(unsafe.Pointer(args)) + uintptr(C.sizeof_BlobStoreGetArgs))
	}

	return cArgs, nil
}

// free all memory newCBlobStoreGetArgs allocated for C.BlobStoreGetArgs, location and CArgs.data
func freeCBlobStoreGetArgs(args *C.BlobStoreGetArgs) {
	if args != nil {
		cLoc := args.location
		if cLoc != nil {
			free_blobstore_location(&cLoc)
		}
		C.free(unsafe.Pointer(args))
	}
}

// wrapper of C.blobstore_get
func blobStoreGet(handle blobStoreHandle, loc *access.Location, offset, readSize uint64, nilData bool) ([]byte, int) {
	// Allocate and initialize C.BlobStoreGetArgs including data buffer
	cArgs, err := newCBlobStoreGetArgs(loc, int64(offset), int64(readSize), nilData)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStoreGetArgs(cArgs)

	retCode := C.blobstore_get(handle.handle, cArgs)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	// Read the data from the C buffer into a Go byte slice
	goData := C.GoBytes(cArgs.data, C.int(cArgs.read_size))

	return goData, int(C.BS_ERR_OK)
}

func blobStoreGetWithNilHandle(loc *access.Location, offset, readSize uint64) ([]byte, int) {
	// Allocate and initialize C.BlobStoreGetArgs including data buffer
	cArgs, err := newCBlobStoreGetArgs(loc, int64(offset), int64(readSize), false)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStoreGetArgs(cArgs)

	retCode := C.blobstore_get(C.BLOBSTORE_INVALID_HANDLE, cArgs)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	// Read the data from the C buffer into a Go byte slice
	goData := C.GoBytes(cArgs.data, C.int(cArgs.read_size))

	return goData, int(C.BS_ERR_OK)
}

func blobStoreGetWithNilArgs(handle blobStoreHandle) int {
	return int(C.blobstore_get(handle.handle, nil))
}

func exportGoLocationToCWrapper(gLoc *access.Location) (unsafe.Pointer, int) {
	var cLoc *C.BlobStoreLocation = nil
	retCode := exportGoLocation(&cLoc, gLoc, nil)
	if retCode != 0 {
		return nil, int(retCode)
	}

	return unsafe.Pointer(cLoc), int(C.BS_ERR_OK)
}

func freeCLocationWrapper(cLoc unsafe.Pointer) {
	loc := (*C.BlobStoreLocation)(cLoc)
	C.free_blobstore_location(&loc)
}

func convertCLocationToGoWrapper(cLoc unsafe.Pointer) *access.Location {
	loc := (*C.BlobStoreLocation)(cLoc)
	gLoc, _ := convertCLocationToGo(loc)
	return gLoc
}

func exportGoLocationsToCWrapper(gLocs []access.Location) (unsafe.Pointer, int) {
	var cLocs *C.BlobStoreLocations = nil
	retCode := exportGoLocations(&cLocs, gLocs)
	if retCode != 0 {
		return nil, int(retCode)
	}
	return unsafe.Pointer(cLocs), int(C.BS_ERR_OK)
}

func freeCLocationsWrapper(cLocs unsafe.Pointer) {
	locs := (*C.BlobStoreLocations)(cLocs)
	C.free_blobstore_locations(&locs)
}

func convertCLocationsToGoWrapper(cLocs unsafe.Pointer) []access.Location {
	locs := (*C.BlobStoreLocations)(cLocs)
	gLocs, _ := convertCLocationsToGo(locs)
	return gLocs
}

// newCBlobStoreDelArgs allocates and initializes a C.BlobStoreDeleteArgs structure
func newCBlobStoreDelArgs(locs []access.Location) (*C.BlobStoreDeleteArgs, error) {
	if len(locs) == 0 {
		return nil, fmt.Errorf("location slice is empty")
	}

	// Allocate C.BlobStoreDeleteArgs structure
	args := C.malloc(C.sizeof_BlobStoreDeleteArgs)
	if args == nil {
		return nil, fmt.Errorf("failed to allocate BlobStoreDeleteArgs")
	}
	cArgs := (*C.BlobStoreDeleteArgs)(args)

	// Set hint to 0 as reserved
	cArgs.hint = 0

	if ret := exportGoLocations(&cArgs.locations, locs); ret != 0 {
		C.free(args)
		return nil, fmt.Errorf("failed to convert locations: %d", ret)
	}

	return cArgs, nil
}

func freeCBlobStoreDelArgs(args *C.BlobStoreDeleteArgs) {
	if args != nil {
		free_blobstore_locations(&args.locations)
		C.free(unsafe.Pointer(args))
	}
}

// wrapper of C.blobstore_delete
func blobStoreDelete(handle blobStoreHandle, locs []access.Location) ([]access.Location, int) {
	cArgs, err := newCBlobStoreDelArgs(locs)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStoreDelArgs(cArgs)

	var cResult *C.BlobStoreLocations = nil

	retCode := C.blobstore_delete(handle.handle, cArgs, &cResult)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	var goResult []access.Location
	if cResult != nil {
		defer free_blobstore_locations(&cResult)

		converted, err := convertCLocationsToGo(cResult)
		if err != nil {
			return nil, -1
		}
		goResult = converted
	}

	return goResult, int(C.BS_ERR_OK)
}

func blobStoreDeleteWithNilHandle(locs []access.Location) ([]access.Location, int) {
	cArgs, err := newCBlobStoreDelArgs(locs)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStoreDelArgs(cArgs)

	var cResult *C.BlobStoreLocations = nil

	retCode := C.blobstore_delete(C.BLOBSTORE_INVALID_HANDLE, cArgs, &cResult)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	var goResult []access.Location
	if cResult != nil {
		defer free_blobstore_locations(&cResult)

		converted, err := convertCLocationsToGo(cResult)
		if err != nil {
			return nil, -1
		}
		goResult = converted
	}

	return goResult, int(C.BS_ERR_OK)
}

func blobStoreDeleteWithNilArgs(handle blobStoreHandle) ([]access.Location, int) {
	var cResult *C.BlobStoreLocations = nil

	retCode := C.blobstore_delete(handle.handle, nil, &cResult)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	var goResult []access.Location
	if cResult != nil {
		defer free_blobstore_locations(&cResult)

		converted, err := convertCLocationsToGo(cResult)
		if err != nil {
			return nil, -1
		}
		goResult = converted
	}

	return goResult, int(C.BS_ERR_OK)
}

func blobStoreDeleteWithNilResult(handle blobStoreHandle, locs []access.Location) ([]access.Location, int) {
	cArgs, err := newCBlobStoreDelArgs(locs)
	if err != nil {
		return nil, -1
	}
	defer freeCBlobStoreDelArgs(cArgs)

	retCode := C.blobstore_delete(handle.handle, cArgs, nil)
	if retCode != C.BS_ERR_OK {
		return nil, int(retCode)
	}

	return nil, int(C.BS_ERR_OK)
}
