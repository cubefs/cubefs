package main

/*
#include <stdlib.h> // malloc, free
#include <string.h> // memcpy
#include <stdint.h>

#include "blobstore_types.h"
#include "blobstore_errno.h"
*/
import (
	"C"
)

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/cgo"
	"syscall"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/sdk"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type AccessAPIProvider func(string) (access.API, error)

var (
	accessAPIProvider AccessAPIProvider = newAccessAPI
	finalizerTracker  *FinalizerTracker = nil
)

// errorToStatus converts a Go error into a C-compatible integer error code.
// The returned value is suitable for use in CGO-exported functions,
// where 0 typically indicates success and non-zero values indicate errors.
//
// Priority order:
// 1. nil -> return 0
// 2. syscall.Errno -> return the raw errno value as int
// 3. else -> use DetectCode to extract the error code
func errorToStatus(err error) C.int {
	// OK
	if err == nil {
		return C.BS_ERR_OK
	}

	// Try to extract a system-level errno first.
	if errno, ok := err.(syscall.Errno); ok {
		return C.int(errno)
	}

	// Use DetectCode to extract the appropriate error code from the error object.
	return C.int(errcode.DetectCode(err))
}

// Wrapper of access API
type blobStorageWrapper struct {
	api access.API
}

func (b *blobStorageWrapper) Close() {
	// do nothing
}

// new_blobstore_handle creates a new BlobStore handle based on the provided configuration file path.
//
// Parameters:
//   - conf_path: A null-terminated C string representing the path to the configuration file.
//     The file should exist and contain valid JSON data required to initialize the BlobStore client.
//   - error_code: Optional pointer where the result of this function will be stored.
//     If not NULL, it will be set to 0 on success,
//     or a negative integer representing an error code on failure.
//
// Returns:
//   - On success, returns a valid non-zero BlobStoreHandle.
//   - On failure, returns 0. If error_code is not NULL, it will be set to a negative error code.
//
// Notes:
//   - The caller must call free_blobstore_handle() to release the returned handle and associated resources.
//   - If error_code is NULL, no detailed error information will be returned, but the function will still return 0 on failure.
//
//export new_blobstore_handle
func new_blobstore_handle(conf_path *C.char, error_code *C.int) C.BlobStoreHandle {
	if conf_path == nil || *conf_path == 0 {
		if error_code != nil {
			*error_code = -C.int(syscall.EINVAL)
		}
		return C.BLOBSTORE_INVALID_HANDLE
	}

	goConfPath := C.GoString(conf_path)

	// Create a new access API instance
	api, err := accessAPIProvider(goConfPath)
	if err != nil {
		if error_code != nil {
			*error_code = -C.int(errorToStatus(err))
		}
		return C.BLOBSTORE_INVALID_HANDLE
	}

	// Wrap the API and create a handle
	wrapper := &blobStorageWrapper{api: api}
	h := cgo.NewHandle(wrapper)

	log.Debug("new_blobstore_handle: wrapper=0x%x, handle=%d\n", wrapper, h)

	// Set finalizer and start tracking
	if finalizerTracker != nil {
		finalizerTracker.Track(wrapper)
	}
	if error_code != nil {
		*error_code = C.BS_ERR_OK
	}

	return C.BlobStoreHandle(h)
}

func newAccessAPI(confPath string) (access.API, error) {
	confStr, err := os.ReadFile(confPath)
	if err != nil {
		return nil, err
	}

	var conf sdk.Config
	if err := json.Unmarshal(confStr, &conf); err != nil {
		return nil, err
	}

	client, err := sdk.New(&conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// free_blobstore_handle releases the resources associated with a BlobStoreHandle and sets it to invalid.
//
// Parameters:
//   - handle: A pointer to a BlobStoreHandle. The function will release any associated resources
//     and set the handle to BLOBSTORE_INVALID_HANDLE.
//     If '*handle' is already BLOBSTORE_INVALID_HANDLE, this function does nothing.
//
// Notes:
//   - This function must be called when the handle is no longer needed to avoid resource leaks.
//   - This function is only suitable for releasing BlobStoreHandle returned by new_blobstore_handle,
//     and is not applicable for freeing data structures allocated on the C side
//
//export free_blobstore_handle
func free_blobstore_handle(handle *C.BlobStoreHandle) {
	if handle == nil || *handle == C.BLOBSTORE_INVALID_HANDLE {
		return
	}

	h := cgo.Handle(*handle)
	wrapper, ok := h.Value().(*blobStorageWrapper)
	if ok {
		wrapper.Close()
	}

	h.Delete()
	*handle = C.BLOBSTORE_INVALID_HANDLE
}

func calcCLocSize(goLoc *access.Location) C.size_t {
	blobsLen := len(goLoc.Blobs)
	blobsSize := C.size_t(blobsLen) * C.sizeof_BlobStoreSliceInfo
	totalSize := C.size_t(C.sizeof_BlobStoreLocation) + blobsSize

	return totalSize
}

// exportGoLocation allocates memory for a C BlobStoreLocation and its Blobs array in one allocation if mem is nil,
// then copies data from the provided Go Location struct to this newly allocated or provided memory.
//
// Memory Layout:
// +--------------------------------+
// | BlobStoreLocation (Fixed Part) |
// +--------------------------------+
// | BlobStoreSliceInfo[0]          |
// | BlobStoreSliceInfo[1]          |
// | ...                            |
// | BlobStoreSliceInfo[N-1]        |
// +--------------------------------+
//
// Parameters:
//   - out_location: A pointer to a pointer (double pointer) to a BlobStoreLocation structure.
//     The function will populate this with the newly allocated or provided memory.
//   - goLocation: The Go Location struct to convert.
//   - mem: If non-nil, it should point to an already allocated memory block of sufficient size.
//     If nil, this function will allocate the required memory.
//
// Returns 0 on success, -ENOMEM if allocation fails. On failure, no memory is leaked.
func exportGoLocation(out_location **C.BlobStoreLocation, goLocation *access.Location, mem unsafe.Pointer) C.int {
	blobsLen := len(goLocation.Blobs)
	totalSize := calcCLocSize(goLocation)

	// Allocate memory if not provided
	if mem == nil {
		mem = C.malloc(totalSize)
		if mem == nil {
			return -C.int(syscall.ENOMEM)
		}
	}

	cLocation := (*C.BlobStoreLocation)(mem)

	// Set the 'blobs' pointer to point right after the BlobStoreLocation structure in memory
	cBlobs := (*C.BlobStoreSliceInfo)(unsafe.Pointer(
		uintptr(mem) + uintptr(C.sizeof_BlobStoreLocation),
	))

	// Initialize the main fields of the BlobStoreLocation struct
	cLocation.cluster_id = C.uint32_t(goLocation.ClusterID)
	cLocation.code_mode = C.BlobStoreCodeMode(goLocation.CodeMode)
	cLocation.size = C.uint64_t(goLocation.Size)
	cLocation.blob_size = C.uint32_t(goLocation.BlobSize)
	cLocation.crc = C.uint32_t(goLocation.Crc)
	cLocation.blobs_len = C.uint32_t(blobsLen)
	cLocation.blobs = cBlobs

	// Populate the blobs array
	for i := 0; i < blobsLen; i++ {
		cBlob := (*C.BlobStoreSliceInfo)(unsafe.Pointer(
			uintptr(unsafe.Pointer(cBlobs)) + uintptr(i)*uintptr(C.sizeof_BlobStoreSliceInfo),
		))
		cBlob.min_bid = C.uint64_t(goLocation.Blobs[i].MinBid)
		cBlob.vid = C.uint32_t(goLocation.Blobs[i].Vid)
		cBlob.count = C.uint32_t(goLocation.Blobs[i].Count)
	}

	// Assign the newly allocated and populated BlobStoreLocation to the output parameter
	if out_location != nil {
		*out_location = cLocation
	}

	return C.BS_ERR_OK
}

// blobstore_put uploads data to the BlobStore and returns the location and hash sum map of the uploaded data.
//
// Parameters:
//   - handle: A valid BlobStoreHandle returned by new_blobstore_handle.
//   - put_args: A pointer to a BlobStorePutArgs structure containing the upload parameters.
//   - out_location: A pointer to a BlobStoreLocation pointer where the location of the uploaded data will be stored.
//     This memory must be freed using free_blobstore_location after use.
//   - out_hash_sum_map: A pointer to a BlobStoreHashSumMap pointer where the computed hash sums will be stored.
//     This memory must be freed using free_blobstore_hash_sum_map after use.
//
// Returns:
// - On success, BS_ERR_OK is returned, and out_location and out_hash_sum_map are populated with valid pointers.
// - On failure, a negative integer representing an error code on failure.
//
// Notes:
// - The caller is responsible for freeing the resources associated with out_location and out_hash_sum_map using free_blobstore_location and free_blobstore_hash_sum_map, respectively.
// - The function assumes that the data buffer pointed to by put_args.data is accessible for the duration of the upload.
// - If an error occurs, any allocated resources for out_location and out_hash_sum_map will be cleaned up before returning the error code.
//
//export blobstore_put
func blobstore_put(handle C.BlobStoreHandle, put_args *C.BlobStorePutArgs, out_location **C.BlobStoreLocation, out_hash_sum_map **C.BlobStoreHashSumMap) C.int {
	// Validate input parameters
	if handle == C.BLOBSTORE_INVALID_HANDLE || put_args == nil || out_location == nil || put_args.data == nil {
		return -C.int(syscall.EINVAL)
	}

	// Retrieve the wrapper from the handle
	h := cgo.Handle(handle)
	bs, ok := h.Value().(*blobStorageWrapper)
	if !ok || bs == nil || bs.api == nil {
		log.Fatal("invalid handle")
		return -C.int(syscall.EINVAL)
	}

	// Wrap the C buffer as an io.Reader using ioWrapper
	wrapper, err := NewIOWrapper(unsafe.Pointer(put_args.data), int64(put_args.size), 0, ReadOnly)
	if err != nil {
		log.Fatal("iowrapper err:", err)
		return -C.int(syscall.EINVAL)
	}

	// Convert C.BlobStorePutArgs to Go's PutArgs
	args := &access.PutArgs{
		Size:   int64(put_args.size),
		Hashes: access.HashAlgorithm(put_args.hashes),
		Body:   wrapper,
	}

	// Call the Go implementation of Put
	ctx := context.Background()
	goLocation, goHashSumMap, err := bs.api.Put(ctx, args)
	if err != nil {
		return -C.int(errorToStatus(err))
	}

	// Export Go Location to C Location
	ret := exportGoLocation(out_location, &goLocation, nil)
	if ret < 0 {
		return ret
	}

	if out_hash_sum_map != nil {
		// Export Go HashSumMap to C HashSumMap
		ret = exportHashSumMap(out_hash_sum_map, goHashSumMap)
		if ret < 0 {
			// Clean up previously allocated resources
			free_blobstore_location(out_location)
			return ret
		}
	}

	return C.BS_ERR_OK
}

// exportHashSumMap converts a Go HashSumMap to a C BlobStoreHashSumMap, allocating necessary memory in one go.
//
// Parameters:
//   - out_hash_sum_map: A pointer to a pointer (double pointer) to a BlobStoreHashSumMap structure.
//     The function will allocate and populate this structure with data from the provided Go HashSumMap.
//   - goHashSumMap: The Go HashSumMap to convert.
//
// Returns:
//
//	0 on success, -ENOMEM if allocation fails. On failure, all allocated memory is freed automatically.
//
// Memory Layout:
// +------------------------------+
// | BlobStoreHashSumMap          |
// +------------------------------+
// | BlobStoreHashEntry[0]        |
// | BlobStoreHashEntry[1]        |
// | ...                          |
// | BlobStoreHashEntry[N-1]      |
// +------------------------------+
// | BlobStoreHashEntry[0].value  |
// | BlobStoreHashEntry[1].value  |
// | ...                          |
// | BlobStoreHashEntry[N-1].value|
// +------------------------------+
func exportHashSumMap(out_hash_sum_map **C.BlobStoreHashSumMap, goHashSumMap access.HashSumMap) C.int {
	// Calculate total size needed for BlobStoreHashSumMap and its entries and values
	totalEntriesSize := C.size_t(len(goHashSumMap)) * C.sizeof_BlobStoreHashEntry
	totalValuesSize := C.size_t(0)
	for _, entry := range goHashSumMap {
		totalValuesSize += C.size_t(len(entry))
	}
	totalSize := C.size_t(C.sizeof_BlobStoreHashSumMap) + totalEntriesSize + totalValuesSize

	// Allocate memory in one go
	mem := C.malloc(totalSize)
	if mem == nil {
		return -C.int(syscall.ENOMEM)
	}

	hashSumMap := (*C.BlobStoreHashSumMap)(mem)
	hashSumMap.count = C.uint8_t(len(goHashSumMap))

	// Set the 'entries' pointer to point right after the BlobStoreHashSumMap structure in memory
	entries := unsafe.Pointer(
		uintptr(mem) + uintptr(C.sizeof_BlobStoreHashSumMap),
	)

	// Set the 'values' pointer to point right after the entries array in memory
	valuesPtr := unsafe.Pointer(
		uintptr(unsafe.Pointer(entries)) + uintptr(totalEntriesSize),
	)

	// Populate entries and their values
	idx := 0
	for algorithm, entry := range goHashSumMap {
		cEntry := (*C.BlobStoreHashEntry)(unsafe.Pointer(
			uintptr(entries) +
				uintptr(idx)*uintptr(C.sizeof_BlobStoreHashEntry)),
		)

		cEntry.algorithm = C.BlobStoreHashAlgorithm(algorithm)
		cEntry.value_length = C.uint8_t(len(entry))
		cEntry.value = (*C.uint8_t)(valuesPtr)

		// Copy value data
		C.memcpy(unsafe.Pointer(cEntry.value), unsafe.Pointer(&entry[0]), C.size_t(cEntry.value_length))

		// Move the values pointer forward by the size of the current value
		valuesPtr = unsafe.Pointer(uintptr(valuesPtr) + uintptr(len(entry)))
		idx++
	}

	// Assign the newly allocated and populated BlobStoreHashSumMap to the output parameter
	hashSumMap.entries = (*C.BlobStoreHashEntry)(entries)
	*out_hash_sum_map = hashSumMap

	return C.BS_ERR_OK
}

// free_blobstore_hash_sum_map frees the memory allocated for a BlobStoreHashSumMap structure,
// including all associated BlobStoreHashEntry structures and their values.
//
// Parameters:
//   - hashSumMap: A pointer to a pointer (double pointer) to a BlobStoreHashSumMap structure.
//     The function will release the memory and set '*hashSumMap' to NULL.
//     If 'hashSumMap' or '*hashSumMap' is NULL, this function does nothing.
//
// Notes:
//   - This function is only suitable for releasing BlobStoreHashSumMap returned by blobstore_put,
//     and is not applicable for freeing data structures allocated on the C side
//
//export free_blobstore_hash_sum_map
func free_blobstore_hash_sum_map(hashSumMap **C.BlobStoreHashSumMap) {
	if hashSumMap == nil || *hashSumMap == nil {
		return
	}

	// Refer exportHashSumMap() for memory layout.
	C.free(unsafe.Pointer(*hashSumMap))
	*hashSumMap = nil
}

// free_blobstore_location frees the memory allocated for a BlobStoreLocation structure,
// including all associated BlobStoreSliceInfo blobs if present.
//
// Parameters:
//   - location: A pointer to a pointer (double pointer) to a BlobStoreLocation structure.
//     The function will release the memory and set '*location' to NULL.
//     If 'location' or '*location' is NULL, this function does nothing.
//
// Notes:
//   - This function is only suitable for releasing BlobStoreLocation returned by blobstore_put,
//     and is not applicable for freeing data structures allocated on the C side
//   - For BlobStoreLocation within BlobStoreLocations, use free_blobstore_locations() instead.
//
//export free_blobstore_location
func free_blobstore_location(location **C.BlobStoreLocation) {
	if location == nil || *location == nil {
		return
	}

	// Free the single contiguous block of memory that contains both BlobStoreLocation and its Blobs array.
	// Refer exportGoLocation() for memory layout.
	C.free(unsafe.Pointer(*location))
	*location = nil
}

func convertCLocationToGo(cLoc *C.BlobStoreLocation) (*access.Location, error) {
	if cLoc == nil {
		return nil, fmt.Errorf("nil C.BlobStoreLocation")
	}

	// Convert Blobs array
	var blobs []access.SliceInfo
	if cLoc.blobs_len > 0 && cLoc.blobs != nil {
		blobs = make([]access.SliceInfo, cLoc.blobs_len)
		for i := 0; i < int(cLoc.blobs_len); i++ {
			cBlob := (*C.BlobStoreSliceInfo)(unsafe.Pointer(
				uintptr(unsafe.Pointer(cLoc.blobs)) +
					uintptr(i)*C.sizeof_BlobStoreSliceInfo,
			))
			blobs[i] = access.SliceInfo{
				MinBid: proto.BlobID(cBlob.min_bid),
				Vid:    proto.Vid(cBlob.vid),
				Count:  uint32(cBlob.count),
			}
		}
	}

	location := &access.Location{
		ClusterID: proto.ClusterID(cLoc.cluster_id),
		CodeMode:  codemode.CodeMode(cLoc.code_mode),
		Size:      uint64(cLoc.size),
		BlobSize:  uint32(cLoc.blob_size),
		Crc:       uint32(cLoc.crc),
		Blobs:     blobs,
	}

	return location, nil
}

func convertCLocationsToGo(locs *C.BlobStoreLocations) ([]access.Location, error) {
	if locs == nil || locs.count == 0 {
		return nil, nil
	}

	locations := make([]access.Location, locs.count)

	currentPtr := uintptr(unsafe.Pointer(locs.elements))
	for i := 0; i < int(locs.count); i++ {
		cLoc := (*C.BlobStoreLocation)(unsafe.Pointer(currentPtr))
		goLoc, err := convertCLocationToGo(cLoc)
		if err != nil {
			return nil, err
		}

		locations[i] = *goLoc
		currentPtr += uintptr(calcCLocSize(goLoc))
	}

	return locations, nil
}

// blobstore_get retrieves data from the BlobStore based on the provided GetArgs.
//
// Parameters:
//   - handle: A valid BlobStoreHandle returned by new_blobstore_handle.
//   - get_args: A pointer to a BlobStoreGetArgs structure containing the get related parameters.
//
// Returns:
//   - On success, returns BS_ERR_OK (0).
//   - On failure, a negative integer representing an error code on failure.
//
// Notes:
//   - The caller must ensure that the data buffer pointed to by get_args.data is large enough to hold the requested amount of data.
//   - It's important to check the return value for errors and handle them appropriately.
//
//export blobstore_get
func blobstore_get(handle C.BlobStoreHandle, get_args *C.BlobStoreGetArgs) C.int {
	// Validate handle
	if handle == C.BLOBSTORE_INVALID_HANDLE {
		return -C.int(syscall.EINVAL)
	}

	// Retrieve the wrapper from the handle
	h := cgo.Handle(handle)
	bs, ok := h.Value().(*blobStorageWrapper)
	if !ok || bs == nil || bs.api == nil {
		return -C.int(syscall.EINVAL)
	}

	// Validate getArgs and output buffer
	if get_args == nil || get_args.data == nil {
		return -C.int(syscall.EINVAL)
	}

	// Convert C.BlobStoreLocation to Go access.Location
	goLocation, err := convertCLocationToGo(get_args.location)
	if err != nil {
		return -C.int(syscall.EINVAL)
	}

	// Wrap the C buffer as an io.Writer
	writer, err := NewIOWrapper(unsafe.Pointer(get_args.data), int64(get_args.read_size), 0, WriteOnly)
	if err != nil {
		return -C.int(errorToStatus(err))
	}

	// Prepare Go's GetArgs
	args := &access.GetArgs{
		Location: *goLocation,
		Offset:   uint64(get_args.offset),
		ReadSize: uint64(get_args.read_size),
		Writer:   writer,
	}

	// Call the Go implementation
	// Get blobs by zero copy
	ctx := context.Background()
	_, err = bs.api.Get(ctx, args)
	if err != nil {
		return -C.int(errorToStatus(err))
	}

	return C.BS_ERR_OK
}

// blobstore_delete deletes the specified Locations and returns information about any failed deletions.
//
// Parameters:
//   - handle: A BlobStoreHandle representing the BlobStore instance. Must be non-zero and valid.
//   - deleteArgs: A pointer to a BlobStoreDeleteArgs structure containing the list of Locations(<=1024) to delete.
//   - result: A pointer to a BlobStoreLocations structure that will store information about failed deletions.
//     If no failures occur, `result` is set to NULL.
//
// Returns:
//   - On success, returns BS_ERR_OK (0).
//   - On failure, a negative integer representing an error code on failure.
//
// Notes:
//   - The caller is responsible for freeing the resources allocated for `result` using `free_blobstore_locations`.
//
//export blobstore_delete
func blobstore_delete(handle C.BlobStoreHandle, delete_args *C.BlobStoreDeleteArgs, result **C.BlobStoreLocations) C.int {
	// Validate handle
	if handle == C.BLOBSTORE_INVALID_HANDLE {
		return -C.int(syscall.EINVAL)
	}

	// Retrieve wrapper from handle
	h := cgo.Handle(handle)
	bs, ok := h.Value().(*blobStorageWrapper)
	if !ok || bs == nil || bs.api == nil {
		return -C.int(syscall.EINVAL)
	}

	// Validate parameters
	if delete_args == nil || delete_args.locations == nil || result == nil {
		return -C.int(syscall.EINVAL)
	}

	// Set result to safe nil state
	*result = nil

	if delete_args.locations.elements == nil || delete_args.locations.count == 0 {
		return -C.int(syscall.EINVAL)
	}

	locations, err := convertCLocationsToGo(delete_args.locations)
	if err != nil {
		return -C.int(syscall.EINVAL)
	}

	// Prepare Go args
	args := &access.DeleteArgs{
		Locations: locations,
	}

	// Call go implementation
	ctx := context.Background()
	failedLocations, err := bs.api.Delete(ctx, args)
	if err != nil {
		return -C.int(errorToStatus(err))
	}

	return exportGoLocations(result, failedLocations)
}

// exportGoLocations exports a Go slice of access.Location into a C-allocated BlobStoreLocations.
//
// This function:
//   - Does NOT allocate memory if locs is empty. Sets *out_result to NULL and returns success.
//   - Otherwise, calculates the total size required for both the BlobStoreLocations struct and all BlobStoreLocation structs,
//     including their respective BlobStoreSliceInfo blobs arrays, then allocates this in one call to C.malloc.
//   - Each BlobStoreLocation is populated using exportGoLocation.
//
// Memory layout:
// +----------------------------+
// | BlobStoreLocations         |
// +----------------------------+
// | BlobStoreLocation[0]       |
// | ...                        |
// | BlobStoreLocation[N-1]     |
// | ...                        |
// +----------------------------+
//
// Returns BS_ERR_OK on success.
func exportGoLocations(out_result **C.BlobStoreLocations, locs []access.Location) C.int {
	count := len(locs)

	// No locations: set output to NULL and return success
	if count == 0 {
		*out_result = nil
		return C.BS_ERR_OK
	}

	// Calculate total memory needed: result struct + all BlobStoreLocation structs + blob arrays
	totalSize := C.size_t(C.sizeof_BlobStoreLocations)

	for _, goLoc := range locs {
		locSize := calcCLocSize(&goLoc)
		totalSize += locSize
	}

	// Allocate the entire memory block at once
	mem := C.malloc(totalSize)
	if mem == nil {
		return -C.int(syscall.ENOMEM)
	}

	// Initialize the main result structure
	cResult := (*C.BlobStoreLocations)(mem)
	currentPtr := uintptr(mem) + C.sizeof_BlobStoreLocations

	// Set up the header fields
	cResult.count = C.uint64_t(count)
	cResult.elements = (*C.BlobStoreLocation)(unsafe.Pointer(currentPtr))

	// Export each location into the pre-allocated memory block
	for i := 0; i < count; i++ {
		srcGoLoc := &locs[i]
		dstCLoc := (*C.BlobStoreLocation)(unsafe.Pointer(currentPtr))

		// Calculate size needed for current location and its blobs
		locSize := calcCLocSize(srcGoLoc)

		// Export Go location into the allocated memory
		if r := exportGoLocation(nil, srcGoLoc, unsafe.Pointer(dstCLoc)); r != C.BS_ERR_OK {
			C.free(mem)
			return r
		}

		// Move pointer forward for next location
		currentPtr += uintptr(locSize)
	}

	// Assign final result
	*out_result = cResult

	return C.BS_ERR_OK
}

// free_blobstore_locations frees the memory allocated for a BlobStoreLocations structure,
// including all associated BlobStoreLocation entries if present.
//
// Parameters:
//   - result: A pointer to a pointer (double pointer) to a BlobStoreLocations structure.
//     The function will release the memory and set '*result' to NULL.
//     If 'result' or '*result' is NULL, this function does nothing.
//
// Notes:
//   - This function must be called to properly release resources after using a BlobStoreLocations.
//   - This function is only suitable for releasing BlobStoreLocations returned by blobstore_delete,
//     and is not applicable for freeing data structures allocated on the C side
//
//export free_blobstore_locations
func free_blobstore_locations(result **C.BlobStoreLocations) {
	if result == nil || *result == nil {
		return
	}

	// Free the entire memory block (BlobStoreLocations + BlobStoreLocation array).
	// Refer exportGoLocations() for memory layout.
	C.free(unsafe.Pointer(*result))
	*result = nil
}

func main() {}
