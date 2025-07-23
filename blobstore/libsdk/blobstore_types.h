#ifndef BLOBSTORE_TYPES_H
#define BLOBSTORE_TYPES_H

#include <stdint.h> // for uintptr_t

// ----------------------------------------------------------------------------
// HashAlgorithm Macro Definition (Bit Flags)
// ----------------------------------------------------------------------------
// BlobStoreHashAlgorithm represents the hash algorithms that can be used.
// Each algorithm is defined as a bit flag, allowing multiple algorithms to be combined.

typedef uint8_t BlobStoreHashAlgorithm;

#define BLOBSTORE_HASH_ALG_DUMMY ((BlobStoreHashAlgorithm)(1 << 0))
#define BLOBSTORE_HASH_ALG_CRC32 ((BlobStoreHashAlgorithm)(1 << 1))
#define BLOBSTORE_HASH_ALG_MD5 ((BlobStoreHashAlgorithm)(1 << 2))
#define BLOBSTORE_HASH_ALG_SHA1 ((BlobStoreHashAlgorithm)(1 << 3))
#define BLOBSTORE_HASH_ALG_SHA256 ((BlobStoreHashAlgorithm)(1 << 4))

// Hash entry structure
typedef struct
{
    BlobStoreHashAlgorithm algorithm; // Algorithm used to generate the hash
    uint8_t *value;                   // Pointer to the hash value string
    uint8_t value_length;             // Length of the hash value
} BlobStoreHashEntry;

// Container for hash values and their algorithms
typedef struct
{
    uint8_t count;               // Number of hash entries
    BlobStoreHashEntry *entries; // Array of hash entries
} BlobStoreHashSumMap;

// Handle of BlobStore instance
typedef uintptr_t BlobStoreHandle;
#define BLOBSTORE_INVALID_HANDLE ((BlobStoreHandle)0)

// SliceInfo blobs info (16 bytes)
// blob ids = [min_bid, min_bid+count)
typedef struct
{
    uint64_t min_bid; // First blob ID
    uint32_t vid;     // Volume ID where all blobs are located
    uint32_t count;   // Number of consecutive blob IDs
} BlobStoreSliceInfo;

typedef uint8_t BlobStoreCodeMode;

typedef uint64_t BlobStorePutHint;

// Location file location
typedef struct
{
    uint64_t size;               // File size
    uint32_t cluster_id;         // ClusterID which cluster file is in
    uint32_t blob_size;          // Size of each blob (except possibly the last one)
    uint32_t crc;                // Checksum; change anything of the location, crc will mismatch

    // Blobs: array of blob information
    uint32_t blobs_len;        // Array length
    BlobStoreSliceInfo *blobs; // Pointer to Blob information array

    BlobStoreCodeMode code_mode; // Encoding mode
} BlobStoreLocation;

// PutArgs for service /put
typedef struct
{
    int64_t size;                  // Size of data
    const void *data;              // Pointer to the buffer
    BlobStorePutHint hint;         // Put hint tags
    BlobStoreHashAlgorithm hashes; // Bitmask of hash algorithms
} BlobStorePutArgs;

typedef uint64_t BlobStoreGetHint;

// GetArgs for service /get
typedef struct
{
    BlobStoreLocation *location; // Location of the data to retrieve
    uint64_t offset;            // Offset in bytes from the start of the data
    uint64_t read_size;         // Number of bytes to read (0 means read until the end)
    void *data;                 // Pointer to the buffer where the retrieved data will be written
    BlobStoreGetHint hint;      // Reserved, set to 0.
} BlobStoreGetArgs;

typedef uint64_t BlobStoreDeleteHint;

typedef struct {
    uint64_t count;              // Number of elements
    BlobStoreLocation *elements; // Pointer to array of locations
} BlobStoreLocations;

// DeleteArgs for service /delete
typedef struct
{
    BlobStoreLocations *locations; // Locations to delete
    BlobStoreDeleteHint hint;     // Reserved, set to 0.
} BlobStoreDeleteArgs;

#endif // BLOBSTORE_TYPES_H
