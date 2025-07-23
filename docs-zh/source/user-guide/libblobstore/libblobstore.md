# BlobStore C SDK 使用说明

## 接口详细说明

参见 [BlobStore C 接口说明](../../../../blobstore/libsdk/libblobstore.h)

## 例子

### 前提

假如 libblobstore 安装路径如下：

```text
/usr/include/blobstore
/usr/include/blobstore/blobstore_errno.h
/usr/include/blobstore/blobstore_types.h
/usr/include/blobstore/libblobstore.h
...
/usr/lib64/libblobstore.so
```

### 源码

```c
#include <stdio.h>
#include <stdlib.h>
#include <blobstore/libblobstore.h>

void print_location(const BlobStoreLocation *loc)
{
    if (!loc) {
        printf("location is null\n");
        return;
    }

    printf("=== location info ===\n");
    printf("cluster_id: %u\n", loc->cluster_id);
    printf("code_mode: %u\n", loc->code_mode);
    printf("size: %lu bytes\n", loc->size);
    printf("blob_size: %u\n", loc->blob_size);
    printf("crc: 0x%x\n", loc->crc);
    printf("blobs count: %u\n", loc->blobs_len);

    if (loc->blobs && loc->blobs_len > 0) {
        for (size_t i = 0; i < loc->blobs_len; ++i)
        {
            const BlobStoreSliceInfo *blob = &loc->blobs[i];
            printf("  blob[%zu]: min_bid=%lu, vid=%u, count=%u\n",
                   i, blob->min_bid, blob->vid, blob->count);
        }
    } else {
        printf("no blob info available.\n");
    }
    printf("=====================\n");
}

const char *hash_algorithm_to_string(BlobStoreHashAlgorithm algo)
{
    switch (algo) {
        case BLOBSTORE_HASH_ALG_DUMMY: return "Dummy";
        case BLOBSTORE_HASH_ALG_CRC32: return "CRC32";
        case BLOBSTORE_HASH_ALG_MD5: return "MD5";
        case BLOBSTORE_HASH_ALG_SHA1: return "SHA1";
        case BLOBSTORE_HASH_ALG_SHA256: return "SHA256";
        default: return "Unknown Algorithm";
    }
}

void print_hash_sum_map(const BlobStoreHashSumMap *map)
{
    if (!map || map->count == 0 || !map->entries) {
        printf("hash_sum_map is empty or invalid.\n");
        return;
    }

    printf("=== hash_sum_map info ===\n");
    printf("hash count: %u\n", map->count);
    for (uint8_t i = 0; i < map->count; ++i) {
        BlobStoreHashEntry entry = map->entries[i];
        printf("hash[%d]: %s - value: ", i, hash_algorithm_to_string(entry.algorithm));

        for (uint8_t j = 0; j < entry.value_length; ++j) {
            printf("%02x", entry.value[j]);
        }
        printf("\n");
    }
    printf("=====================\n");
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        printf("Usage: %s <config_file_path>\n", argv[0]);
        return -1;
    }

    char *config_path = argv[1];
    int error_code;
    BlobStoreHandle handle = new_blobstore_handle(config_path, &error_code);
    if (handle == BLOBSTORE_INVALID_HANDLE || error_code != BS_ERR_OK) {
        printf("failed to create blobstore_handle. error code: %d\n", error_code);
        return -1;
    }

    char data[] = "hello, cubeFS/blobstore!";
    BlobStorePutArgs put_args = {
        .size = sizeof(data),
        .hashes = BLOBSTORE_HASH_ALG_MD5 | BLOBSTORE_HASH_ALG_SHA256,
        .data = data,
        .hint = 0};
    BlobStoreLocation *location = NULL;
    BlobStoreHashSumMap *hash_sum_map = NULL;

    int put_result = blobstore_put(handle, &put_args, &location, &hash_sum_map);
    if (put_result != BS_ERR_OK) {
        printf("put operation failed with error code: %d\n", put_result);
        free_blobstore_handle(&handle);
        return -1;
    }
    printf("put operation succeeded: data = %s\n", data);
    print_location(location);

    if (hash_sum_map != NULL) {
        print_hash_sum_map(hash_sum_map);
    } else {
        printf("hash_sum_map is null\n");
    }

    char read_buffer[location->size];
    BlobStoreGetArgs get_args = {
        .location = location,
        .offset = 0,
        .read_size = location->size,
        .data = read_buffer,
        .hint = 0};

    int get_result = blobstore_get(handle, &get_args);
    if (get_result == BS_ERR_OK) {
        printf("read data: %s\n", (char *)get_args.data);
    } else {
        printf("get operation failed with error code: %d\n", get_result);
    }

    BlobStoreLocations locs = {
        .count = 1,
        .elements = location,
    };
    BlobStoreDeleteArgs delete_args = {
        .locations = &locs,
        .hint = 0};
    BlobStoreLocations *delete_result = NULL;
    int delete_result_code = blobstore_delete(handle, &delete_args, &delete_result);
    if (delete_result_code == BS_ERR_OK) {
        if (delete_result && delete_result->count > 0) {
            printf("some deletions failed.\n");
        } else {
            printf("all deletions succeeded.\n");
        }

        if (delete_result && delete_result->elements != NULL) {
            free_blobstore_locations(&delete_result);
        }
    } else {
        printf("delete operation failed with error code: %d\n", delete_result_code);
    }

    if (location != NULL) {
        free_blobstore_location(&location);
    }
    if (hash_sum_map != NULL) {
        free_blobstore_hash_sum_map(&hash_sum_map);
    }
    free_blobstore_handle(&handle);

    return 0;
}
```

### 编译执行

编译例子程序

```shell
$ gcc -o e example.c -I. -L. -lblobstore
```

执行二进制程序

```shell
$ ./e blob.conf
put operation succeeded: data = hello, cubeFS/blobstore!
=== location info ===
cluster_id: 1
code_mode: 11
size: 25 bytes
blob_size: 4194304
crc: 0xe69118cf
blobs count: 1
  blob[0]: min_bid=40134, vid=2, count=1
=====================
=== hash_sum_map info ===
hash count: 2
hash[0]: SHA256 - value: 687b899aaa76b05547a0ea834e70ab38351ad463f2d0bd7d38dd163948135d85
hash[1]: MD5 - value: 54ad1563ba954ac668f3d463c7336a17
=====================
read data: hello, cubeFS/blobstore!
all deletions succeeded.
```

blob.conf

```json
{
        "log_level": 4,
        "idc": "z0",
        "code_mode_put_quorums": {
                "11": 4
        },
        "cluster_config": {
                "clusters": [{"cluster_id": 1, "hosts": ["http://127.0.0.1:9998"]}]
        }
}
```