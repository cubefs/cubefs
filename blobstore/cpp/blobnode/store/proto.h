// Copyright 2025 The CubeFS Authors.
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

#pragma once

#include <boost/intrusive/list.hpp>
#include <cstdint>
#include <seastar/core/seastar.hh>

namespace blobstore {
namespace blobnode {

using ChunkIndex = uint32_t;
using SliceIndex = uint32_t;
using LogHeaderVer = uint64_t;

constexpr LogHeaderVer kInitLogHeaderVer = 1;
// constexpr uint32_t kDeviceSectorSize = 512;
constexpr uint32_t kSliceMetaSize = 32 + 32;

enum class LogHeaderFlag : uint8_t {
    UnCheckpoint = 0,
    CheckpointDone = 1,

    Max = 2,
};

enum class LogRecordType : uint8_t {
    SliceMeta = 0,

    Max = 1,
};

enum class DeviceDriverType : uint8_t {
    Kernel = 1,
    Spdk = 2,
};

struct StoreConfig {
    seastar::sstring device;
    uint32_t ns_id;
    uint32_t qpair_n;
    DeviceDriverType dev_driver_type;
};

// layout
struct rawStoreFormatLayout {
    uint64_t start_offset = 0;
    uint64_t super_block_size = 0;
    uint64_t log_arena_size = 0;
    uint64_t log_header_size = 0;
    uint64_t log_record_size = 0;
    uint64_t chunk_arena_size = 0;
    uint64_t chunk_meta_size = 0;
    uint64_t slice_meta_size = 0;
    uint64_t slice_size = 0;
    uint64_t block_size = 0;
};

// v1 layout (sizes kept same as Go)
constexpr rawStoreFormatLayout rawStoreFormatLayoutV1{
    .start_offset = 0,
    .super_block_size = (4ull << 20),
    // 4K log header + 64MB log record arena
    .log_arena_size = (64ull << 20),
    .log_header_size = (4ull << 10),
    .log_record_size = (4ull << 10),
    .chunk_arena_size = (16ull << 30),
    .chunk_meta_size = (4ull << 10),
    .slice_meta_size = 512ull,
    // every block(32KB-4) with 4 byte crc
    .slice_size = (4ull << 20),
    .block_size = (32ull << 10),
};

}  // namespace blobnode
}  // namespace blobstore
