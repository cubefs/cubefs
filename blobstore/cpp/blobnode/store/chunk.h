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

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <unordered_map>

#include "blobnode/device/device.h"
#include "blobnode/proto/blobnode.pb.h"
#include "blobnode/store/meta_blocker.h"
#include "blobnode/store/proto.h"
#include "blobnode/store/slice.h"

namespace blobstore {
namespace blobnode {

constexpr size_t kChunkSliceBucket = 4096;
constexpr size_t kChunkMetaMagicSize = 4;
constexpr std::array<uint8_t, kChunkMetaMagicSize> kChunkMetaMagic{0xbb, 0xcc, 0xde, 0xab};

static constexpr std::string kChunkMetaName = "chunkmeta";
using ChunkMetaBlocker = MetaBlocker<kChunkMetaMagicSize, kChunkMetaMagic>;

class ChunkHandler;
using ChunkHandlerPtr = seastar::lw_shared_ptr<ChunkHandler>;

// 只读视图，减少Chunk Meta的读取拷贝开销，同时可以限制上层只读，避免修改下层结构
// 通过智能指针引用，保证存活
struct ChunkMetaInfoView {
    ChunkHandlerPtr owner;      // 保持 chunk 存活
    const ChunkMetaInfo* meta;  // 只读视图
};

struct ChunkMeta {
    ChunkMetaInfo chunk_meta_info;
    size_t block_size;

    inline uint64_t GetChunkSize() const { return chunk_meta_info.chunk_size(); }
    inline void SetChunkSize(uint64_t size) { return chunk_meta_info.set_chunk_size(size); }

    Status<> Encode(char* b) {
        return ChunkMetaBlocker::Encode(block_size, chunk_meta_info, b, kChunkMetaName);
    }
    Status<> Decode(const char* b) {
        return ChunkMetaBlocker::Decode(block_size, b, &chunk_meta_info, kChunkMetaName);
    }
};

struct ChunkConfig {
    uint32_t format_slice_size;
    uint32_t format_block_size;

    Device* device;

    std::function<void(ChunkIndex)> cb_chunk_free;

    std::function<Status<SlicePtr>(SliceID, Vuid, uint32_t)> cb_slice_alloc;
    std::function<FutureStatus<>(SlicePtr)> cb_slice_update;
    std::function<FutureStatus<>(SlicePtr)> cb_slice_delete;
};

// ChunkHandler manages slices within a chunk.
class ChunkHandler {
    ChunkConfig config_;
    ChunkMeta chunk_meta_;

    //  Slice storage using bucket-based sharding for O(1) access
    std::array<std::unordered_map<SliceID, SlicePtr>, kChunkSliceBucket> slices_;
    auto& SliceBucket(SliceID id) noexcept {
        const auto idx = id % kChunkSliceBucket;
        return slices_[idx];
    }

   public:
    ChunkHandler() = delete;
    ChunkHandler(const ChunkHandler&) = delete;
    ChunkHandler(ChunkHandler&&) = delete;
    ChunkHandler& operator=(const ChunkHandler&) = delete;
    ChunkHandler& operator=(ChunkHandler&&) = delete;

    explicit ChunkHandler(ChunkConfig&& cfg, ChunkMeta&& meta);
    ~ChunkHandler();
    static ChunkHandlerPtr Create(ChunkConfig&& cfg, ChunkMeta&& meta);

    // UpdateMetaInfo update chunk meta info in memory
    void UpdateMeta(ChunkMeta meta) noexcept { chunk_meta_ = meta; }
    ChunkMeta GetMeta() noexcept { return chunk_meta_; }
    const ChunkMetaInfo& GetChunkMetaInfo() noexcept { return chunk_meta_.chunk_meta_info; };

    // GetAllSlices get all slices
    std::vector<SlicePtr> GetAllSlices() noexcept;
    Status<SlicePtr> GetSlice(SliceID id) noexcept;
    Status<SlicePtr> AllocSlice(SliceID id) noexcept;
    void AddSlice(SlicePtr slice) noexcept;
    FutureStatus<> DelSlice(SliceID id) noexcept;
};

}  // namespace blobnode
}  // namespace blobstore
