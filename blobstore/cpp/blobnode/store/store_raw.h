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

#include <boost/functional/hash.hpp>
#include <boost/intrusive/list.hpp>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>

#include "blobnode/device/device.h"
#include "blobnode/proto/blobnode.pb.h"
#include "blobnode/store/chunk.h"
#include "blobnode/store/journal.h"
#include "blobnode/store/proto.h"
#include "blobnode/store/slice.h"
#include "blobnode/store/slice_allocator.h"
#include "blobnode/store/store.h"
#include "blobnode/store/superblock.h"
#include "common/status.h"
#include "common/types.h"

namespace blobstore {
namespace blobnode {

// RawStore implements Store interface for raw device access.
class RawStore final : public Store, public SliceHandler {
    SuperBlock superblock_;

    // Chunk management - all chunks currently in use
    // Note: rwlock only needed for DeleteChunk which spans co_await
    struct ChunkRegistry {
        std::unordered_map<ChunkID, ChunkHandlerPtr> chunks;
        std::unordered_map<Vuid, ChunkID> vuids;
        std::vector<ChunkHandlerPtr> pending_clean_chunks;
        seastar::rwlock lock;
    };
    ChunkRegistry chunk_registry_;

    // Available chunks pool - from recycle or newly allocated
    struct ChunkPool {
        uint32_t next_chunk_index = 0;
        FreeChunkList free_list;
    };
    ChunkPool chunk_pool_;

    // Slice meta storage - indexed by slice position
    struct SliceRegistry {
        uint32_t split_slice_num_per_array = 0;
        // Slice index stored in array, sorted by slice index incrementally
        // Layout: [0-N), [N-2N), [2N-3N) ... where N = split_slice_num_per_array
        std::array<std::vector<SliceMetaPtr>, kDefaultSliceSplitMapNum> slices;
        // Checkpoint buffer for persistence (1MB)
        Buffer checkpoint_buffer;
    };
    SliceRegistry slice_registry_;

    // Per-ChunkID semaphore to serialize chunk operations (open/update/delete)
    // This ensures operations on the same chunk are not interleaved
    std::vector<seastar::semaphore> chunk_op_limiter_;

    // Slice allocator - manages free slice indexes
    std::unique_ptr<SliceAllocator> slice_allocator_;

    // Write-ahead log manager (A/B arena pattern)
    JournalPtr log_mgr_;

    StoreConfig cfg_;
    DevicePtr dev_;
    rawStoreFormatLayout format_;

    explicit RawStore(StoreConfig cfg);
    FutureStatus<> FormatV1(Trace& t, DiskMetaInfo disk_meta) noexcept;
    FutureStatus<> UpsertSuperBlock(SuperBlockInfo superblock) noexcept;
    FutureStatus<> AcquireChunkLimit(ChunkID chunk_id) noexcept;
    void ReleaseChunkLimit(ChunkID chunk_id) noexcept;
    Status<ChunkHandlerPtr> AllocChunk() noexcept;
    void FreeChunk(ChunkHandlerPtr ch) noexcept;
    FutureStatus<> UpsertChunkMeta(ChunkMeta chunk_meta) noexcept;
    Status<ChunkHandlerPtr> GetChunk(ChunkID chunk_id) noexcept;
    Status<ChunkHandlerPtr> GetChunkByVuid(Vuid vuid) noexcept;
    void AddChunk(ChunkID chunk_id, ChunkHandlerPtr ch) noexcept;
    Status<ChunkID> GetVuid(Vuid vuid) noexcept;
    void AddVuid(Vuid Vuid, ChunkID chunk_id) noexcept;
    FutureStatus<> UpsertSliceMetaInPersistence(SliceMetaPtr sm) noexcept;
    void UpsertSliceMetaInMemory(SliceMetaPtr sm) noexcept;

   public:
    ~RawStore(){};
    static FutureStatus<StorePtr> Open(StoreConfig cfg) noexcept;

    FutureStatus<> Load(Trace& t) noexcept override;
    FutureStatus<> Format(Trace& t, DiskMetaInfo disk_meta) noexcept override;
    FutureStatus<DiskMetaInfo> LoadFormat(Trace& t) noexcept override;
    FutureStatus<> UpdateFormatInfo(Trace& t, DiskID disk_id,
                                    DiskMetaInfo disk_meta) noexcept override;
    // OpenChunk return chunk with specified Vuid and chunkID,
    // it may create new chunk when specified chunk ID not exist
    FutureStatus<ChunkHandlerPtr> OpenChunk(Trace& t, ChunkID chunk_id,
                                            uint64_t size) noexcept override;
    // GetChunkMeta return chunk meta info with specified chunkID,
    // return os.ErrNotExist when chunk not exist
    Status<ChunkMetaInfoView> GetChunkMeta(Trace& t, ChunkID chunk_id) noexcept override;
    // UpdateChunkMeta save chunk meta into persistence after chunk has been used
    FutureStatus<> UpdateChunkMeta(Trace& t, ChunkID chunk_id,
                                   ChunkMetaInfo chunk_meta) noexcept override;
    // GetVuidBind return the bind chunkID on this Vuid
    Status<ChunkID> GetVuidBind(Trace& t, Vuid Vuid) noexcept override;
    // DeleteChunk delete chunk data and meta
    FutureStatus<> DeleteChunk(Trace& t, ChunkID chunk_id) noexcept override;
    // ListChunkMetas return all chunk meta info
    Status<std::unordered_map<ChunkID, ChunkMetaInfoView>> ListChunkMetas(
        Trace& t) noexcept override;
    // ListVuidMetas return all Vuid meta info
    Status<std::unordered_map<Vuid, ChunkID>> ListVuidMetas(Trace& t) noexcept override;
    FutureStatus<> Close(Trace& t) noexcept override;

    // AllocSlice alloc new slice from available
    Status<SliceMetaPtr> AllocSlice(SliceID sid, Vuid vuid, uint32_t chunk_epoch) override;
    // UpdateSlice update slice meta info in persistence
    FutureStatus<> UpdateSlice(SliceMetaPtr slice_meta_ptr) override;
    // DeleteSlice delete slice in persistence
    FutureStatus<> DeleteSlice(SliceMetaPtr slice_meta_ptr) override;
};

}  // namespace blobnode
}  // namespace blobstore
