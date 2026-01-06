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

#include "blobnode/proto/blobnode.pb.h"
#include "blobnode/store/chunk.h"
#include "common/status.h"
#include "common/trace.h"
#include "common/types.h"

namespace blobstore {
namespace blobnode {

constexpr size_t kDefaultChunkSemaCount = 256;
constexpr uint32_t kDefaultSliceSplitMapNum = 4096;

class Store;
using StorePtr = std::unique_ptr<Store>;

class Store {
   public:
    virtual ~Store(){};

    virtual FutureStatus<> Load(Trace& t) noexcept = 0;
    virtual FutureStatus<> Format(Trace& t, DiskMetaInfo disk_meta) noexcept = 0;
    virtual FutureStatus<> UpdateDiskMeta(Trace& t, DiskMetaInfo disk_meta) noexcept = 0;
    virtual FutureStatus<DiskMetaInfo> GetDiskMeta(Trace& t) noexcept = 0;

    // OpenChunk return chunk with specified vuid and chunkID,
    // it may create new chunk when specified chunkID not exist
    virtual FutureStatus<ChunkHandlerPtr> OpenChunk(Trace& t, ChunkID chunk_id,
                                                    uint64_t size) noexcept = 0;
    // GetChunkMeta return chunk meta info with specified chunkID,
    // return os.ErrNotExist when chunk not exist
    virtual Status<ChunkMetaInfoView> GetChunkMeta(Trace& t, ChunkID chunk_id) noexcept = 0;
    // UpdateChunkMeta save chunk meta into persistence after chunk has been used
    virtual FutureStatus<> UpdateChunkMeta(Trace& t, ChunkID chunk_id,
                                           ChunkMetaInfo chunk_meta) noexcept = 0;
    // GetVuidBind return the bind chunkID on this vuid
    virtual Status<ChunkID> GetVuidBind(Trace& t, Vuid vuid) noexcept = 0;
    // DeleteChunk delete chunk data and meta
    virtual FutureStatus<> DeleteChunk(Trace& t, ChunkID chunk_id) noexcept = 0;
    // ListChunkMetas return all chunk meta info
    virtual Status<std::unordered_map<ChunkID, ChunkMetaInfoView>> ListChunkMetas(
        Trace& t) noexcept = 0;
    // ListVuidMetas return all vuid meta info
    virtual Status<std::unordered_map<Vuid, ChunkID>> ListVuidMetas(Trace& t) noexcept = 0;
    virtual FutureStatus<> Close(Trace& t) noexcept = 0;
};

}  // namespace blobnode
}  // namespace blobstore
