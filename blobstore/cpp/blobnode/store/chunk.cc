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

#include "chunk.h"

#include <seastar/core/coroutine.hh>

namespace blobstore {
namespace blobnode {

ChunkHandler::ChunkHandler(ChunkConfig&& cfg, ChunkMeta&& meta)
    : config_(std::move(cfg)), chunk_meta_(std::move(meta)){};

ChunkHandler::~ChunkHandler() {
    if (config_.cb_chunk_free) {
        config_.cb_chunk_free(chunk_meta_.chunk_meta_info.index());
    }
}

ChunkHandlerPtr ChunkHandler::Create(ChunkConfig&& cfg, ChunkMeta meta) {
    return seastar::make_lw_shared<ChunkHandler>(std::move(cfg), std::move(meta));
}

std::vector<SlicePtr> ChunkHandler::GetAllSlices() noexcept {
    std::vector<SlicePtr> v;
    v.reserve(kChunkSliceBucket);
    for (std::size_t i = 0; i < kChunkSliceBucket; ++i) {
        for (auto& kv : slices_[i]) {
            v.push_back(kv.second);
        }
    }
    return v;
}

Status<SlicePtr> ChunkHandler::GetSlice(SliceID id) noexcept {
    Status<SlicePtr> s;
    auto& m = SliceBucket(id);
    auto it = m.find(id);
    if (it == m.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: slice not found in chunk");
        return s;
    }
    s.SetValue(it->second);
    return s;
}

void ChunkHandler::AddSlice(SlicePtr slice) noexcept {
    SliceID id = slice->GetSliceID();
    auto& m = SliceBucket(id);
    if (m.find(id) == m.end()) {
        m.emplace(id, slice);

        auto size = chunk_meta_.GetChunkSize();
        if (size > config_.format_slice_size) {
            chunk_meta_.SetChunkSize(size - config_.format_slice_size);
        }
    }
}

Status<SlicePtr> ChunkHandler::AllocSlice(SliceID id) noexcept {
    Status<SlicePtr> s;
    auto& m = SliceBucket(id);
    if (auto it = m.find(id); it != m.end()) {
        return s.SetValue(it->second);
    }

    auto rs = config_.cb_slice_alloc(id, chunk_meta_.chunk_meta_info.vuid(),
                                     chunk_meta_.chunk_meta_info.epoch());
    if (!rs) {
        s.SetCode(rs.Code()).SetReason(rs.Reason());
        return s;
    }
    auto slice = rs.Value();
    m[id] = slice;

    s.SetValue(std::move(slice));
    return s;
}

FutureStatus<> ChunkHandler::DelSlice(SliceID id) noexcept {
    Status<> s;
    auto& m = SliceBucket(id);
    auto it = m.find(id);
    if (it != m.end()) {
        SlicePtr slice = it->second;
        m.erase(id);
        chunk_meta_.SetChunkSize(chunk_meta_.GetChunkSize() - config_.format_slice_size);

        s = co_await config_.cb_slice_delete(slice);
    }
    co_return s;
}

}  // namespace blobnode
}  // namespace blobstore
