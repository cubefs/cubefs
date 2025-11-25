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

namespace blobstore {
namespace blobnode {

ChunkHandler::ChunkHandler(const ChunkConfig& cfg)
    : chunk_meta_(std::move(cfg.meta)),  // TODO: move ??
      format_slice_size_(cfg.format_slice_size),
      format_block_size_(cfg.format_block_size),
      sliceHandler_(cfg.slice_handler),
      device_(cfg.device){};

ChunkHandlerPtr ChunkHandler::Create(const ChunkConfig& cfg) {
    ChunkHandlerPtr handler = seastar::make_lw_shared<ChunkHandler>(cfg);
    return std::move(handler);
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
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: slice not found");
        return s;
    }
    s.SetValue(it->second);
    return s;
}

void ChunkHandler::AddSlice(SliceMetaPtr sm) noexcept {
    SliceID id = sm->meta_info.slice_id();
    auto& m = SliceBucket(id);
    if (m.find(id) == m.end()) {
        auto slice = seastar::make_lw_shared<Slice>();
        slice->sm = std::move(sm);
        m.emplace(id, std::move(slice));
        chunk_meta_.chunk_meta_info.set_chunk_size(chunk_meta_.chunk_meta_info.chunk_size() +
                                                   format_slice_size_);
    }
}

void ChunkHandler::DelSlice(SliceID id) noexcept {
    auto& m = SliceBucket(id);
    auto it = m.find(id);
    if (it != m.end()) {
        m.erase(it);
        chunk_meta_.chunk_meta_info.set_chunk_size(chunk_meta_.chunk_meta_info.chunk_size() -
                                                   format_slice_size_);
    }
}

Status<SlicePtr> ChunkHandler::AllocSlice(SliceID id) noexcept {
    Status<SlicePtr> s;
    auto& m = SliceBucket(id);
    if (auto it = m.find(id); it != m.end()) {
        return s.SetValue(it->second);
    }

    auto rs = sliceHandler_->AllocSlice(id, chunk_meta_.chunk_meta_info.vuid(),
                                        chunk_meta_.chunk_meta_info.epoch());
    if (!rs.OK()) {
        s.SetCode(rs.Code()).SetReason(rs.Reason());
        return s;
    }

    auto slice = seastar::make_lw_shared<Slice>();
    slice->sm = rs.Value();
    m[id] = slice;

    s.SetValue(std::move(slice));
    return s;
}

}  // namespace blobnode
}  // namespace blobstore
