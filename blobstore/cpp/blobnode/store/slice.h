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

#include <seastar/core/seastar.hh>

#include "blobnode/proto/store.pb.h"
#include "blobnode/store/meta_blocker.h"
#include "blobnode/store/proto.h"
#include "common/const.h"
#include "common/status.h"
#include "common/types.h"
#include "common/util.h"

namespace blobstore {
namespace blobnode {

// flag_size field layout: | flag (8 bits) | size (24 bits) |
// flag range: 0-255, size range: 0-16MB (2^24 - 1)
constexpr uint32_t kFlagSizeFlagShift = 24;
constexpr uint32_t kFlagSizeFlagMask = 0xFF000000;
constexpr uint32_t kFlagSizeSizeMask = 0x00FFFFFF;

constexpr std::size_t kSliceMetaMagicSize = 4;
constexpr std::array<std::uint8_t, kSliceMetaMagicSize> kSliceMetaMagic{0xaa, 0xbb, 0xcc, 0xdd};

static constexpr std::string kSliceMetaName = "slicemeta";
using SliceMetaBlocker = MetaBlocker<kSliceMetaMagicSize, kSliceMetaMagic>;

class Slice;
using SlicePtr = seastar::lw_shared_ptr<Slice>;

class Slice {
    SliceMetaInfo meta_;
    size_t meta_block_size_;

    Buffer lastSector_;

   public:
    Slice() = delete;
    explicit Slice(SliceMetaInfo meta, size_t meta_block_size)
        : meta_(meta), meta_block_size_(meta_block_size) {}

    uint64_t GetSliceID() const { return meta_.slice_id(); }
    void SetSliceID(uint64_t slice_id) { meta_.set_slice_id(slice_id); }

    SliceIndex GetIndex() const { return meta_.index(); }
    void SetIndex(SliceIndex index) { meta_.set_index(index); }

    Vuid GetVuid() const { return meta_.vuid(); }
    void SetVuid(Vuid vuid) { meta_.set_vuid(vuid); }

    uint32_t GetChunkEpoch() const { return meta_.chunk_epoch(); }
    void SetChunkEpoch(uint32_t epoch) { meta_.set_chunk_epoch(epoch); }

    uint32_t GetLastBlockCrcRaw() const { return meta_.last_block_crc_raw(); }
    void SetLastBlockCrcRaw(uint32_t crc) { meta_.set_last_block_crc_raw(crc); }

    uint32_t GetSize() const { return meta_.flag_size() & kFlagSizeSizeMask; }
    void SetSize(uint32_t size) {
        meta_.set_flag_size((meta_.flag_size() & kFlagSizeFlagMask) | (size & kFlagSizeSizeMask));
    }

    uint8_t GetFlag() const { return meta_.flag_size() >> kFlagSizeFlagShift; }
    void SetFlag(uint8_t flag) {
        meta_.set_flag_size((flag << kFlagSizeFlagShift) | (meta_.flag_size() & 0x00FFFFFF));
    }

    bool IsEmpty() { return GetVuid() == 0 && GetFlag() == +SliceStatus::Init; }
    bool IsNormal() const { return GetFlag() == +SliceStatus::Normal; }

    Status<> Encode(char* b) {
        return SliceMetaBlocker::Encode(meta_block_size_, meta_, b, kSliceMetaName);
    }
    Status<> Decode(const char* b) {
        return SliceMetaBlocker::Decode(meta_block_size_, b, &meta_, kSliceMetaName);
    }
};

}  // namespace blobnode
}  // namespace blobstore
