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
#include "common/const.h"
#include "common/status.h"
#include "common/types.h"
#include "common/util.h"

namespace blobstore {
namespace blobnode {

constexpr std::size_t kSliceMetaMagicSize = 4;
constexpr std::array<std::uint8_t, kSliceMetaMagicSize> kSliceMetaMagic{0xaa, 0xbb, 0xcc, 0xdd};

static constexpr std::string kSliceMetaName = "slicemeta";
using SliceMetaBlocker = MetaBlocker<kSliceMetaMagicSize, kSliceMetaMagic>;

struct Slice;
using SlicePtr = seastar::lw_shared_ptr<Slice>;

struct FreeSliceItem
    : boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
};

using FreeSliceList =
    boost::intrusive::list<FreeSliceItem, boost::intrusive::constant_time_size<false>>;

struct SliceMeta;
using SliceMetaPtr = seastar::lw_shared_ptr<SliceMeta>;

struct SliceMeta {
    // TODO: need slice meta checksum
    SliceMetaInfo meta_info;
    size_t block_size;

    bool IsEmpty() {
        return meta_info.vuid() == 0 &&
               meta_info.flag() == static_cast<uint32_t>(SliceStatus::Init);
    }

    void ResetToDelete() { meta_info.set_flag(static_cast<uint32_t>(SliceStatus::MarkDelete)); }

    bool IsNormal() { return meta_info.flag() == static_cast<uint32_t>(SliceStatus::Normal); }

    Status<> Encode(char* b) {
        return SliceMetaBlocker::Encode(block_size, meta_info, b, kSliceMetaName);
    }
    Status<> Decode(const char* b) {
        return SliceMetaBlocker::Decode(block_size, b, &meta_info, kSliceMetaName);
    }
};

struct Slice {
    // sm is the same pointer to the store's slice meta to save memory cost
    SliceMetaPtr sm;
    Buffer lastSector;
};

class SliceHandler {
   public:
    virtual ~SliceHandler() = default;
    // AllocSlice alloc new slice from available
    virtual Status<SliceMetaPtr> AllocSlice(SliceID id, Vuid vuid, uint32_t chunk_epoch) = 0;
    // UpdateSlice update slice meta info in persistence
    virtual FutureStatus<> UpdateSlice(SliceMetaPtr slice_meta) = 0;
    // DeleteSlice delete slice in persistence
    virtual FutureStatus<> DeleteSlice(SliceMetaPtr slice_meta) = 0;
};

}  // namespace blobnode
}  // namespace blobstore
