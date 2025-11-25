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

#include <cstring>
#include <seastar/core/seastar.hh>

#include "blobnode/proto/blobnode.pb.h"
#include "blobnode/proto/store.pb.h"
#include "blobnode/store/meta_blocker.h"
#include "common/status.h"

namespace blobstore {
namespace blobnode {

static constexpr size_t kSuperBlockMagicSize = 4;
static constexpr std::array<uint8_t, kSuperBlockMagicSize> kSuperBlockMagic{0xab, 0xcd, 0xef, 0xcc};

static constexpr std::string kSuperBlockName = "superblock";
using SuperBlocker = MetaBlocker<kSuperBlockMagicSize, kSuperBlockMagic>;

struct SuperBlock {
    SuperBlockInfo super_block_info;
    size_t block_size;

    bool IsFormatted() { return super_block_info.meta().registered(); }
    const DiskMetaInfo& DiskMeta() { return super_block_info.meta(); }

    Status<> Encode(char* b) {
        return SuperBlocker::Encode(block_size, super_block_info, b, kSuperBlockName);
    }
    Status<> Decode(const char* b) {
        return SuperBlocker::Decode(block_size, b, &super_block_info, kSuperBlockName);
    }
};

}  // namespace blobnode
}  // namespace blobstore
