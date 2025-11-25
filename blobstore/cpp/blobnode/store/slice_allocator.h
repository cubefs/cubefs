#pragma once

#include <array>
#include <vector>

#include "blobnode/store/proto.h"
#include "blobnode/store/slice.h"

namespace blobstore {
namespace blobnode {

constexpr uint32_t kDefaultFreeSliceSplitMapNum = 4096;

static constexpr uint64_t kDeBruijn64 = 0x03f79d71b4ca8b09ULL;
static constexpr uint8_t kDeBruijn64Tab[64] = {
    0,  1,  56, 2,  57, 49, 28, 3,  61, 58, 42, 50, 38, 29, 17, 4,  62, 47, 59, 36, 45, 43,
    51, 22, 53, 39, 33, 30, 24, 18, 12, 5,  63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21,
    52, 32, 23, 11, 54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9,  13, 8,  7,  6,
};

int TrailingZeros64(uint64_t x) noexcept;

// SliceAllocator manages allocation and deallocation of slice indexes.
class SliceAllocator {
    uint32_t robin_count_ = 0;                // Round-robin counter for shard selection
    SliceIndex current_slice_index_ = 0;      // Next unused slice index
    SliceIndex max_slice_index_ = 0;          // Maximum valid slice index
    uint32_t split_slice_num_per_array_ = 0;  // Slices per shard

    struct allocator {
        struct free_slice_index {
            uint64_t cell = 0;
            FreeSliceItem e;
        };

        FreeSliceList list;
        // Free slice indexes stored in array, layout:
        // Shard 0: [0-N)                [0-64) [64-128) ...
        // Shard 1: [N-2N)               [N-N+64) [N+64-N+128) ...
        // where N = split_slice_num_per_array_
        std::vector<free_slice_index> indexes;
    };

    std::array<allocator, kDefaultFreeSliceSplitMapNum> frees_;

   public:
    explicit SliceAllocator(SliceIndex max_slice_index);
    Status<SliceIndex> Alloc() noexcept;
    Status<> Free(SliceIndex si) noexcept;
    SliceIndex CurrentSliceIndex() noexcept;
    void ResetCurrentSliceIndex(SliceIndex current_slice_index);
};

}  // namespace blobnode
}  // namespace blobstore
