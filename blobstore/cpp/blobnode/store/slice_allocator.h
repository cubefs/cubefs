#pragma once

#include <vector>

#include "blobnode/store/proto.h"
#include "common/ring_queque.h"
#include "common/status.h"

namespace blobstore {
namespace blobnode {

// SliceAllocator manages allocation and deallocation of slice indexes.
class SliceAllocator {
    SliceIndex max_index_ = 0;           // Maximum valid slice index
    std::vector<uint64_t> free_bitmap_;  // free slice, bit == 1 means free
    RingQueue<SliceIndex> free_queue_;   // Store all slices

    // Helper methods for bitmap operations
    void BitFree(SliceIndex index) noexcept {
        size_t word_idx = index >> 6;  // 2^6 = 64
        size_t bit_idx = index & 63;
        free_bitmap_[word_idx] |= (1ull << bit_idx);
    }
    void BitAllocated(SliceIndex index) noexcept {
        size_t word_idx = index >> 6;
        size_t bit_idx = index & 63;
        free_bitmap_[word_idx] &= ~(1ull << bit_idx);
    }
    bool BitIsFree(SliceIndex index) const noexcept {
        size_t word_idx = index >> 6;
        size_t bit_idx = index & 63;
        return (free_bitmap_[word_idx] & (1ull << bit_idx)) != 0;
    }

   public:
    explicit SliceAllocator(SliceIndex total, bool free = false) noexcept;
    Status<SliceIndex> Alloc() noexcept;
    Status<> Free(SliceIndex index) noexcept;
};

}  // namespace blobnode
}  // namespace blobstore
