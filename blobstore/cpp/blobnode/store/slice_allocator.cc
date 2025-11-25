#include "slice_allocator.h"

#include <cstdint>

namespace blobstore {
namespace blobnode {

SliceAllocator::SliceAllocator(SliceIndex max_slice_index) : max_slice_index_(max_slice_index) {}

Status<SliceIndex> SliceAllocator::Alloc() noexcept { // TODO
    Status<SliceIndex> s;
    return s;
}

Status<> SliceAllocator::Free(SliceIndex si) noexcept { // TODO
    Status<> s;
    return s;
}

SliceIndex SliceAllocator::CurrentSliceIndex() noexcept { return current_slice_index_; }

void SliceAllocator::ResetCurrentSliceIndex(SliceIndex current_slice_index) {
    if (current_slice_index > 0) {
        current_slice_index_ = current_slice_index + 1;
    }
}

int TrailingZeros64(uint64_t x) noexcept {
    if (x == 0) return 64;
    return kDeBruijn64Tab[((x & -x) * kDeBruijn64) >> (64 - 6)];
}

}  // namespace blobnode
}  // namespace blobstore
