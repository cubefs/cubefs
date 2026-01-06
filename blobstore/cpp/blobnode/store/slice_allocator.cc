#include "slice_allocator.h"

#include "common/util.h"

namespace blobstore {
namespace blobnode {

SliceAllocator::SliceAllocator(SliceIndex total, bool free) noexcept : free_queue_(total) {
    assert(total > 0);
    max_index_ = total - 1;
    size_t words = CeilDiv<uint32_t>(total, 64);
    free_bitmap_.resize(words, 0);

    if (free) {
        for (SliceIndex ii = 0; ii < total; ++ii) {
            BitFree(ii);
            free_queue_.Push(ii);
        }
    }
}

Status<SliceIndex> SliceAllocator::Alloc() noexcept {
    Status<SliceIndex> s;
    SliceIndex index;
    if (!free_queue_.Pop(index)) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("store: allocator no available slice");
        return s;
    }
    BitAllocated(index);
    s.SetValue(index);
    return s;
}

Status<> SliceAllocator::Free(SliceIndex index) noexcept {
    Status<> s;
    if (index > max_index_) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("store: allocator index out of range");
        return s;
    }
    if (BitIsFree(index)) {
        return s;
    }
    BitFree(index);
    free_queue_.Push(index);
    return s;
}

}  // namespace blobnode
}  // namespace blobstore
