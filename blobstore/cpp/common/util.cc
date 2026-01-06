#include "util.h"

#include <fmt/format.h>

namespace blobstore {

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>> org) {
    if (org.get_owner_shard() == seastar::this_shard_id()) {
        return std::move(*org);
    }
    seastar::temporary_buffer<char>* one = org.get();
    return seastar::temporary_buffer<char>(one->get_write(), one->size(),
                                           make_object_deleter(std::move(org)));
}

std::string GenerateTraceid() {
    uint64_t num = GetRandomNumber<uint64_t>(0, std::numeric_limits<uint64_t>::max());
    return fmt::format("{0:0>16x}", num);
}

uint8_t TrailingZeros64(uint64_t x) noexcept {
    if (x == 0) return 64;
    return kDeBruijn64Tab[((x & -x) * kDeBruijn64) >> (64 - 6)];
}

}  // namespace blobstore

