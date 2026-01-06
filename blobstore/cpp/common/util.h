#pragma once

#include <fmt/format.h>

#include <memory>
#include <ostream>
#include <random>
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>

namespace blobstore {

using Buffer = seastar::temporary_buffer<char>;

// Create an aligned temporary buffer
template <size_t Alignment>
Buffer AlignedBuffer(size_t size) {
    return seastar::temporary_buffer<char>::aligned(Alignment, size);
}

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>> org);

std::string GenerateTraceid();

// get a random number from range [start, end]
template <typename T>
T GetRandomNumber(T start, T end) {
    static thread_local std::random_device rd;
    static thread_local std::default_random_engine re(rd());

    std::uniform_int_distribution<T> u(start, end);
    return u(re);
}

}  // namespace blobstore
