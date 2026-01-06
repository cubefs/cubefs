#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <memory>
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

static constexpr uint64_t kDeBruijn64 = 0x03f79d71b4ca8b09ULL;
static constexpr uint8_t kDeBruijn64Tab[64] = {
    0,  1,  56, 2,  57, 49, 28, 3,  61, 58, 42, 50, 38, 29, 17, 4,  62, 47, 59, 36, 45, 43,
    51, 22, 53, 39, 33, 30, 24, 18, 12, 5,  63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21,
    52, 32, 23, 11, 54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9,  13, 8,  7,  6,
};

uint8_t TrailingZeros64(uint64_t x) noexcept;

template <typename T>
inline constexpr uint8_t Log2PowerOf2(T n) {
    if constexpr (sizeof(T) == 8) {
        return kDeBruijn64Tab[((n & -n) * kDeBruijn64) >> 58];
    }
    // Simple shift count for smaller types
    uint8_t log2 = 0;
    T t = n;
    while (t > 1) {
        t >>= 1;
        log2++;
    }
    return log2;
}

// Calculate ceiling division: ceil(a / b) = (a + b - 1) / b
template <typename T>
inline constexpr T CeilDiv(T dividend, T divisor) {
    if (divisor == 0) {
        return 0;
    }
    if ((divisor & (divisor - 1)) == 0) {
        return (dividend + (divisor - 1)) >> Log2PowerOf2(divisor);
    }
    return (dividend + (divisor - 1)) / divisor;
}

constexpr size_t NextPowerOf2(size_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if constexpr (sizeof(size_t) > 4) {
        n |= n >> 32;
    }
    return n + 1;
}

}  // namespace blobstore
