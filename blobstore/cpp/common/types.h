#pragma once

#include <array>
#include <chrono>
#include <compare>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

#include "common/byteorder.h"

namespace blobstore {

using NodeID = uint32_t;
using DiskID = uint32_t;
using Vid = uint32_t;
using Vuid = uint64_t;
using SliceID = uint64_t;

using FormatDiskType = std::string_view;

// ChunkID layout: 16 bytes
// - bytes 0-7:  vuid (big endian)
// - bytes 8-15: timestamp in nanoseconds (big endian)
struct ChunkID {
    static constexpr size_t kLength = 16;
    static constexpr size_t kVuidLen = 8;
    static constexpr size_t kTimestampLen = 8;
    static constexpr size_t kEncodeLen = 33;  // 16 hex + 1 delimiter + 16 hex

    std::array<uint8_t, kLength> data{};

    // Create a new ChunkID from vuid with current timestamp
    static ChunkID New(Vuid vuid) {
        ChunkID id;
        auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        BigEndian::PutUint64(reinterpret_cast<char*>(id.data.data()), vuid);
        BigEndian::PutUint64(reinterpret_cast<char*>(id.data.data() + kVuidLen),
                             static_cast<uint64_t>(now));
        return id;
    }

    // Get vuid from ChunkID
    Vuid GetVuid() const { return BigEndian::Uint64(reinterpret_cast<const char*>(data.data())); }

    // Get timestamp from ChunkID
    uint64_t GetTimestamp() const {
        return BigEndian::Uint64(reinterpret_cast<const char*>(data.data() + kVuidLen));
    }

    std::string String() const {
        char buf[kEncodeLen + 1];
        std::snprintf(buf, sizeof(buf), "%016llx-%016llx",
                      static_cast<unsigned long long>(GetVuid()),
                      static_cast<unsigned long long>(GetTimestamp()));
        return std::string(buf);
    }

    bool From(std::string_view str) {
        if (str.size() != kEncodeLen || str[16] != '-') {
            return false;
        }

        unsigned long long vuid_val = 0, ts_val = 0;
        if (std::sscanf(str.data(), "%16llx-%16llx", &vuid_val, &ts_val) != 2) {
            return false;
        }

        BigEndian::PutUint64(reinterpret_cast<char*>(data.data()), vuid_val);
        BigEndian::PutUint64(reinterpret_cast<char*>(data.data() + kVuidLen), ts_val);
        return true;
    }

    // Use default comparison operators (C++20)
    auto operator<=>(const ChunkID&) const = default;
    bool operator==(const ChunkID&) const = default;
};

}  // namespace blobstore

// std::hash specialization for ChunkID
namespace std {
template <>
struct hash<blobstore::ChunkID> {
    size_t operator()(const blobstore::ChunkID& id) const {
        size_t h = 0;
        for (auto b : id.data) {
            h ^= std::hash<uint8_t>{}(b) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        return h;
    }
};
}  // namespace std
