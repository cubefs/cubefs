#pragma once

#include <cstdint>

#include "types.h"

namespace blobstore {

constexpr size_t kMemoryAlignment = 4096;
constexpr size_t kMemoryAlignmentMask = 4095;

// 逻辑扇区大小
constexpr size_t kSectorSize = 4096;
constexpr size_t kSectorSizeMask = 4095;

constexpr FormatDiskType kFormatDiskTypeFS = "fs";
constexpr FormatDiskType kFormatDiskTypeRawDeviceV1 = "raw-device-v1";

enum class DiskStatus : uint8_t {
    Normal = 1,
    Broken = 2,
    Repairing = 3,
    Repaired = 4,
    Dropped = 5,

    Max = 6,
};

enum class NodeStatus : uint8_t {
    Normal = 1,
    Dropped = 2,

    Max = 3,
};

enum class ChunkStatus : uint8_t {
    Init = 0,
    Normal = 1,
    ReadOnly = 2,
    Release = 3,

    Max = 4,
};

enum class SliceStatus : uint8_t {
    Init = 0,
    Normal = 1,
    MarkDelete = 2,

    Max = 3,
};

}  // namespace blobstore
