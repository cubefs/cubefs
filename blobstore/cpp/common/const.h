#pragma once

namespace blobstore {

constexpr size_t kMemoryAlignment = 4096;
constexpr size_t kMemoryAlignmentMask = 4095;

// 逻辑扇区大小
constexpr size_t kSectorSize = 4096;
constexpr size_t kSectorSizeMask = 4095;

}  // namespace blobstore
