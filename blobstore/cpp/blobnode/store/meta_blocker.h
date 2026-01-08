#pragma once

#include <google/protobuf/message.h>

#include <array>
#include <cstdint>
#include <cstring>

#include "common/byteorder.h"
#include "common/concepts.h"
#include "common/crc.h"
#include "common/status.h"

namespace blobstore {
namespace blobnode {

// MetaBlocker provides unified encode/decode for protobuf messages
//
// Wire format:
//   | magic (4B) | len (4B) | meta (len B) | padding | crc (4B) |
//   |<---- kHeaderSize ---->|                                   |
//   |<-------------------- BlockSize -------------------------->|
//
// Template parameters:
//   - MagicSize: size of magic bytes (typically 4)
//   - Magic: magic bytes array for identifying the block type
//
template <size_t MagicSize, const std::array<uint8_t, MagicSize>& Magic>
class MetaBlocker {
   public:
    static constexpr size_t kMagicSize = MagicSize;
    static constexpr size_t kLenSize = 4;
    static constexpr size_t kCrcSize = 4;
    static constexpr size_t kHeaderSize = MagicSize + kLenSize;
    static constexpr size_t MaxPayloadSize(size_t block_size) {
        return block_size - kHeaderSize - kCrcSize;
    }

    template <::blobstore::ProtobufMessageSerdes Meta>
    static Status<> Encode(size_t block_size, const Meta& meta, char* b, const std::string& name) {
        Status<> s;

        auto len = meta.ByteSizeLong();
        if (len > MaxPayloadSize(block_size)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": encode payload too large");
            return s;
        }

        std::memcpy(b, Magic.data(), MagicSize);
        BigEndian::PutUint32(b + MagicSize, static_cast<uint32_t>(len));

        char* data = b + kHeaderSize;
        if (!meta.SerializeToArray(data, static_cast<int>(len))) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": encode serialize failed");
            return s;
        }

        uint32_t crc = CRC32_IEEE(0, b, kHeaderSize + len);
        BigEndian::PutUint32(b + block_size - kCrcSize, crc);
        return s;
    }

    template <::blobstore::ProtobufMessageSerdes Meta>
    static Status<> Decode(size_t block_size, const char* b, Meta* meta, const std::string& name) {
        Status<> s;

        if (std::memcmp(b, Magic.data(), MagicSize) != 0) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": decode magic mismatch");
            return s;
        }

        uint32_t len = BigEndian::Uint32(b + MagicSize);
        if (len == 0 || len > MaxPayloadSize(block_size)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": decode length invalid");
            return s;
        }

        const char* data = b + kHeaderSize;
        if (!meta->ParseFromArray(data, static_cast<int>(len))) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": deoce parse failed");
            return s;
        }

        uint32_t crc = BigEndian::Uint32(b + block_size - kCrcSize);
        if (crc != CRC32_IEEE(0, b, meta->ByteSizeLong() + kHeaderSize)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason(name + ": decode crc mismatch");
            return s;
        }
        return s;
    }
};

}  // namespace blobnode
}  // namespace blobstore
