#pragma once

#include <cstdint>

namespace blobstore {

class LittleEndian {
   public:
    static inline uint16_t Uint16(const char *b) {
        const uint16_t *v = reinterpret_cast<const uint16_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return *v;
#else
        return __builtin_bswap16(*v);
#endif
    }

    static inline uint32_t Uint32(const char *b) {
        const uint32_t *v = reinterpret_cast<const uint32_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return *v;
#else
        return __builtin_bswap32(*v);
#endif
    }

    static inline uint64_t Uint64(const char *b) {
        const uint64_t *v = reinterpret_cast<const uint64_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return *v;
#else
        return __builtin_bswap64(*v);
#endif
    }

    static inline void PutUint16(char *b, uint16_t v) {
        uint16_t *u16 = reinterpret_cast<uint16_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u16 = v;
#else
        *u16 = __builtin_bswap16(v);
#endif
    }

    static inline void PutUint32(char *b, uint32_t v) {
        uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u32 = v;
#else
        *u32 = __builtin_bswap32(v);
#endif
    }

    static inline void PutUint64(char *b, uint64_t v) {
        uint64_t *u64 = reinterpret_cast<uint64_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u64 = v;
#else
        *u64 = __builtin_bswap64(v);
#endif
    }
};

class BigEndian {
   public:
    static inline uint16_t Uint16(const char *b) {
        const uint16_t *v = reinterpret_cast<const uint16_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return __builtin_bswap16(*v);
#else
        return *v;
#endif
    }

    static inline uint32_t Uint32(const char *b) {
        const uint32_t *v = reinterpret_cast<const uint32_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return __builtin_bswap32(*v);
#else
        return *v;
#endif
    }

    static inline uint64_t Uint64(const char *b) {
        const uint64_t *v = reinterpret_cast<const uint64_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return __builtin_bswap64(*v);
#else
        return *v;
#endif
    }

    static inline void PutUint16(char *b, uint16_t v) {
        uint16_t *u16 = reinterpret_cast<uint16_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u16 = __builtin_bswap16(v);
#else
        *u16 = v;
#endif
    }

    static inline void PutUint32(char *b, uint32_t v) {
        uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u32 = __builtin_bswap32(v);
#else
        *u32 = v;
#endif
    }

    static inline void PutUint64(char *b, uint64_t v) {
        uint64_t *u64 = reinterpret_cast<uint64_t *>(b);
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        *u64 = __builtin_bswap64(v);
#else
        *u64 = v;
#endif
    }
};

}  // namespace blobstore
