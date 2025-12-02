#pragma once

#include <isa-l/crc.h>

#define CRC32(crc, buf, len) crc32_iscsi((uint8_t*)(buf), len, crc)
#define CRC32_IEEE(crc, buf, len) crc32_gzip_refl(crc, (uint8_t*)buf, len)