#pragma once

#include <sys/uio.h>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "common/util.h"

namespace blobstore {
namespace blobnode {

class Device;

using DevicePtr = std::unique_ptr<Device>;

class Device {
   public:
    virtual ~Device() = default;

    virtual const std::string& Name() const = 0;

    virtual size_t Capacity() const = 0;

    virtual uint32_t SectorSize() const = 0;

    virtual Buffer Alloc(size_t n) = 0;

    // Write data to the device at the specified position using a buffer.
    // @param pos: The offset position on the device to start writing (must be physical sector
    // aligned).
    // @param b: The buffer to write. The address must be aligned with memory alignment.
    // @param len: The length of the data to write (must be aligned with physical sector size).
    // @return: A future containing the status of the write operation.
    virtual seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) = 0;

    // Write data to the device at the specified position using a vector of iovec.
    // @param pos: The offset position on the device to start writing (must be logical sector
    // aligned).
    // @param iov: Each iovec buffer address must be align with memory alignment, and its length
    // must be align with logical sector size.
    // @return: A future containing the status of the write operation.
    virtual seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iovs) = 0;

    // Write data to the device at the specified position using a vector of Buffer.
    // @param pos: The offset position on the device to start writing (must be logical sector
    // aligned).
    // @param buffers: Each Buffer's address must be aligned with memory alignment, and its length
    // must be aligned with logical sector size.
    // @return: A future containing the status of the write operation.
    virtual seastar::future<Status<>> Write(uint64_t pos, std::vector<Buffer> buffers) = 0;

    // Read data from the device at the specified position using a buffer.
    // @param pos: The offset position on the device to start reading (must be physical sector
    // aligned).
    // @param b: The buffer to read. The address must be aligned with memory alignment.
    // @param len: The length of the data to read (must be aligned with physical sector size).
    // @return: A future containing the status of the read operation.
    virtual seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) = 0;

    // Read data from the device at the specified position using a vector of iovec.
    // @param pos: The offset position on the device to start reading (must be logical sector
    // aligned).
    // @param iovs: Each iovec buffer address must be align with memory alignment, and its length
    // must be align with logical sector size.
    // @return: A future containing the status of the read operation.
    virtual seastar::future<Status<size_t>> Read(uint64_t pos, std::vector<iovec> iovs) = 0;

    virtual seastar::future<> Close() = 0;
};

}  // namespace blobnode
}  // namespace blobstore
