#pragma once

#include <sys/uio.h>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string_view>
#include <vector>

#include "blobnode/device/device.h"
#include "common/status.h"
#include "common/util.h"

namespace blobstore {
namespace blobnode {

class KernelDevice : public Device {
    std::string name_;
    seastar::file fp_;
    uint32_t sector_size_ = 512;
    size_t capacity_;
    seastar::gate gate_;

    KernelDevice() noexcept {}

   public:
    virtual ~KernelDevice() {}

    static seastar::future<Status<DevicePtr>> Create(const std::string_view name) noexcept;

    const std::string& Name() const override { return name_; }

    size_t Capacity() const override { return capacity_; }

    uint32_t SectorSize() const override { return sector_size_; }

    Buffer Alloc(size_t n) override;

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) override;

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iovs) override;

    seastar::future<Status<>> Write(uint64_t pos, std::vector<Buffer> buffers) override;

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) override;

    seastar::future<Status<size_t>> Read(uint64_t pos, std::vector<iovec> iovs) override;

    seastar::future<> Close() override;
};

}  // namespace blobnode
}  // namespace blobstore
