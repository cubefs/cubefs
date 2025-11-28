#include "kernel_device.h"

#include <bits/stdint-uintn.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/poll.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>

#include "common/const.h"
#include "common/logger.h"

namespace blobstore {
namespace blobnode {

static seastar::sstring kDevClosedKernel = "dev: kernel device closed";

seastar::future<Status<DevicePtr>> KernelDevice::Create(const std::string_view name) noexcept {
    Status<DevicePtr> s;
    std::unique_ptr<KernelDevice> ptr(new KernelDevice());
    uint32_t sector_size;
    try {
        ptr->fp_ = co_await seastar::open_file_dma(name, seastar::open_flags::rw);
        ptr->capacity_ = co_await ptr->fp_.size();
        ptr->capacity_ = seastar::align_down(ptr->capacity_, kSectorSize);
        co_await ptr->fp_.ioctl(BLKSSZGET, &sector_size);
        ptr->sector_size_ = sector_size;
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    if (!s) {
        LOG_ERROR("open device {} error: {}", name, s);
        if (ptr->fp_) {
            co_await ptr->fp_.close();
        }
        co_return s;
    }
    ptr->name_ = name;
    s.SetValue(std::move(ptr));
    co_return s;
}

Buffer KernelDevice::Alloc(size_t n) {
    size_t len = std::max(n, kMemoryAlignment);
    auto buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, len);
    buf.trim(n);
    return std::move(buf);
}

seastar::future<Status<>> KernelDevice::Write(uint64_t pos, const char* b, size_t len) {
    Status<> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedKernel);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if ((pos & (sector_size_ - 1)) || (len & (sector_size_ - 1))) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos or len is not align sector");
        co_return s;
    }
    if ((reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: buffer address is not align");
        co_return s;
    }
    if (pos + len > capacity_) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: data len is larger than capacity");
        co_return s;
    }
    try {
        auto size = co_await fp_.dma_write(pos, b, len);
        if (size != len) {
            s.SetCode(ErrCode::ErrDevice).SetReason("dev: returned size is not expect");
        }
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrEIO).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    co_return s;
}

seastar::future<Status<>> KernelDevice::Write(uint64_t pos, std::vector<iovec> iovs) {
    Status<> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedKernel);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if (pos & kSectorSizeMask) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos is not align sector");
        co_return s;
    }
    int n = iovs.size();
    if (n == 0) {
        co_return s;
    }
    size_t total = 0;
    for (int i = 0; i < n; i++) {
        auto v = iovs[i];
        if (pos + total + v.iov_len > capacity_) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: data exceeds capacity");
            co_return s;
        }
        if ((reinterpret_cast<uintptr_t>(v.iov_base) & kMemoryAlignmentMask) ||
            (v.iov_len & kSectorSizeMask)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: iov is not align");
            co_return s;
        }
        total += v.iov_len;
    }
    try {
        auto size = co_await fp_.dma_write(pos, std::move(iovs));
        if (size != total) {
            s.SetCode(ErrCode::ErrDevice).SetReason("dev: returned size is not expect");
        }
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrEIO).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    co_return s;
}

seastar::future<Status<>> KernelDevice::Write(uint64_t pos, std::vector<Buffer> buffers) {
    Status<> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedKernel);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if (pos & kSectorSizeMask) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos is not align sector");
        co_return s;
    }
    int n = buffers.size();
    if (n == 0) {
        co_return s;
    }
    std::vector<iovec> iovs;
    size_t total = 0;
    for (int i = 0; i < n; i++) {
        auto& b = buffers[i];
        if (pos + total + b.size() > capacity_) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: data exceeds capacity");
            co_return s;
        }
        if ((reinterpret_cast<uintptr_t>(b.get()) & kMemoryAlignmentMask) ||
            (b.size() & kSectorSizeMask)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: buffer is not align");
            co_return s;
        }
        total += b.size();
        iovec io;
        io.iov_base = b.get_write();
        io.iov_len = b.size();
        iovs.push_back(io);
    }
    try {
        auto size = co_await fp_.dma_write(pos, std::move(iovs));
        if (size != total) {
            s.SetCode(ErrCode::ErrDevice).SetReason("dev: returned size is not expect");
        }
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrEIO).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    co_return s;
}

seastar::future<Status<size_t>> KernelDevice::Read(uint64_t pos, char* b, size_t len) {
    Status<size_t> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedKernel);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if ((pos & (sector_size_ - 1)) || (len & (sector_size_ - 1))) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos or len is not align sector");
        co_return s;
    }
    if ((reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: buffer address is not align");
        co_return s;
    }
    if (pos >= capacity_) {
        s.SetValue(0);
        co_return s;
    } else if (pos + len > capacity_) {
        len = capacity_ - pos;
    }
    try {
        auto size = co_await fp_.dma_read<char>(pos, b, len);
        s.SetValue(size);
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrEIO).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    co_return s;
}

seastar::future<Status<size_t>> KernelDevice::Read(uint64_t pos, std::vector<iovec> iovs) {
    Status<size_t> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedKernel);
        co_return s;
    }

    seastar::gate::holder holder(gate_);
    if (pos & kSectorSizeMask) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos is not align sector");
        co_return s;
    }
    int n = iovs.size();
    if (n == 0) {
        s.SetValue(0);
        co_return s;
    }
    try {
        auto size = co_await fp_.dma_read(pos, std::move(iovs));
        s.SetValue(size);
    } catch (std::system_error& e) {
        s.SetCode(ErrCode::ErrEIO).SetReason(e.what());
    } catch (std::exception& e) {
        s.SetCode(ErrCode::ErrDevice).SetReason(e.what());
    }
    co_return s;
}

seastar::future<> KernelDevice::Close() {
    if (gate_.is_closed()) {
        co_return;
    }
    co_await gate_.close();
    if (fp_) {
        co_await fp_.close();
    }
    co_return;
}

}  // namespace blobnode
}  // namespace blobstore
