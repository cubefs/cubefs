#include "spdk_device.h"

#include <bits/stdint-uintn.h>

#ifdef HAS_SPDK
#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/nvme_zns.h>
#endif

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

static seastar::sstring kDevClosedSpdk = "dev: spdk device closed";

#ifdef HAS_SPDK

struct SpdkDeleter final : public seastar::deleter::impl {
    void* obj;
    SpdkDeleter(void* obj) : impl(seastar::deleter()), obj(obj) {}
    virtual ~SpdkDeleter() override { spdk_free(obj); }
};

static seastar::deleter make_spdk_deleter(void* obj) {
    return seastar::deleter(new SpdkDeleter(obj));
}

bool SpdkDevice::ProbeCallback(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                               struct spdk_nvme_ctrlr_opts* opts) {
    return true;
}

void SpdkDevice::AttachCallback(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                                struct spdk_nvme_ctrlr* ctrlr,
                                const struct spdk_nvme_ctrlr_opts* opts) {
    SpdkDevice::request* req = reinterpret_cast<SpdkDevice::request*>(cb_ctx);
    req->dev_ptr->attach_ = true;
    for (auto nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
        if (nsid == req->dev_ptr->ns_id_) {
            auto ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
            if (!spdk_nvme_ns_is_active(ns)) {
                LOG_ERROR("device {} ns: {} is not active", trid->traddr, nsid);
                req->pr.set_value(-1);
                return;
            }
            req->dev_ptr->ctrlr_ = ctrlr;
            req->dev_ptr->ns_ = ns;
            req->pr.set_value(0);
            return;
        }
    }
    LOG_WARN("device {} ns {} is not found", trid->traddr, req->dev_ptr->ns_id_);
    req->pr.set_value(-1);
    return;
}

bool SpdkDevice::Poll() {
    if (!attach_ && probe_ctx_ && spdk_nvme_probe_poll_async(probe_ctx_) == 0) {
        probe_ctx_ = nullptr;
    }

    for (auto& qpair : qpairs_) {
        spdk_nvme_qpair_process_completions(qpair, 0);
    }

    if (detach_ctx_ && detach_pr_ && spdk_nvme_detach_poll_async(detach_ctx_) == 0) {
        detach_ctx_ = nullptr;
        detach_pr_.value().set_value();
    }
    return true;
}
#endif

seastar::future<Status<DevicePtr>> SpdkDevice::Create(const std::string_view name, uint32_t ns_id,
                                                      uint32_t qpair_n) noexcept {
    Status<DevicePtr> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support to create spdk device");
#else
    if (qpair_n == 0 || qpair_n > 16) {
        qpair_n == 8;
    }
    std::unique_ptr<SpdkDevice> dev_ptr(new SpdkDevice());
    spdk_nvme_trid_populate_transport(&dev_ptr->trid_, SPDK_NVME_TRANSPORT_PCIE);
    if (0 != spdk_nvme_transport_id_parse(&dev_ptr->trid_, name.data())) {
        s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: invalid transport address");
        co_return s;
    }
    dev_ptr->name_ = name;

    std::unique_ptr<SpdkDevice::request> req(new SpdkDevice::request);
    req->dev_ptr = dev_ptr.get();
    dev_ptr->probe_ctx_ = spdk_nvme_probe_async(
        &dev_ptr->trid_, req.get(), SpdkDevice::ProbeCallback, SpdkDevice::AttachCallback, NULL);

    dev_ptr->poller_ =
        seastar::reactor::poller::simple([ptr = dev_ptr.get()]() -> bool { return ptr->Poll(); });

    auto res = co_await req->pr.get_future();
    if (res == -1) {
        co_await dev_ptr->Close();
        s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: nvme probe error");
        co_return s;
    }

    for (uint32_t i = 0; i < qpair_n; i++) {
        auto qpair = spdk_nvme_ctrlr_alloc_io_qpair(dev_ptr->ctrlr_, NULL, 0);
        if (qpair == NULL) {
            co_await dev_ptr->Close();
            s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: alloc io qpair error");
            co_return s;
        }
        dev_ptr->qpairs_.push_back(qpair);
    }

    if (spdk_nvme_ns_get_csi(dev_ptr->ns_) == SPDK_NVME_CSI_ZNS) {
        std::unique_ptr<SpdkDevice::request> ns_req(new SpdkDevice::request);
        ns_req->dev_ptr = dev_ptr.get();
        if (spdk_nvme_zns_reset_zone(
                dev_ptr->ns_, dev_ptr->qpairs_[0], 0, false,
                [](void* arg, const struct spdk_nvme_cpl* completion) {
                    SpdkDevice::request* r = reinterpret_cast<SpdkDevice::request*>(arg);
                    if (spdk_nvme_cpl_is_error(completion)) {
                        LOG_ERROR("I/O error status: {}",
                                  spdk_nvme_cpl_get_status_string(&completion->status));
                        r->pr.set_value(-1);
                        return;
                    }
                    r->pr.set_value(0);
                },
                ns_req.get())) {
            co_await dev_ptr->Close();
            s.SetCode(ErrCode::ErrEIO).SetReason("dev: reset zone error");
            co_return s;
        }
        auto res = co_await ns_req->pr.get_future();
        if (res == -1) {
            s.SetCode(ErrCode::ErrEIO).SetReason("dev: reset zone failed");
            co_await dev_ptr->Close();
            co_return s;
        }
    }
    dev_ptr->sector_size_ = spdk_nvme_ns_get_sector_size(dev_ptr->ns_);
    dev_ptr->capacity_ = spdk_nvme_ns_get_size(dev_ptr->ns_);
    if (dev_ptr->sector_size_ > kSectorSize) {
        LOG_ERROR("device {} sector size {} is larger than kSectorSize", dev_ptr->name_,
                  dev_ptr->sector_size_);
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: sector size is larger than kSectorSize");
        co_await dev_ptr->Close();
        co_return s;
    } else if (kSectorSize % dev_ptr->sector_size_ != 0) {
        LOG_ERROR("device {} physical sector size {} is invalid", dev_ptr->name_,
                  dev_ptr->sector_size_);
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: physical sector size is invalid");
        co_await dev_ptr->Close();
        co_return s;
    }

    s.SetValue(std::move(dev_ptr));
#endif
    co_return s;
}

Buffer SpdkDevice::Alloc(size_t n) {
#ifndef HAS_SPDK
    size_t len = std::max(n, kMemoryAlignment);
    auto buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, len);
    buf.trim(n);
    return std::move(buf);
#else
    // 申请的内存必须4K大小对齐
    void* b = spdk_malloc(seastar::align_up(n, kMemoryAlignment), kMemoryAlignment, NULL,
                          SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    return Buffer((char*)b, n, make_spdk_deleter(b));
#endif
}

seastar::future<Status<>> SpdkDevice::Write(uint64_t pos, const char* b, size_t len) {
    Status<> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support spdk device");
#else
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedSpdk);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    std::unique_ptr<request> req(new request);
    req->dev_ptr = this;
    if ((reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: buffer address is not align");
        co_return s;
    }
    if ((len & (sector_size_ - 1)) || (pos & (sector_size_ - 1))) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos or len is not align sector");
        co_return s;
    }
    if (pos + len > capacity_) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos+len is larger than device capacity");
        co_return s;
    }
    auto res = spdk_nvme_ns_cmd_write(
        ns_, qpairs_[qpairs_idx_], (void*)b, (pos / sector_size_), len / sector_size_,
        [](void* arg, const struct spdk_nvme_cpl* completion) {
            SpdkDevice::request* r = reinterpret_cast<SpdkDevice::request*>(arg);
            if (spdk_nvme_cpl_is_error(completion)) {
                LOG_ERROR("I/O error status: {}",
                          spdk_nvme_cpl_get_status_string(&completion->status));
                r->pr.set_value(-1);
                return;
            }
            r->pr.set_value(0);
        },
        req.get(), 0);
    qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();
    if (res != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk write submit error");
        co_return s;
    }

    auto code = co_await req->pr.get_future();
    if (code != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk write I/O error");
    }
#endif
    co_return s;
}

seastar::future<Status<>> SpdkDevice::Write(uint64_t pos, std::vector<iovec> iov) {
    Status<> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support spdk device");
#else
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedSpdk);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if (pos & kSectorSizeMask) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos is not align sector");
        co_return s;
    }

    uint64_t lba = pos / sector_size_;
    uint32_t lba_count = 0;
    for (int i = 0; i < iov.size(); i++) {
        if (((uint64_t)iov[i].iov_base & kMemoryAlignmentMask) ||
            (iov[i].iov_len & kSectorSizeMask)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: data len is not align sector");
            break;
        }
        if (pos + iov[i].iov_len > capacity_) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: data len is larger than capacity");
            break;
        }
        lba_count += iov[i].iov_len / sector_size_;
    }
    qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();

    std::unique_ptr<SpdkDevice::bulk_request> req(new SpdkDevice::bulk_request);
    req->dev_ptr = this;
    req->iovs = std::move(iov);
    auto res = spdk_nvme_ns_cmd_writev(
        ns_, qpairs_[qpairs_idx_], lba, lba_count,
        [](void* arg, const struct spdk_nvme_cpl* cpl) {
            SpdkDevice::bulk_request* r = reinterpret_cast<SpdkDevice::bulk_request*>(arg);
            if (spdk_nvme_cpl_is_error(cpl)) {
                LOG_ERROR("I/O error status: {}", spdk_nvme_cpl_get_status_string(&cpl->status));
                r->pr.set_value(-1);
                return;
            }
            r->pr.set_value(0);
        },
        req.get(), 0, [](void* arg, uint32_t sql_offset) {},
        [](void* arg, void** address, uint32_t* length) -> int {
            SpdkDevice::bulk_request* r = reinterpret_cast<SpdkDevice::bulk_request*>(arg);
            if (r->index >= r->iovs.size()) {
                *address = nullptr;
                *length = 0;
                return 0;
            }
            *address = (void*)r->iovs[r->index].iov_base;
            *length = r->iovs[r->index].iov_len;
            r->index++;
            return 0;
        });
    qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();
    if (res != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk writev submit error");
        LOG_ERROR("spdk nvme write error: {}", s);
        co_return s;
    }

    auto code = co_await req->pr.get_future();
    if (code != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk writev I/O error");
    }
#endif
    co_return s;
}

seastar::future<Status<>> SpdkDevice::Write(uint64_t pos, std::vector<Buffer> buffers) {
    Status<> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support spdk device");
#else
    std::vector<iovec> iov;
    for (int i = 0; i < buffers.size(); i++) {
        iovec io;
        io.iov_base = buffers[i].get_write();
        io.iov_len = buffers[i].size();
        iov.push_back(io);
    }
    s = co_await Write(pos, std::move(iov));
#endif
    co_return s;
}

seastar::future<Status<size_t>> SpdkDevice::Read(uint64_t pos, char* b, size_t len) {
    Status<size_t> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support spdk device");
#else
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedSpdk);
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
    std::unique_ptr<SpdkDevice::request> req(new SpdkDevice::request);
    req->dev_ptr = this;
    auto res = spdk_nvme_ns_cmd_read(
        ns_, qpairs_[qpairs_idx_], b, pos / sector_size_, len / sector_size_,
        [](void* arg, const struct spdk_nvme_cpl* cpl) {
            SpdkDevice::request* r = reinterpret_cast<SpdkDevice::request*>(arg);
            if (spdk_nvme_cpl_is_error(cpl)) {
                LOG_ERROR("I/O error status: {}", spdk_nvme_cpl_get_status_string(&cpl->status));
                r->pr.set_value(-1);
                return;
            }
            r->pr.set_value(0);
        },
        req.get(), 0);
    qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();
    if (res != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk read submit error");
        LOG_ERROR("spdk nvme read error: {}", s);
        co_return s;
    }

    auto code = co_await req->pr.get_future();
    if (code != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk read I/O error");
    } else {
        s.SetValue(len);
    }
#endif
    co_return s;
}

seastar::future<Status<size_t>> SpdkDevice::Read(uint64_t pos, std::vector<iovec> iovs) {
    Status<size_t> s;
#ifndef HAS_SPDK
    s.SetCode(ErrCode::ErrUnsupported).SetReason("dev: not support spdk device");
#else
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kDevClosedSpdk);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if (pos & kSectorSizeMask) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("dev: pos is not align sector");
        co_return s;
    }
    if (pos >= capacity_) {
        s.SetValue(0);
        co_return s;
    }

    uint64_t lba = pos / sector_size_;
    uint32_t lba_count = 0;
    for (int i = 0; i < iovs.size(); i++) {
        if (iovs[i].iov_len == 0 || (iovs[i].iov_len & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(iovs[i].iov_base) & kMemoryAlignmentMask)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("dev: iov is not align sector");
            co_return s;
        }
        lba_count += iovs[i].iov_len / sector_size_;
    }
    if (lba_count == 0) {
        s.SetValue(0);
        co_return s;
    }
    std::unique_ptr<SpdkDevice::bulk_request> req(new SpdkDevice::bulk_request);
    req->dev_ptr = this;
    req->iovs = std::move(iovs);
    qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();

    auto res = spdk_nvme_ns_cmd_readv(
        ns_, qpairs_[qpairs_idx_], lba, lba_count,
        [](void* arg, const struct spdk_nvme_cpl* cpl) {
            SpdkDevice::bulk_request* r = reinterpret_cast<SpdkDevice::bulk_request*>(arg);
            if (spdk_nvme_cpl_is_error(cpl)) {
                LOG_ERROR("I/O error status: {}", spdk_nvme_cpl_get_status_string(&cpl->status));
                r->pr.set_value(-1);
                return;
            }
            r->pr.set_value(0);
        },
        req.get(), 0, [](void* arg, uint32_t sql_offset) {},
        [](void* arg, void** address, uint32_t* length) -> int {
            SpdkDevice::bulk_request* r = reinterpret_cast<SpdkDevice::bulk_request*>(arg);
            if (r->index >= r->iovs.size()) {
                *address = nullptr;
                *length = 0;
                return 0;
            }
            *address = (void*)r->iovs[r->index].iov_base;
            *length = r->iovs[r->index].iov_len;
            r->index++;
            return 0;
        });

    if (res != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk readv submit error");
        LOG_ERROR("spdk nvme readv error: {}", s);
        co_return s;
    }

    auto code = co_await req->pr.get_future();
    if (code != 0) {
        s.SetCode(ErrCode::ErrEIO).SetReason("dev: spdk readv I/O error");
    } else {
        s.SetValue(lba_count * sector_size_);
    }
#endif
    co_return s;
}

seastar::future<> SpdkDevice::Close() {
#ifdef HAS_SPDK
    if (gate_.is_closed()) {
        co_return;
    }
    co_await gate_.close();
    for (int i = 0; i < qpairs_.size(); ++i) {
        spdk_nvme_ctrlr_free_io_qpair(qpairs_[i]);
    }
    qpairs_.clear();
    if (ctrlr_) {
        spdk_nvme_detach_async(ctrlr_, &detach_ctx_);
        if (detach_ctx_) {
            detach_pr_ = seastar::promise<>();
            co_await detach_pr_.value().get_future();
            detach_pr_.reset();
        }
    }
#endif
    co_return;
}

}  // namespace blobnode
}  // namespace blobstore
