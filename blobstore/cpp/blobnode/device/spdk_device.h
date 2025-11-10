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

#include "blobnode/device/device.h"
#include "common/const.h"
#include "common/logger.h"

namespace blobstore {
namespace blobnode {

class SpdkDevice : public Device {
    std::string name_;
    size_t capacity_ = 0;
    uint32_t sector_size_ = 512;
    seastar::gate gate_;

#ifdef HAS_SPDK
    struct spdk_env_opts opts_;
    struct spdk_nvme_transport_id trid_;
    uint32_t ns_id_ = 0;
    struct spdk_nvme_probe_ctx* probe_ctx_ = nullptr;
    struct spdk_nvme_ctrlr* ctrlr_ = nullptr;
    struct spdk_nvme_ns* ns_ = nullptr;
    std::vector<struct spdk_nvme_qpair*> qpairs_;
    int qpairs_idx_ = 0;

    bool attach_ = false;
    struct spdk_nvme_detach_ctx* detach_ctx_ = nullptr;
    std::optional<seastar::promise<>> detach_pr_;
    std::optional<seastar::reactor::poller> poller_;

    struct request {
        SpdkDevice* dev_ptr;
        seastar::promise<int> pr;
        std::string reason;
    };

    struct bulk_request {
        SpdkDevice* dev_ptr;
        seastar::promise<int> pr;
        std::vector<iovec> iovs;
        size_t index = 0;
    };

    static bool ProbeCallback(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                              struct spdk_nvme_ctrlr_opts* opts);

    static void AttachCallback(void* cb_ctx, const struct spdk_nvme_transport_id* trid,
                               struct spdk_nvme_ctrlr* ctrlr,
                               const struct spdk_nvme_ctrlr_opts* opts);

    bool Poll();

#endif

    SpdkDevice() noexcept {}

   public:
    virtual ~SpdkDevice() {}

    static seastar::future<Status<DevicePtr>> Create(const std::string_view name, uint32_t ns_id,
                                                     uint32_t qpair_n) noexcept;
    const std::string& Name() const override { return name_; }

    uint32_t SectorSize() const override { return sector_size_; }

    size_t Capacity() const override { return capacity_; }

    Buffer Alloc(size_t n) override;

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) override;

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iov) override;

    seastar::future<Status<>> Write(uint64_t pos, std::vector<Buffer> buffers) override;

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) override;

    seastar::future<Status<size_t>> Read(uint64_t pos, std::vector<iovec> iovs) override;

    seastar::future<> Close() override;
};

}  // namespace blobnode
}  // namespace blobstore
