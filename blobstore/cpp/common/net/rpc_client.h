#pragma once

#include <seastar/core/condition-variable.hh>

#include "common/net/client.h"
#include "common/net/rpc.h"

namespace blobstore {
namespace net {

class RpcClient;
class RpcClientContext;

class ClientMgr {
    seastar::gate gate_;
    seastar::condition_variable recyle_cv_;
    // use double map to avoid interator invalid in recyle loop
    std::unordered_map<seastar::socket_address, ClientPtr> client_map_;
    std::unordered_map<seastar::socket_address, ClientPtr> new_client_map_;

    ClientPtr GetClient(const seastar::socket_address &sa);

    seastar::future<> RecycleLoop(std::chrono::seconds idle_timeout);

    seastar::future<Status<ClientStreamPtr>> GetClientStream(
        seastar::socket_address sa, Option opt, std::chrono::milliseconds connect_timeout,
        BufferAllocator *allocator);

    friend class RpcClientContext;
    friend class RpcClient;

   public:
    explicit ClientMgr(std::chrono::seconds idle_timeout) noexcept;
    seastar::future<> Close();
};

using ClientMgrPtr = std::unique_ptr<ClientMgr>;

class RpcClientContext {
    Status<> last_status_;
    seastar::socket_address sa_;
    seastar::foreign_ptr<ClientStreamPtr> client_stream_;

    Buffer pending_body_;  // has body in first frame
    bool has_pending_body_ = false;

    friend RpcClient;

    explicit RpcClientContext(seastar::socket_address sa,
                              seastar::foreign_ptr<ClientStreamPtr> client_stream)
        : sa_(sa), client_stream_(std::move(client_stream)), has_pending_body_(false) {}
    RpcClientContext() = delete;

   public:
    ~RpcClientContext();

    void SetPendingBody(Buffer body) {
        pending_body_ = std::move(body);
        has_pending_body_ = true;
    }

    seastar::future<Status<RpcResponseHeader>> ReadHeader(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) noexcept;

    seastar::future<Status<Buffer>> ReadBody(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) noexcept;

    seastar::future<Status<>> WriteHeader(RpcRequestHeader req_header) noexcept;

    seastar::future<Status<>> WriteBody(Buffer body) noexcept;
    seastar::future<Status<>> WriteBody(const char *b, size_t n) noexcept;
    seastar::future<Status<>> WriteBody(std::vector<iovec> iovs) noexcept;
};

class RpcClient {
    Option opt_;
    std::chrono::milliseconds connect_timeout_ = std::chrono::milliseconds(100);
    std::chrono::seconds idle_timeout_ = std::chrono::seconds(300);
    BufferAllocator *allocator_ = nullptr;
    std::vector<seastar::foreign_ptr<ClientMgrPtr>> client_mgr_vec_;

    void RecycleLoop();
    RpcClient() {}

   public:
    static seastar::future<std::unique_ptr<RpcClient>> MakeRpcClient(
        const Option opt = Option(),
        std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(100),
        std::chrono::seconds idle_timeout = std::chrono::seconds(300),
        BufferAllocator *allocator = nullptr);

    seastar::future<Status<std::unique_ptr<RpcClientContext>>> MakeRpcClientContext(
        std::string_view host, uint16_t port);
    seastar::future<Status<std::unique_ptr<RpcClientContext>>> MakeRpcClientContext(
        seastar::socket_address sa);

    // release all resources
    seastar::future<> Close();
};

}  // namespace net
}  // namespace blobstore
