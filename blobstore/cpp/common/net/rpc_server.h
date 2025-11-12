#pragma once

#include <google/protobuf/message.h>

#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/smp.hh>
#include <string>
#include <unordered_map>

#include "common/net/rpc.h"
#include "common/net/session.h"

namespace blobstore {
namespace net {

class RpcServer {
   protected:
    // Handle request, find route, run middlewares, call handler
    seastar::future<Status<>> HandleContext(RpcServerContext* ctx) noexcept;

   public:
    using RouterIndex = int32_t;
    // RouteHandler function type for routing
    using RouteHandler = std::function<seastar::future<Status<>>(RpcServerContext* ctx)>;
    // RouteMiddleware stop with status code != OK
    using RouteMiddleware = RouteHandler;

    void AddMiddleware(RouteMiddleware middleware) noexcept;
    void RegisterHandler(RouterIndex index, RouteHandler handler) noexcept;

    virtual ~RpcServer() {
        middlewares_.clear();
        handlers_.clear();
    }

    virtual seastar::future<> Start() = 0;
    virtual seastar::future<> Close() = 0;

   private:
    std::vector<RouteMiddleware> middlewares_;
    std::unordered_map<RouterIndex, RouteHandler> handlers_;
};

class TcpRpcServer : public RpcServer {
    Option opt_;
    std::string host_;
    uint16_t port_;

    seastar::gate gate_;
    seastar::pollable_fd fd_;
    std::unordered_map<uint64_t, SessionPtr> sess_mgr_;

    seastar::future<> HandleStream(StreamPtr stream);
    seastar::future<> HandleSession(SessionPtr sess);

   public:
    explicit TcpRpcServer(const Option& opt, const std::string& host, uint16_t port);

    virtual ~TcpRpcServer() {}

    virtual seastar::future<> Start() override;

    virtual seastar::future<> Close() override;
};

}  // namespace net
}  // namespace blobstore
