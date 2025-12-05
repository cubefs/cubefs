#include "rpc_server.h"

#include <fmt/format.h>

namespace blobstore {
namespace net {

void RpcServer::AddMiddleware(RouteMiddleware middleware) noexcept {
    middlewares_.push_back(std::move(middleware));
}

void RpcServer::RegisterHandler(RouterIndex index, RouteHandler handler) noexcept {
    handlers_[index] = std::move(handler);
}

seastar::future<Status<>> RpcServer::HandleContext(RpcServerContext* ctx) noexcept {
    Status<> s;

    RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
    RouterIndex path_index = req_header.RemotePathIndex();

    auto it = handlers_.find(path_index);
    if (it == handlers_.end()) {
        // No handler found, return 404
        RpcResponseHeader resp_header;
        resp_header.SetStatus(ErrCode::ErrNotFound);
        std::string reason =
            fmt::format("no router for path([{}]{})", path_index, req_header.RemotePath());
        resp_header.SetReason(reason);
        s = co_await ctx->WriteHeader(std::move(resp_header));
        co_return s;
    }

    for (auto& middleware : middlewares_) {
        auto middleware_result = co_await middleware(ctx);
        if (!middleware_result.OK()) {
            // Middleware returned error, stop processing
            RpcResponseHeader resp_header;
            resp_header.SetStatus(middleware_result.Code());
            const auto& sreason = middleware_result.Reason();
            std::string reason(sreason.c_str(), sreason.size());
            resp_header.SetReason(reason);
            s = co_await ctx->WriteHeader(std::move(resp_header));
            co_return s;
        }
    }

    RouteHandler& handler = it->second;
    s = co_await handler(ctx);
    co_return s;
}

}  // namespace net
}  // namespace blobstore
