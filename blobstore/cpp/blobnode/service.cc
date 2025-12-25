#include "service.h"

#include "store_handler.h"

namespace blobstore {
namespace blobnode {

seastar::future<Status<>> Service::HandlePing(net::RpcServerContext* ctx) {
    Status<> s;
    net::RpcResponseHeader resp;

    if (gate_.is_closed()) {
        resp.SetStatus(ErrCode::ErrClosed);
        s = co_await ctx->WriteHeader(std::move(resp));
        co_return s;
    }
    seastar::gate::holder holder(gate_);  // hold the gate
    resp.SetStatus(ErrCode::OK);
    /*
    sotre_handler = store_handler_factory_->GetStoreHandler(disk_id);
    for (;;) {
        Buffer body = co_await ctx->Read();
        auto fn = [store_handler, buf = body.get(),
                   len = body.size()]() -> seastar::future<Status<>> {
            co_await store_handler->HandleWrite(buf, len, off);
        };
        auto s = co_await seastar::smp::submit_to(sotre_handler->Shard(), std::ref(fn));
    }
    */
    s = co_await ctx->WriteHeader(std::move(resp));
    co_return s;
}

seastar::future<> Service::Close() {
    if (gate_.is_closed()) {
        co_return;
    }

    co_await gate_.close();
    co_return;
}

}  // namespace blobnode
}  // namespace blobstore
