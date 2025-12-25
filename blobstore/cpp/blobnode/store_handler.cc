#include "store_handler.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

namespace blobstore {
namespace blobnode {

thread_local std::unordered_map<uint32_t, StoreHandler*> StoreHandlerFactory::tls_store_handlers_ =
    {};

uint32_t StoreHandler::ID() const {
    // TODO
    return 0;
}

seastar::future<> StoreHandler::Close() {
    // TODO
    co_return;
}

seastar::future<Status<StoreHandlerFactoryPtr>> StoreHandlerFactory::Create(
    std::vector<StoreConfig> cfgs) {
    Status<StoreHandlerFactoryPtr> s;
    StoreHandlerFactoryPtr ptr(new StoreHandlerFactory);
    auto res = co_await ptr->Init(std::move(cfgs));
    if (!res) {
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_await ptr->Close();
    } else {
        s.SetValue(std::move(ptr));
    }
    co_return s;
}

seastar::future<Status<>> StoreHandlerFactory::Init(std::vector<StoreConfig> cfgs) {
    Status<> s;
    int n = cfgs.size();
    std::vector<seastar::future<Status<seastar::foreign_ptr<StoreHandlerPtr>>>> fu_vec;
    for (int i = 0; i < n; i++) {
        unsigned shard = i % seastar::smp::count;
        auto res = seastar::smp::submit_to(
            shard,
            [&cfg = cfgs[i],
             this]() -> seastar::future<Status<seastar::foreign_ptr<StoreHandlerPtr>>> {
                Status<seastar::foreign_ptr<StoreHandlerPtr>> s;
                StoreHandlerPtr store_handler(new StoreHandler);
                // TODO 初始化
                s.SetValue(seastar::make_foreign(std::move(store_handler)));
                co_return s;
            });
        fu_vec.emplace_back(std::move(res));
    }

    auto results = co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for (auto& res : results) {
        if (!res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
            continue;
        }
        auto store_handler = std::move(res.Value());
        store_handlers_[store_handler->ID()] = std::move(store_handler);
    }
    if (!s) {
        co_return s;
    }
    co_await seastar::smp::invoke_on_all([this]() {
        for (auto& [k, v] : store_handlers_) {
            tls_store_handlers_[k] = v.get();
        }
    });
    co_return s;
}

StoreHandler* StoreHandlerFactory::GetStoreHandler(uint32_t disk_id) {
    auto it = tls_store_handlers_.find(disk_id);
    if (it == tls_store_handlers_.end()) {
        return nullptr;
    }
    return it->second;
}

seastar::future<> StoreHandlerFactory::Close() {
    // TODO close all store handler
    co_return;
}

}  // namespace blobnode
}  // namespace blobstore
