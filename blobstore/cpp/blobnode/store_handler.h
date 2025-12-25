#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <unordered_map>

#include "common/status.h"
#include "config.h"

namespace blobstore {
namespace blobnode {

class StoreHandler;
class StoreHandlerFactory;

using StoreHandlerPtr = std::unique_ptr<StoreHandler>;
using StoreHandlerFactoryPtr = std::unique_ptr<StoreHandlerFactory>;

class StoreHandler {
    unsigned shard_;
    seastar::gate gate_;
    friend class StoreHandlerFactory;

    StoreHandler() : shard_(seastar::this_shard_id()) {}

   public:
    unsigned Shard() const { return shard_; }

    uint32_t ID() const;

    seastar::future<> Close();
};

class StoreHandlerFactory {
    std::unordered_map<uint32_t, seastar::foreign_ptr<StoreHandlerPtr>> store_handlers_;
    static thread_local std::unordered_map<uint32_t, StoreHandler*> tls_store_handlers_;

    StoreHandlerFactory() {}

    seastar::future<Status<>> Init(std::vector<StoreConfig> cfgs);

   public:
    static seastar::future<Status<StoreHandlerFactoryPtr>> Create(std::vector<StoreConfig> cfgs);

    StoreHandler* GetStoreHandler(uint32_t disk_id);

    seastar::future<> Close();
};

}  // namespace blobnode
}  // namespace blobstore
