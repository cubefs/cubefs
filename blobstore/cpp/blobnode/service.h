#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include "common/net/rpc.h"
#include "common/status.h"

namespace blobstore {
namespace blobnode {

class StoreHandlerFactory;

class Service {
    StoreHandlerFactory* store_handler_factory_ = nullptr;
    seastar::gate gate_;

   public:
    explicit Service(StoreHandlerFactory* factory) : store_handler_factory_(factory) {}

    seastar::future<Status<>> HandlePing(net::RpcServerContext* ctx);

    seastar::future<> Close();
};

}  // namespace blobnode
}  // namespace blobstore
