#pragma once

#include <google/protobuf/message.h>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/smp.hh>

#include "common/net/rpc.h"
#include "common/net/session.h"

namespace blobstore {
namespace net {

class RpcServer {
   public:
    virtual ~RpcServer() {}

    virtual seastar::future<> Start() = 0;

    virtual seastar::future<> Close() = 0;
};

class TcpRpcServer : public RpcServer {
    Option opt_;
    std::string host_;
    uint16_t port_;
    RpcService* service_;

    seastar::gate gate_;
    seastar::pollable_fd fd_;
    std::unordered_map<uint64_t, SessionPtr> sess_mgr_;

    seastar::future<> HandleStream(StreamPtr stream);
    seastar::future<> HandleSession(SessionPtr sess);

   public:
    explicit TcpRpcServer(const Option& opt, const std::string& host, uint16_t port,
                          RpcService* service);

    virtual ~TcpRpcServer() {}

    virtual seastar::future<> Start() override;

    virtual seastar::future<> Close() override;
};

}  // namespace net
}  // namespace blobstore
