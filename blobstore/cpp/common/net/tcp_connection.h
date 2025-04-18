#pragma once

#include <seastar/core/fstream.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include "common/status.h"

namespace blobstore {
namespace net {

class TcpConnection {
    seastar::pollable_fd fd_;
    seastar::socket_address remote_address_;
    bool closed_;

   public:
    explicit TcpConnection(seastar::pollable_fd fd, seastar::socket_address remote);
    ~TcpConnection() {}
    seastar::future<Status<>> Write(seastar::net::packet&& p);
    seastar::future<Status<size_t>> Read(char* buffer, size_t size);
    seastar::future<Status<size_t>> ReadExactly(char* buffer, size_t size);
    void Close();
    inline seastar::socket_address LocalAddress() const {
        return fd_.get_file_desc().get_address();
    }
    inline seastar::socket_address RemoteAddress() const { return remote_address_; }

    static std::unique_ptr<TcpConnection> MakeConnection(seastar::pollable_fd fd,
                                                         seastar::socket_address remote);
};

using TcpConnectionPtr = std::unique_ptr<TcpConnection>;

}  // namespace net
}  // namespace blobstore
