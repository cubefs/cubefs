#include "tcp_connection.h"

#include <netinet/tcp.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/posix.hh>

namespace blobstore {
namespace net {

TcpConnection::TcpConnection(seastar::pollable_fd fd, seastar::socket_address remote)
    : fd_(std::move(fd)), remote_address_(remote), closed_(false) {}

seastar::future<Status<>> TcpConnection::Write(seastar::net::packet&& p) {
    return fd_.write_all(p).then_wrapped([](auto fu) -> Status<> {
        Status<> s;
        if (fu.failed()) {
            auto eptr = fu.get_exception();
            try {
                rethrow_exception(eptr);
            } catch (std::system_error& e) {
                s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            } catch (std::exception& e) {
                s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            }
            return s;
        }
        return s;
    });
}

seastar::future<Status<size_t>> TcpConnection::Read(char* buffer, size_t len) {
    return fd_.read_some(buffer, len).then_wrapped([](auto fu) -> Status<size_t> {
        Status<size_t> s;
        if (fu.failed()) {
            auto eptr = fu.get_exception();
            try {
                rethrow_exception(eptr);
            } catch (std::system_error& e) {
                s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            } catch (std::exception& e) {
                s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            }
            return s;
        }
        s.SetValue(fu.get());
        return s;
    });
}

seastar::future<Status<size_t>> TcpConnection::ReadExactly(char* buffer, size_t len) {
    Status<size_t> s;
    size_t n = 0;
    while (n < len) {
        try {
            auto bytes = co_await fd_.read_some(buffer + n, len - n);
            n += bytes;
            if (bytes == 0) {  // the conn has been closed
                s.SetValue(0);
                break;
            }
        } catch (std::system_error& e) {
            s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            co_return s;
        } catch (std::exception& e) {
            s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
            co_return s;
        }
    }
    s.SetValue(n);
    co_return s;
}

void TcpConnection::Close() {
    if (!closed_) {
        closed_ = true;
        fd_.shutdown(SHUT_RDWR);
    }
}

std::unique_ptr<TcpConnection> TcpConnection::MakeConnection(seastar::pollable_fd fd,
                                                             seastar::socket_address remote) {
    return std::make_unique<TcpConnection>(std::move(fd), remote);
}

}  // namespace net
}  // namespace blobstore
