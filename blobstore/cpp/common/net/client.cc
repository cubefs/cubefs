#include "client.h"

#include <netinet/tcp.h>

#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/defer.hh>
#include <utility>

#include "common/logger.h"
#include "tcp_connection.h"
#include "tcp_session.h"

namespace blobstore {
namespace net {

ClientStream::ClientStream(StreamPtr s, ClientPtr c)
    : stream_(std::move(s)), client_(std::move(c)), valid_(true) {}

ClientStream::ClientStream(ClientStream&& x)
    : stream_(std::move(x.stream_)), client_(std::move(x.client_)), valid_(std::move(x.valid_)) {}

ClientStream& ClientStream::operator=(ClientStream&& x) {
    if (&x != this) {
        stream_ = std::move(x.stream_);
        client_ = std::move(x.client_);
        valid_ = std::move(x.valid_);
    }
    return *this;
}

ClientStream::~ClientStream() {
    if (stream_ && client_) {
        (void)seastar::do_with(std::move(stream_), std::move(client_), std::move(valid_),
                               [](auto& s, auto& c, auto& v) { return c->Release(s, !v); });
    }
}

seastar::future<Status<Buffer>> ClientStream::ReadFrame(std::chrono::milliseconds timeout) {
    client_->UpdateTime();
    return stream_->ReadFrame(timeout);
}

seastar::future<Status<>> ClientStream::WriteFrame(const char* data, size_t size) {
    client_->UpdateTime();
    return stream_->WriteFrame(data, size);
}

seastar::future<Status<>> ClientStream::WriteFrame(std::vector<iovec> iov) {
    client_->UpdateTime();
    return stream_->WriteFrame(std::move(iov));
}

Client::Client(const seastar::socket_address& sa, const Option opt,
               std::chrono::milliseconds connect_timeout, net::BufferAllocator* allocator)
    : sa_(sa),
      opt_(opt),
      utime_(seastar::lowres_clock::now()),
      connect_timeout_(connect_timeout),
      allocator_(allocator) {
    if (connect_timeout_.count() == 0) connect_timeout_ = std::chrono::milliseconds(100);
}

seastar::future<Status<ClientStreamPtr>> Client::TryGetExistingStream() {
    Status<ClientStreamPtr> s;

    if (!sess_impl_ || !sess_impl_->sess_->Valid()) {
        s.SetCode(ErrCode::ErrNetwork).SetReason("net: no valid session");
        co_return s;
    }

    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeClient);
        co_return s;
    }

    // 检查是否有可用的流
    std::vector<StreamPtr> invalid_stream_vec;
    while (!sess_impl_->streams_.empty()) {
        auto stream = sess_impl_->streams_.front();
        sess_impl_->streams_.pop();

        if (!stream->Valid()) {
            invalid_stream_vec.emplace_back(std::move(stream));
            continue;
        }

        gate_.enter();
        ClientStreamPtr cs = std::make_unique<ClientStream>(std::move(stream), shared_from_this());
        s.SetValue(std::move(cs));
        if (invalid_stream_vec.size()) {
            gate_.enter();
            (void)seastar::do_with(std::move(invalid_stream_vec), std::move(shared_from_this()),
                                   [](auto& streams, auto& c) {
                                       return seastar::parallel_for_each(
                                                  streams.begin(), streams.end(),
                                                  [](auto stream) { return stream->Close(); })
                                           .then([c] { c->gate_.leave(); });
                                   });
        }
        co_return s;
    }

    if (invalid_stream_vec.size()) {
        gate_.enter();
        (void)seastar::do_with(std::move(invalid_stream_vec), std::move(shared_from_this()),
                               [](auto& streams, auto& c) {
                                   return seastar::parallel_for_each(
                                              streams.begin(), streams.end(),
                                              [](auto stream) { return stream->Close(); })
                                       .then([c] { c->gate_.leave(); });
                               });
    }

    // 尝试打开新流
    auto stream_result = co_await sess_impl_->sess_->OpenStream();
    if (!stream_result) {
        s.SetCode(stream_result.Code()).SetReason(stream_result.Reason());
        co_return s;
    }

    if (!gate_.try_enter()) {
        co_await stream_result.Value()->Close();
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeClient);
        co_return s;
    }

    ClientStreamPtr cs =
        std::make_unique<ClientStream>(std::move(stream_result.Value()), shared_from_this());
    s.SetValue(std::move(cs));
    co_return s;
}

void Client::CleanupOldSession() {
    if (old_sess_impl_) {
        if (!old_sess_impl_->sess_->Valid() ||
            old_sess_impl_->sess_->Streams() == old_sess_impl_->streams_.size()) {
            gate_.enter();  // 这里需要确保一定成功
            (void)seastar::do_with(
                std::move(old_sess_impl_), std::move(shared_from_this()), [](auto& impl, auto& c) {
                    return impl->sess_->Close().then([c]() { c->gate_.leave(); });
                });
        }
    }
}

seastar::future<Status<Client::SessionImplPtr>> Client::CreateNewConnection() {
    Status<Client::SessionImplPtr> s;

    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeClient);
        co_return s;
    }
    // 清理旧会话
    if (sess_impl_) {
        if (!sess_impl_->sess_->Valid()) {
            gate_.enter();
            (void)seastar::do_with(std::move(sess_impl_), std::move(shared_from_this()),
                                   [](auto& impl, auto& c) {
                                       return impl->sess_->Close().then([c] { c->gate_.leave(); });
                                   });
        } else {
            if (old_sess_impl_) {
                gate_.enter();
                (void)seastar::do_with(
                    std::move(old_sess_impl_), std::move(shared_from_this()),
                    [](auto& impl, auto& c) {
                        return impl->sess_->Close().then([c] { c->gate_.leave(); });
                    });
            }
            old_sess_impl_ = std::move(sess_impl_);
        }
        sess_impl_ = nullptr;
    }

    // 创建连接
    seastar::pollable_fd fd;
    seastar::timer<seastar::steady_clock_type> timer;

    try {
        fd = seastar::engine().make_pollable_fd(sa_, 0);
    } catch (const std::system_error& e) {
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    } catch (const std::exception& e) {
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    }

    // 设置连接超时
    timer.set_callback([&fd]() mutable { fd.close(); });
    timer.arm(connect_timeout_);

    try {
        co_await seastar::engine().posix_connect(fd, sa_, seastar::socket_address());

        // 设置 TCP_NODELAY
        int opt = 1;
        fd.get_file_desc().setsockopt(IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    } catch (const std::system_error& e) {
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
    } catch (const std::exception& e) {
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
    }

    timer.cancel();

    if (!s) {
        fd.close();
        co_return s;
    }

    // 创建会话
    auto conn = TcpConnection::MakeConnection(fd, sa_);
    auto sess_ptr = TcpSession::MakeSession(opt_, std::move(conn), true);
    auto sess_impl = seastar::make_lw_shared<Client::SessionImpl>();
    sess_impl->sess_ = std::move(sess_ptr);

    s.SetValue(std::move(sess_impl));
    co_return s;
}

seastar::future<Status<ClientStreamPtr>> Client::GetClientStream() {
    Status<ClientStreamPtr> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeClient);
        co_return s;
    }

    seastar::gate::holder holder(gate_);
    // 清理旧会话
    CleanupOldSession();
    co_await mu_.lock();
    auto unlock_guard = seastar::defer([this] { mu_.unlock(); });

    // 尝试获取现有流
    s = co_await TryGetExistingStream();
    if (s) {
        co_return s;
    }

    // 创建新连接
    auto conn_result = co_await CreateNewConnection();
    if (!conn_result) {
        s.SetCode(conn_result.Code()).SetReason(conn_result.Reason());
        co_return s;
    }
    sess_impl_ = std::move(conn_result.Value());
    unlock_guard.cancel();
    mu_.unlock();

    s = co_await TryGetExistingStream();
    co_return s;
}

seastar::future<> Client::Release(net::StreamPtr s, bool close) {
    auto defer = seastar::defer([this] { gate_.leave(); });
    if (gate_.is_closed() || close) {
        co_await s->Close();
        co_return;
    }
    SessionImplPtr sess_impl = sess_impl_;
    if (!s->Valid() || !sess_impl || sess_impl->sess_->ID() != s->SessID()) {
        co_await s->Close();
        co_return;
    }

    sess_impl->streams_.push(s);
    co_return;
}

seastar::lowres_clock::time_point Client::GetUtime() const { return utime_; }

void Client::UpdateTime() { utime_ = seastar::lowres_clock::now(); }

seastar::future<> Client::Close() {
    if (gate_.is_closed()) {
        co_return;
    }
    co_await gate_.close();
    SessionImplPtr impl = std::move(sess_impl_);
    SessionImplPtr old_impl = std::move(old_sess_impl_);
    std::vector<seastar::future<>> fu_vec;
    if (impl && impl->sess_) {
        auto fu = impl->sess_->Close();
        fu_vec.emplace_back(std::move(fu));
    }
    if (old_impl && old_impl->sess_) {
        auto fu = old_impl->sess_->Close();
        fu_vec.emplace_back(std::move(fu));
    }
    co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    co_return;
}

}  // namespace net
}  // namespace blobstore
