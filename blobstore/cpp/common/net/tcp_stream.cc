#include "tcp_stream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include "byteorder.h"
#include "tcp_session.h"

namespace blobstore {
namespace net {

TcpStream::TcpStream(uint32_t id, uint8_t ver, uint32_t frame_size, uint32_t wnd_size,
                     seastar::weak_ptr<TcpSession> sess)
    : shard_(seastar::this_shard_id()),
      id_(id),
      sess_id_(sess->ID()),
      ver_(ver),
      frame_size_(frame_size),
      remote_wnd_(wnd_size),
      sess_(sess),
      local_addr_(sess->LocalAddress()),
      remote_addr_(sess->RemoteAddress()) {}

StreamPtr TcpStream::MakeStream(uint32_t id, uint8_t ver, uint32_t frame_size, uint32_t wnd_size,
                                seastar::weak_ptr<TcpSession> sess) {
    seastar::shared_ptr<TcpStream> stream =
        seastar::make_shared<TcpStream>(id, ver, frame_size, wnd_size, sess);
    return seastar::dynamic_pointer_cast<Stream, TcpStream>(stream);
}

bool TcpStream::Valid() const { return !gate_.is_closed() && sess_ && sess_->Valid(); }

seastar::future<Status<Buffer>> TcpStream::ReadFrameInternal(std::chrono::milliseconds timeout) {
    Status<seastar::temporary_buffer<char>> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    while (buffers_.empty()) {
        auto wait_res = co_await WaitRead(timeout);
        if (!wait_res) {
            s.SetCode(wait_res.Code()).SetReason(wait_res.Reason());
            co_return s;
        }
    }
    auto &b = buffers_.front();
    uint32_t n = static_cast<uint32_t>(b.size());
    buffer_size_ -= n;
    if (!sess_) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
        co_return s;
    }
    sess_->ReturnTokens(n);
    s.SetValue(std::move(b));
    buffers_.pop();
    recv_bytes_ += n;
    incr_ += n;
    if (incr_ >= kMaxStreamBufferSize / 2 || recv_bytes_ == n) {
        incr_ = 0;
        auto res = co_await SendWindowUpdate(recv_bytes_);
        if (!res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
        }
    }
    co_return s;
}

seastar::future<Status<>> TcpStream::WaitRead(std::chrono::milliseconds timeout) {
    Status<> s;
    seastar::timer<seastar::lowres_clock> wait_timer([this, &s] {
        r_cv_.signal();
        s.SetCode(ErrCode::ErrTimeout);
    });

    for (int i = 0; i < 2; i++) {
        if (gate_.is_closed() || !sess_) {
            s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
            break;
        }
        if (has_fin_) {
            if (buffers_.empty()) {
                s.SetCode(ErrCode::ErrEOF);
            }
            break;
        }
        if (!sess_->status_) {
            s = sess_->status_;
            break;
        }
        if (i > 0) break;

        if (timeout != std::chrono::milliseconds::zero() &&
            timeout != std::chrono::milliseconds::max()) {
            wait_timer.arm(timeout);
        }
        co_await r_cv_.wait();
        wait_timer.cancel();
        if (!s) {
            break;
        }
    }
    co_return s;
}

void TcpStream::PushData(seastar::temporary_buffer<char> data) {
    buffer_size_ += data.size();
    buffers_.emplace(std::move(data));
    r_cv_.signal();
}

void TcpStream::Fin() {
    has_fin_ = true;
    r_cv_.signal();
    wnd_cv_.broadcast();
}

seastar::future<Status<>> TcpStream::SendWindowUpdate(uint32_t consumed) {
    Status<> s;
    Frame f;
    f.ver = ver_;
    f.cmd = static_cast<uint8_t>(CmdType::UPD);
    f.sid = id_;
    auto data = seastar::temporary_buffer<char>(8);
    LittleEndian::PutUint32(data.get_write(), consumed);
    LittleEndian::PutUint32(data.get_write() + 4, kMaxStreamBufferSize);
    f.packet = std::move(seastar::net::packet(std::move(data)));
    if (!sess_) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
    } else {
        s = co_await sess_->WriteFrameInternal(std::move(f), TcpSession::ClassID::DATA);
    }
    co_return s;
}

seastar::future<Status<Buffer>> TcpStream::ReadFrame(std::chrono::milliseconds timeout) {
    if (shard_ == seastar::this_shard_id()) {
        auto s = co_await ReadFrameInternal(timeout);
        co_return s;
    }

    Status<Buffer> s;
    Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>> st;
    auto fn =
        [this,
         timeout]() -> seastar::future<Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>>> {
        Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>> s;
        auto st = co_await ReadFrameInternal(timeout);
        if (!st) {
            s.SetCode(st.Code()).SetReason(st.Reason());
            co_return s;
        }
        std::unique_ptr<Buffer> b = std::make_unique<Buffer>(std::move(st.Value()));
        s.SetValue(seastar::make_foreign<std::unique_ptr<Buffer>>(std::move(b)));
        co_return s;
    };
    st = co_await seastar::smp::submit_to(shard_, std::ref(fn));
    if (!st) {
        s.SetCode(st.Code()).SetReason(st.Reason());
        co_return s;
    }
    s.SetValue(foreign_buffer_copy(std::move(st.Value())));
    co_return s;
}

seastar::future<Status<>> TcpStream::WriteFrame(std::vector<iovec> iov) {
    Status<> s;
    auto fn = [this, iov = std::move(iov)]() mutable -> seastar::future<Status<>> {
        Status<> s;

        if (gate_.is_closed()) {
            s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
            co_return s;
        }
        if (has_fin_) {
            s.SetCode(ErrCode::ErrEOF);
            co_return s;
        }
        seastar::gate::holder holder(gate_);

        if (iov.size() >= IOV_MAX) {
            s.SetCode(ErrCode::ErrTooLarge).SetReason("net: iov size exceeds IOV_MAX");
            co_return s;
        }

        seastar::net::packet packet;
        for (int i = 0; i < iov.size(); ++i) {
            seastar::net::packet p = seastar::net::packet::from_static_data(
                reinterpret_cast<const char *>(iov[i].iov_base), iov[i].iov_len);
            packet.append(std::move(p));
        }
        if (packet.len() > frame_size_) {
            s.SetCode(ErrCode::ErrTooLarge).SetReason("net: packet size exceeds frame size");
            co_return s;
        } else if (packet.len() == 0) {
            co_return s;
        }

        int32_t inflight = static_cast<int32_t>(sent_bytes_ - remote_consumed_);
        int32_t win = static_cast<int32_t>(remote_wnd_) - inflight;
        while (inflight < 0 || win <= 0) {
            co_await wnd_cv_.wait();
            if (gate_.is_closed()) {
                s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
                co_return s;
            }
            if (has_fin_) {
                s.SetCode(ErrCode::ErrEOF);
                co_return s;
            }
            inflight = static_cast<int32_t>(sent_bytes_ - remote_consumed_);
            win = static_cast<int32_t>(remote_wnd_) - inflight;
        }
        sent_bytes_ += packet.len();
        Frame frame;
        frame.ver = ver_;
        frame.cmd = static_cast<uint8_t>(CmdType::PSH);
        frame.sid = id_;
        frame.packet = std::move(packet);
        if (!sess_) {
            s.SetCode(ErrCode::ErrNetworkPipe).SetReason(kErrorPipeSession);
        } else {
            s = co_await sess_->WriteFrameInternal(std::move(frame), TcpSession::ClassID::DATA);
        }
        co_return s;
    };
    s = co_await seastar::smp::submit_to(shard_, std::ref(fn));
    co_return s;
}

seastar::future<Status<>> TcpStream::WriteFrame(const char *b, size_t n) {
    iovec iov = {(void *)b, n};
    return WriteFrame({iov});
}

seastar::future<Status<>> TcpStream::WriteFrame(seastar::temporary_buffer<char> b) {
    auto s = co_await WriteFrame(b.get(), b.size());
    co_return s;
}

seastar::future<Status<>> TcpStream::WriteFrame(
    std::vector<seastar::temporary_buffer<char>> buffers) {
    Status<> s;
    std::vector<iovec> iov;
    int n = buffers.size();
    for (int i = 0; i < n; i++) {
        iovec io;
        io.iov_base = (void *)buffers[i].get_write();
        io.iov_len = buffers[i].size();
        iov.push_back(io);
    }
    s = co_await WriteFrame(std::move(iov));
    co_return s;
}

void TcpStream::Update(uint32_t consumed, uint32_t window) {
    remote_consumed_ = consumed;
    remote_wnd_ = window;
    wnd_cv_.broadcast();
}

seastar::future<> TcpStream::SessionClose() {
    if (gate_.is_closed()) {
        return seastar::make_ready_future<>();
    }
    r_cv_.signal();
    wnd_cv_.broadcast();
    return gate_.close();
}

seastar::future<> TcpStream::Close() {
    auto fn = [this]() -> seastar::future<> {
        if (gate_.is_closed()) {
            co_return;
        }
        auto fu = gate_.close();
        r_cv_.signal();
        wnd_cv_.broadcast();
        co_await std::move(fu);
        Frame frame;
        frame.ver = ver_;
        frame.cmd = static_cast<uint8_t>(CmdType::FIN);
        frame.sid = id_;
        if (sess_) {
            co_await sess_->WriteFrameInternal(std::move(frame), TcpSession::ClassID::CTRL);
        }
        if (buffer_size_ > 0) {
            if (sess_) {
                sess_->ReturnTokens(buffer_size_);
            }
            buffer_size_ = 0;
        }
        if (sess_) {
            sess_->streams_.erase(id_);
        }
        co_return;
    };
    co_await seastar::smp::submit_to(shard_, std::ref(fn));
    co_return;
}

}  // namespace net
}  // namespace blobstore
