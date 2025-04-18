#include "tcp_session.h"

#include <limits.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "byteorder.h"
#include "common/util.h"

namespace blobstore {
namespace net {

static const size_t default_accept_backlog = 1024;

std::atomic<uint64_t> TcpSession::session_id_ = 1;

class DefaultBufferAllocator : public BufferAllocator {
   public:
    virtual ~DefaultBufferAllocator() {}
    seastar::temporary_buffer<char> Allocate(size_t len) override {
        size_t origin_len = len;
        len = std::max(len, static_cast<size_t>(4096));
        auto buf = seastar::temporary_buffer<char>::aligned(4096, len);
        buf.trim(origin_len);
        return buf;
    }
};

static DefaultBufferAllocator defaultAllocator;

TcpSession::TcpSession(const Option &opt, TcpConnectionPtr conn, bool client,
                       BufferAllocator *allocator)
    : opt_(opt),
      sess_id_(TcpSession::session_id_++),
      conn_(std::move(conn)),
      client_(client),
      allocator_(allocator ? allocator : dynamic_cast<BufferAllocator *>(&defaultAllocator)),
      next_id_((client ? 1 : 0)),
      accept_sem_(default_accept_backlog),
      tokens_(kMaxReceiveBufferSize) {
    if (opt.max_frame_size > kMaxFrameSize) {
        opt_.max_frame_size = kMaxFrameSize;
    }
}

TcpSession::~TcpSession() { ClearWriteq(); }

SessionPtr TcpSession::MakeSession(Option opt, TcpConnectionPtr conn, bool client,
                                   BufferAllocator *allocator) {
    auto sess_ptr = seastar::make_shared<TcpSession>(opt, std::move(conn), client, allocator);
    (void)sess_ptr->RecvLoop();
    (void)sess_ptr->SendLoop();
    sess_ptr->StartKeepalive();
    return seastar::dynamic_pointer_cast<Session, TcpSession>(sess_ptr);
}

seastar::future<> TcpSession::HandleSyn(Frame f) noexcept {
    auto it = streams_.find(f.sid);
    if (it != streams_.end()) {
        co_return;
    }
    co_await accept_sem_.wait();
    if (gate_.is_closed() || !status_) {
        co_return;
    }
    StreamPtr stream = TcpStream::MakeStream(f.sid, f.ver, opt_.max_frame_size,
                                             kMaxStreamBufferSize, weak_from_this());
    streams_[f.sid] = seastar::dynamic_pointer_cast<TcpStream, Stream>(stream);
    accept_q_.emplace(stream);
    accept_cv_.signal();
    co_return;
}

void TcpSession::HandleFin(const Frame &f) noexcept {
    auto it = streams_.find(f.sid);
    if (it != streams_.end()) {
        it->second->Fin();
    }
}

seastar::future<> TcpSession::HandlePsh(
    Frame f, uint32_t len, seastar::timer<seastar::lowres_clock> *read_timer) noexcept {
    if (len == 0) {
        co_return;
    }
    auto buf = allocator_->Allocate(static_cast<size_t>(len));
    if (read_timer) {
        read_timer->arm(3 * opt_.keep_alive_interval);
    }
    auto s = co_await conn_->ReadExactly(buf.get_write(), len);
    if (read_timer) {
        read_timer->cancel();
    }
    if (!s || s.Value() == 0) {
        if (!s) {
            SetStatus(s.Code(), s.Reason());
        } else {
            SetStatus(static_cast<ErrCode>(ECONNRESET));
        }
        co_return;
    }
    auto it = streams_.find(f.sid);
    if (it != streams_.end()) {
        it->second->PushData(std::move(buf));
        tokens_ -= len;
    }
    co_return;
}

seastar::future<> TcpSession::HandleUpd(
    Frame f, uint32_t len, seastar::timer<seastar::lowres_clock> *read_timer) noexcept {
    if (len != 8) {
        SetStatus(static_cast<ErrCode>(EINVAL));
        co_return;
    }
    char buf[8];
    if (read_timer) {
        read_timer->arm(3 * opt_.keep_alive_interval);
    }
    auto s = co_await conn_->ReadExactly(buf, sizeof(buf));
    if (read_timer) {
        read_timer->cancel();
    }
    if (!s || s.Value() == 0) {
        if (!s) {
            SetStatus(s.Code(), s.Reason());
        } else {
            SetStatus(static_cast<ErrCode>(ECONNRESET));
        }
        co_return;
    }
    auto it = streams_.find(f.sid);
    if (it != streams_.end()) {
        uint32_t consumed = LittleEndian::Uint32(buf);
        uint32_t window = LittleEndian::Uint32(&buf[4]);
        it->second->Update(consumed, window);
    }
    co_return;
}

seastar::future<> TcpSession::RecvLoop() {
    char hdr[STREAM_HEADER_SIZE];
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    seastar::timer<seastar::lowres_clock> pong_timer;
    pong_timer.set_callback([this] {
        SetStatus(static_cast<ErrCode>(ETIMEDOUT));
        conn_->Close();
    });

    while (!gate_.is_closed() && status_) {
        bool wait_pong = opt_.keep_alive_enable && opt_.keep_alive_interval.count() > 0 && client_;
        if (tokens_ < 0) {
            co_await token_cv_.wait();
            continue;
        }

        if (wait_pong) {
            pong_timer.arm(3 * opt_.keep_alive_interval);
        }
        auto s = co_await conn_->ReadExactly(hdr, sizeof(hdr));
        if (wait_pong) {
            pong_timer.cancel();
        }
        if (gate_.is_closed() || !status_) {
            break;
        }
        if (!s) {
            SetStatus(s.Code(), s.Reason());
            break;
        }

        if (s.Value() == 0) {
            SetStatus(static_cast<ErrCode>(ECONNRESET));
            break;
        }
        Frame f;
        uint32_t len = f.Unmarshal(hdr);
        if (f.ver != 2) {
            SetStatus(static_cast<ErrCode>(EBADMSG));
            break;
        }
        CmdType type = static_cast<CmdType>(f.cmd);
        if (type != CmdType::PSH && type != CmdType::UPD && len != 0) {
            SetStatus(static_cast<ErrCode>(EBADMSG));
            break;
        }
        switch (type) {
            case CmdType::PING:
                WritePingPong(false);
                break;
            case CmdType::PONG:
                break;
            case CmdType::SYN:
                co_await HandleSyn(std::move(f));
                break;
            case CmdType::FIN:
                HandleFin(f);
                break;
            case CmdType::PSH:
                co_await HandlePsh(std::move(f), len, (wait_pong ? &pong_timer : nullptr));
                break;
            case CmdType::UPD:
                co_await HandleUpd(std::move(f), len, (wait_pong ? &pong_timer : nullptr));
                break;
            default:
                SetStatus(static_cast<ErrCode>(EINVAL));
                break;
        }
    }
    conn_->Close();
    accept_cv_.signal();
    w_cv_.signal();
    co_await CloseAllStreams();
    co_return;
}

seastar::future<> TcpSession::SendLoop() {
    static uint32_t max_packet_num = IOV_MAX;
    seastar::timer<seastar::lowres_clock> write_timer;
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    write_timer.set_callback([this] {
        SetStatus(static_cast<ErrCode>(ETIMEDOUT));
        conn_->Close();
    });

    std::queue<TcpSession::write_request *> sent_q;

    while (!gate_.is_closed() && status_) {
        Status<> s;
        seastar::net::packet packet;
        uint32_t packet_n = 0;
        std::optional<TcpSession::write_request *> ping_req;

        for (int i = 0; i < write_q_.size(); i++) {
            std::queue<write_request *> &req_queue = write_q_[i];
            while (!req_queue.empty() && packet_n < max_packet_num) {
                write_request *req = req_queue.front();
                if (req->frame.cmd == static_cast<uint8_t>(CmdType::PING)) {
                    req_queue.pop();
                    if (!ping_req) {
                        ping_req = req;
                    } else {
                        delete req;
                    }
                    continue;
                }
                if (req->frame.packet.nr_frags() > 0) {
                    packet_n += 1 + req->frame.packet.nr_frags();
                } else {
                    packet_n += 1;
                }
                if (packet_n > max_packet_num) {
                    break;
                }
                req_queue.pop();
                sent_q.emplace(req);
                Buffer hdr(STREAM_HEADER_SIZE);
                req->frame.MarshalTo(hdr.get_write());
                packet = seastar::net::packet(std::move(packet), std::move(hdr));
                if (req->frame.packet.nr_frags() > 0) {
                    packet.append(std::move(req->frame.packet));
                }
            }
        }

        if (ping_req) {
            TcpSession::write_request *req = ping_req.value();
            if (packet.len() == 0) {
                seastar::temporary_buffer<char> hdr(STREAM_HEADER_SIZE);
                req->frame.MarshalTo(hdr.get_write());
                packet = seastar::net::packet(std::move(packet), std::move(hdr));
            }
            delete req;
            ping_req.reset();
        }
        if (packet.len() > 0) {
            if (opt_.write_timeout.count() > 0) {
                write_timer.arm(opt_.write_timeout);
            }
            s = co_await conn_->Write(std::move(packet));
            write_timer.cancel();
        }
        while (!sent_q.empty()) {
            write_request *req = sent_q.front();
            sent_q.pop();
            if (!req->pr) {  // maybe this is a pong request
                delete req;
            } else {
                Status<> st = s;
                req->pr.value().set_value(std::move(st));
            }
        }

        if (!s) {
            SetStatus(s);
            break;
        }

        if (write_q_[0].empty() && write_q_[1].empty() && !gate_.is_closed() && status_) {
            co_await w_cv_.wait();
        }
    }

    conn_->Close();
    auto fu = CloseAllStreams();
    accept_cv_.signal();
    accept_sem_.signal();
    token_cv_.signal();
    SetStatus(static_cast<ErrCode>(EPIPE));
    ClearWriteq();
    keepalive_timer_.cancel();
    co_await std::move(fu);
    co_return;
}

void TcpSession::ClearWriteq() {
    for (int i = 0; i < 2; i++) {
        while (!write_q_[i].empty()) {
            write_request *req = write_q_[i].front();
            write_q_[i].pop();
            if (req->pr) {
                Status<> s = status_;
                req->pr.value().set_value(std::move(s));
            } else {
                delete req;
            }
        }
    }
}

void TcpSession::StartKeepalive() {
    if (client_ && opt_.keep_alive_enable && opt_.keep_alive_interval.count() > 0) {
        uint32_t keep_alive_interval_ms = opt_.keep_alive_interval.count() * 1000;
        keep_alive_interval_ms += GetRandomNumber<unsigned>(0, keep_alive_interval_ms);
        keepalive_timer_.set_callback([this]() { WritePingPong(true); });
        keepalive_timer_.arm_periodic(std::chrono::milliseconds(keep_alive_interval_ms));
    }
}

void TcpSession::ReturnTokens(uint32_t n) {
    tokens_ += n;
    if (tokens_ > 0) {
        token_cv_.signal();
    }
}

seastar::future<Status<>> TcpSession::WriteFrameInternal(Frame f, ClassID classid) {
    Status<> s;
    if (gate_.is_closed()) {
        s.SetCode(EPIPE);
        co_return s;
    }
    if (!status_) {
        s = status_;
        co_return s;
    }

    seastar::gate::holder holder(gate_);

    std::unique_ptr<write_request> req(new write_request());

    req->frame = std::move(f);
    req->pr = seastar::promise<Status<>>();

    write_q_[static_cast<int>(classid)].emplace(req.get());
    w_cv_.signal();
    s = co_await req->pr.value().get_future();
    co_return s;
}

void TcpSession::WritePingPong(bool ping) {
    if (gate_.is_closed() || !status_) {
        return;
    }

    write_request *req = new write_request();
    req->frame.ver = 2;
    req->frame.cmd =
        ping ? static_cast<uint8_t>(CmdType::PING) : static_cast<uint8_t>(CmdType::PONG);
    req->frame.sid = 0;

    write_q_[static_cast<int>(ClassID::DATA)].emplace(req);
    w_cv_.signal();
    return;
}

seastar::future<Status<StreamPtr>> TcpSession::OpenStream() {
    Status<StreamPtr> s;
    if (gate_.is_closed()) {
        s.SetCode(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    uint32_t id = next_id_ + 2;
    if (id < next_id_) {
        s.SetCode(ENOSR);
        co_return s;
    }
    next_id_ = id;

    auto stream =
        TcpStream::MakeStream(id, 2, opt_.max_frame_size, kMaxStreamBufferSize, weak_from_this());
    Frame frame;
    frame.ver = 2;
    frame.cmd = static_cast<uint8_t>(CmdType::SYN);
    frame.sid = id;
    auto st = co_await WriteFrameInternal(std::move(frame), ClassID::CTRL);
    if (!st) {
        s.SetCode(st.Code()).SetReason(st.Reason());
        co_return s;
    }
    if (gate_.is_closed()) {
        s.SetCode(EPIPE);
        co_return s;
    }
    if (!status_) {
        s.SetCode(status_.Code()).SetReason(status_.Reason());
        co_return s;
    }
    streams_[id] = seastar::dynamic_pointer_cast<TcpStream, Stream>(stream);
    s.SetValue(stream);
    co_return s;
}

seastar::future<Status<StreamPtr>> TcpSession::AcceptStream() {
    Status<StreamPtr> s;
    if (gate_.is_closed()) {
        s.SetCode(EPIPE).SetReason("tcp session is closing");
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    for (;;) {
        if (gate_.is_closed()) {
            s.SetCode(EPIPE).SetReason("tcp session is closing");
            break;
        }
        if (!status_.OK()) {
            s.SetCode(status_.Code()).SetReason(status_.Reason());
            break;
        }
        if (accept_q_.empty()) {
            co_await accept_cv_.wait();
            continue;
        }
        break;
    }
    if (!accept_q_.empty() && s) {
        StreamPtr stream = accept_q_.front();
        accept_q_.pop();
        accept_sem_.signal();
        s.SetValue(stream);
    }
    co_return s;
}

seastar::future<> TcpSession::Close() {
    if (!gate_.is_closed()) {
        auto fu = gate_.close();
        w_cv_.signal();
        accept_sem_.signal();
        token_cv_.signal();
        accept_cv_.signal();
        keepalive_timer_.cancel();
        conn_->Close();
        co_await std::move(fu);
        co_await CloseAllStreams();
    }
    co_return;
}

seastar::future<> TcpSession::CloseAllStreams() {
    std::vector<seastar::future<>> fu_vec;
    std::unordered_map<uint32_t, TcpStreamPtr> streams = std::move(streams_);
    uint32_t n = 0;
    for (auto &iter : streams) {
        auto fu = iter.second->SessionClose();
        fu_vec.emplace_back(std::move(fu));
        if ((++n) % 128 == 0) {
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
            fu_vec.clear();
        }
        co_await seastar::coroutine::maybe_yield();
    }
    std::queue<StreamPtr> tmp = std::move(accept_q_);
    if (fu_vec.size()) {
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
    co_return;
}

}  // namespace net
}  // namespace blobstore
