#include <netinet/tcp.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>

#include "byteorder.h"
#include "common/logger.h"
#include "rpc.h"
#include "rpc_server.h"
#include "tcp_connection.h"
#include "tcp_session.h"

namespace blobstore {
namespace net {

TcpRpcServer::TcpRpcServer(const Option& opt, const std::string& host, uint16_t port)
    : opt_(opt), host_(host), port_(port) {}

seastar::future<> TcpRpcServer::HandleSession(net::SessionPtr sess) {
    if (gate_.is_closed()) {
        sess_mgr_.erase(sess->ID());
        co_await sess->Close();
        co_return;
    }

    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        auto res = co_await sess->AcceptStream();
        if (!res) {
            LOG_ERROR("accept stream error: {}, remote: {}", res, sess->RemoteAddress());
            break;
        }
        (void)HandleStream(res.Value());
    }
    sess_mgr_.erase(sess->ID());
    co_await sess->Close();
    co_return;
}

seastar::future<> TcpRpcServer::HandleStream(net::StreamPtr stream) {
    if (gate_.is_closed()) {
        co_await stream->Close();
        co_return;
    }
    seastar::gate::holder holder(gate_);
    proto::StreamCmd prev_cmd = proto::StreamCmd::NOT;
    while (!gate_.is_closed() && stream->Valid()) {
        auto res = co_await stream->ReadFrame();
        if (!res) {
            LOG_WARN("read rpc request header frame error: {}, remote: {}", res,
                     stream->RemoteAddress());
            break;
        }
        Buffer b = std::move(res.Value());

        RpcRequestHeader req_header;
        size_t body_offset = 0;
        if (!DeserializeRpcHeader(b, req_header, body_offset)) {
            LOG_WARN("parse rpc header error, remote: {}", stream->RemoteAddress());
            break;
        }
        if (req_header.Version() != BLOBSTORE_NET_RPC_HEADER_VERSION ||
            req_header.Magic() != BLOBSTORE_NET_RPC_HEADER_MAGIC) {
            LOG_WARN("bad rpc header version({}) or magic({}), remote: {}", req_header.Version(),
                     req_header.Magic(), stream->RemoteAddress());
            break;
        }

        proto::StreamCmd cmd = req_header.StreamCmd();
        RpcServerContext ctx(stream.get(), std::move(req_header));

        size_t body_size_in_frame = (b.size() > body_offset) ? (b.size() - body_offset) : 0;
        if (body_size_in_frame > 0) {
            Buffer body_buf = b.share(body_offset, body_size_in_frame);
            ctx.SetPendingBody(std::move(body_buf));
        }

        if (cmd == proto::StreamCmd::NOT) {
            if (prev_cmd != proto::StreamCmd::NOT) {
                LOG_WARN("invalid stream cmd={} in rpc header, remote: {}", (int)cmd,
                         stream->RemoteAddress());
                break;
            }
            if (ctx.GetRpcRequestHeader().ContentLength() == 0) {
                ctx.has_fin_ = true;
            }
            auto s = co_await HandleContext(&ctx);
            if (!s) {
                break;
            }
        } else if (cmd == proto::StreamCmd::SYN) {
            if (prev_cmd != proto::StreamCmd::NOT) {
                LOG_WARN("invalid stream cmd={} in rpc header, remote: {}", (int)cmd,
                         stream->RemoteAddress());
                break;
            }
            RpcResponseHeader resp;
            Buffer sent_buf(resp.ByteSizeLong());
            resp.SerializeToArray(sent_buf.get_write(), sent_buf.size());
            auto s = co_await stream->WriteFrame(std::move(sent_buf));
            if (!s) {
                LOG_WARN("response StreamCmd::SYN to remote: {} error: {}", stream->RemoteAddress(),
                         s);
                break;
            }
            prev_cmd = proto::StreamCmd::SYN;
            continue;
        } else if (cmd == proto::StreamCmd::PSH) {
            if (prev_cmd != proto::StreamCmd::SYN) {
                LOG_WARN("invalid stream cmd={} in rpc header, remote: {}", (int)cmd,
                         stream->RemoteAddress());
                break;
            }
            ctx.stream_ctx_ = true;
            auto s = co_await HandleContext(&ctx);
            if (!s) {
                break;
            }
            prev_cmd = proto::StreamCmd::NOT;
        } else {
            LOG_WARN("invalid stream cmd={} in rpc header, remote: {}", (int)cmd,
                     stream->RemoteAddress());
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> TcpRpcServer::Start() {
    seastar::socket_address sa(seastar::ipv4_addr(host_, port_));

    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    try {
        seastar::file_desc fd =
            seastar::file_desc::socket(sa.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        fd.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1);
        fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);

        fd.bind(sa.u.sa, sa.length());
        fd.listen(1024);
        fd_ = seastar::pollable_fd(std::move(fd));
    } catch (std::exception& e) {
        LOG_ERROR("bind or listen error: {}", e.what());
        co_return;
    }

    LOG_INFO("listen succeed on host: {} port: {}", host_, port_);
    while (!gate_.is_closed()) {
        try {
            auto ar = co_await fd_.accept();
            auto fd = std::get<0>(ar);
            auto remote_addr = std::get<1>(ar);
            int val = 1;
            fd.get_file_desc().setsockopt(IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
            int flags = fcntl(fd.get_file_desc().get(), F_GETFL, 0);
            fcntl(fd.get_file_desc().get(), F_SETFL, flags | O_NONBLOCK);
            auto conn = TcpConnection::MakeConnection(fd, remote_addr);
            auto sess = TcpSession::MakeSession(opt_, std::move(conn), false);
            sess_mgr_[sess->ID()] = sess;
            (void)HandleSession(sess);
        } catch (std::exception& e) {
            LOG_ERROR("accept error: {}", e.what());
            break;
        }
    }
    co_return;
}

seastar::future<> TcpRpcServer::Close() {
    if (gate_.is_closed()) {
        co_return;
    }
    fd_.close();

    std::vector<seastar::future<>> fu_vec;
    auto fu = gate_.close();
    fu_vec.emplace_back(std::move(fu));

    for (int i = 0; i < 2; i++) {
        std::unordered_map<uint64_t, SessionPtr> tmp_mgr = std::move(sess_mgr_);
        for (auto it = tmp_mgr.begin(); it != tmp_mgr.end(); it++) {
            auto fu = it->second->Close();
            fu_vec.emplace_back(std::move(fu));
        }
        if (!fu_vec.empty()) {
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
            fu_vec.clear();
        }
    }
    co_return;
}

}  // namespace net
}  // namespace blobstore
