#include "rpc_client.h"

#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>

#include "common/logger.h"
#include "common/net/rpc_stream.h"

namespace blobstore {
namespace net {

ClientMgr::ClientMgr(std::chrono::seconds idle_timeout) noexcept {
    (void)RecycleLoop(idle_timeout);
}

ClientPtr ClientMgr::GetClient(const seastar::socket_address &sa) {
    ClientPtr client;
    auto iter = new_client_map_.find(sa);
    if (iter != new_client_map_.end()) {
        client = iter->second;
    } else {
        iter = client_map_.find(sa);
        if (iter != client_map_.end()) {
            client = iter->second;
        }
    }
    return client;
}

seastar::future<Status<ClientStreamPtr>> ClientMgr::GetClientStream(
    seastar::socket_address sa, Option opt, std::chrono::milliseconds connect_timeout,
    BufferAllocator *allocator) {
    Status<ClientStreamPtr> s;
    if (gate_.is_closed()) {
        s.SetCode(ErrCode::ErrNetworkPipe).SetReason("net: client mgr has closed");
        LOG_ERROR("client_mgr on shard={} is closed, sa={}", seastar::this_shard_id(), sa);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    auto c = GetClient(sa);
    if (!c) {
        c = seastar::make_lw_shared<Client>(sa, opt, connect_timeout, allocator);
        new_client_map_[sa] = c;
    }
    auto res = co_await c->GetClientStream();
    if (!res) {
        LOG_ERROR("get client stream shard={} error={}, sa={}", seastar::this_shard_id(), res, sa);
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_return s;
    }
    s.SetValue(std::move(res.Value()));
    co_return s;
}

seastar::future<> ClientMgr::RecycleLoop(std::chrono::seconds idle_timeout) {
    seastar::gate::holder holder(gate_);

    seastar::timer<seastar::lowres_clock> timer([this] { recyle_cv_.signal(); });
    timer.arm_periodic(idle_timeout / 3);
    while (!gate_.is_closed()) {
        co_await recyle_cv_.wait();
        if (gate_.is_closed()) {
            break;
        }
        for (auto it = new_client_map_.begin(); it != new_client_map_.end();) {
            client_map_[it->first] = it->second;
            new_client_map_.erase(it++);
        }

        std::vector<seastar::future<>> fu_vec;
        std::vector<ClientPtr> need_deleted_vec;

        auto iter = client_map_.begin();
        while (iter != client_map_.end()) {
            ClientPtr client = iter->second;
            auto now = seastar::lowres_clock::now();
            auto diff = std::chrono::duration_cast<std::chrono::seconds>(now - client->GetUtime());
            if (diff >= idle_timeout) {
                need_deleted_vec.push_back(client);
                client_map_.erase(iter++);
                auto fu = client->Close();
                LOG_INFO("close client {}, because the idle time is {}s", client->RemoteAddress(),
                         diff.count());
                fu_vec.emplace_back(std::move(fu));
            } else {
                iter++;
            }

            co_await seastar::coroutine::maybe_yield();
        }
        if (!fu_vec.empty()) {
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        }
    }
    timer.cancel();
    co_return;
}

seastar::future<> ClientMgr::Close() {
    if (gate_.is_closed()) {
        co_return;
    }

    recyle_cv_.signal();
    co_await gate_.close();

    std::vector<seastar::future<>> fu_vec;
    std::unordered_map<seastar::socket_address, ClientPtr> new_map = std::move(new_client_map_);
    std::unordered_map<seastar::socket_address, ClientPtr> client_map = std::move(client_map_);
    for (auto it : new_map) {
        auto fu = it.second->Close();
        fu_vec.emplace_back(std::move(fu));
    }

    for (auto it : client_map) {
        auto fu = it.second->Close();
        fu_vec.emplace_back(std::move(fu));
    }
    co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    co_return;
}

RpcClientContext::~RpcClientContext() {
    if (client_stream_) {
        client_stream_->SetValid(last_status_ ? true : false);
    }
}

seastar::future<Status<RpcResponseHeader>> RpcClientContext::ReadHeader(
    std::chrono::milliseconds timeout) noexcept {
    Status<RpcResponseHeader> s;
    RpcResponseHeader resp_header;
    if (!last_status_) {
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }
    auto res = co_await client_stream_->ReadFrame(timeout);
    if (!res) {
        last_status_.SetCode(res.Code()).SetReason(res.Reason());
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_return s;
    }

    Buffer b = std::move(res.Value());
    size_t body_offset = 0;
    if (!DeserializeRpcHeader(b, resp_header, body_offset)) {
        last_status_.SetCode(ErrCode::ErrNetworkProtocol)
            .SetReason("net: deserialize header error");
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }

    if (resp_header.Version() != BLOBSTORE_NET_RPC_HEADER_VERSION ||
        resp_header.Magic() != BLOBSTORE_NET_RPC_HEADER_MAGIC) {
        last_status_.SetCode(ErrCode::ErrNetworkProtocol)
            .SetReason("net: invalid version or magic");
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }

    size_t body_size_in_frame = (b.size() > body_offset) ? (b.size() - body_offset) : 0;
    if (body_size_in_frame > 0) {
        Buffer body_buf = b.share(body_offset, body_size_in_frame);
        SetPendingBody(std::move(body_buf));
    }

    s.SetValue(std::move(resp_header));
    co_return s;
}

seastar::future<Status<Buffer>> RpcClientContext::ReadBody(
    std::chrono::milliseconds timeout) noexcept {
    Status<Buffer> s;
    if (!last_status_) {
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }

    // has pending body
    if (has_pending_body_ && pending_body_.size() > 0) {
        has_pending_body_ = false;
        s.SetValue(std::move(pending_body_));
        pending_body_ = Buffer();
        co_return s;
    }

    auto res = co_await client_stream_->ReadFrame(timeout);
    if (!res) {
        last_status_.SetCode(res.Code()).SetReason(res.Reason());
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_return s;
    }
    co_return res;
}

seastar::future<Status<>> RpcClientContext::WriteHeader(RpcRequestHeader req_header) noexcept {
    Status<> s;
    if (!last_status_) {
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }

    req_header.SetVersion(BLOBSTORE_NET_RPC_HEADER_VERSION);
    req_header.SetMagic(BLOBSTORE_NET_RPC_HEADER_MAGIC);
    Buffer b = SerializeRpcHeader(req_header);
    if (b.size() == 0) {
        last_status_.SetCode(ErrCode::ErrInvalid).SetReason("net: serialize header error");
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }

    s = co_await client_stream_->WriteFrame(b.get(), b.size());
    if (!s) {
        last_status_.SetCode(s.Code()).SetReason(s.Reason());
    }
    co_return s;
}

seastar::future<Status<>> RpcClientContext::WriteBody(Buffer body) noexcept {
    auto s = co_await WriteBody(body.get(), body.size());
    co_return s;
}

seastar::future<Status<>> RpcClientContext::WriteBody(const char *b, size_t n) noexcept {
    Status<> s;
    if (!last_status_) {
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }
    s = co_await client_stream_->WriteFrame(b, n);
    if (!s) {
        last_status_.SetCode(s.Code()).SetReason(s.Reason());
    }
    co_return s;
}

seastar::future<Status<>> RpcClientContext::WriteBody(std::vector<iovec> iovs) noexcept {
    Status<> s;
    if (!last_status_) {
        s.SetCode(last_status_.Code()).SetReason(last_status_.Reason());
        co_return s;
    }
    s = co_await client_stream_->WriteFrame(std::move(iovs));
    if (!s) {
        last_status_.SetCode(s.Code()).SetReason(s.Reason());
    }
    co_return s;
}

seastar::future<std::unique_ptr<RpcClient>> RpcClient::MakeRpcClient(
    const Option opt, std::chrono::milliseconds connect_timeout, std::chrono::seconds idle_timeout,
    BufferAllocator *allocator) {
    std::unique_ptr<RpcClient> instance(new RpcClient());
    instance->opt_ = opt;
    instance->connect_timeout_ = connect_timeout;
    instance->idle_timeout_ = idle_timeout;
    instance->allocator_ = allocator;
    instance->client_mgr_vec_.resize(seastar::smp::count);
    co_await seastar::smp::invoke_on_all([ins = instance.get(), idle_timeout] {
        auto foreign_ptr = seastar::make_foreign(std::make_unique<ClientMgr>(idle_timeout));
        unsigned shard = foreign_ptr.get_owner_shard();
        ins->client_mgr_vec_[shard] = std::move(foreign_ptr);
    });
    co_return instance;
}

seastar::future<Status<std::unique_ptr<RpcClientContext>>> RpcClient::MakeRpcClientContext(
    seastar::socket_address sa) {
    Status<std::unique_ptr<RpcClientContext>> s;
    size_t h = std::hash<seastar::socket_address>{}(sa);
    unsigned idx = h % client_mgr_vec_.size();
    unsigned shard = client_mgr_vec_[idx].get_owner_shard();
    ClientMgr *mgr = client_mgr_vec_[idx].get();

    auto fn = [this, sa, mgr]() -> seastar::future<Status<seastar::foreign_ptr<ClientStreamPtr>>> {
        Status<seastar::foreign_ptr<ClientStreamPtr>> s;

        auto res = co_await mgr->GetClientStream(sa, opt_, connect_timeout_, allocator_);
        if (!res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
            co_return s;
        }
        s.SetValue(seastar::make_foreign(std::move(res.Value())));
        co_return s;
    };

    auto res = co_await seastar::smp::submit_to(shard, std::ref(fn));
    if (!res) {
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_return s;
    }
    std::unique_ptr<RpcClientContext> ctx(new RpcClientContext(sa, std::move(res.Value())));
    s.SetValue(std::move(ctx));
    co_return std::move(s);
}

seastar::future<Status<std::unique_ptr<RpcClientContext>>> RpcClient::MakeRpcClientContext(
    std::string_view host, uint16_t port) {
    Status<std::unique_ptr<RpcClientContext>> s;
    seastar::net::inet_address addr;
    try {
        addr = co_await seastar::net::dns::resolve_name(seastar::sstring(host));
    } catch (std::system_error &e) {
        LOG_ERROR("resolve host={} error: {}", host, e.what());
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    } catch (std::exception &e) {
        LOG_ERROR("resolve host={} error: {}", host, e.what());
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    }
    seastar::socket_address sa(addr, port);
    s = co_await MakeRpcClientContext(std::move(sa));
    co_return s;
}

seastar::future<Status<std::unique_ptr<RpcClientStream>>> RpcClient::CreateStream(
    std::string_view trace_id, seastar::socket_address sa, int32_t remote_path_index,
    const std::string &remote_path, const google::protobuf::Message *para) {
    Status<std::unique_ptr<RpcClientStream>> s;
    auto ctx_res = co_await MakeRpcClientContext(sa);
    if (!ctx_res) {
        s.SetCode(ctx_res.Code()).SetReason(ctx_res.Reason());
        co_return s;
    }

    std::unique_ptr<RpcClientContext> ctx = std::move(ctx_res.Value());

    RpcRequestHeader req_header;
    req_header.SetTraceid(trace_id.data());
    req_header.SetStreamCmd(proto::StreamCmd::SYN);
    req_header.SetRemotePath(remote_path);
    req_header.SetRemotePathIndex(remote_path_index);
    req_header.SetContentLength(0);

    if (para != nullptr) {
        size_t para_size = para->ByteSizeLong();
        std::string para_data;
        para_data.resize(para_size);
        if (!para->SerializeToArray(&para_data[0], para_size)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("net: serialize stream parameter failed");
            co_return s;
        }
        req_header.SetParameter(para_data);
    }
    auto write_res = co_await ctx->WriteHeader(std::move(req_header));
    if (!write_res) {
        s.SetCode(write_res.Code()).SetReason(write_res.Reason());
        co_return s;
    }
    auto read_res = co_await ctx->ReadHeader();
    if (!read_res) {
        s.SetCode(read_res.Code()).SetReason(read_res.Reason());
        co_return s;
    }

    RpcResponseHeader resp_header = std::move(read_res.Value());
    if (resp_header.Status() != static_cast<int32_t>(ErrCode::OK)) {
        s.SetCode(ErrCode::ErrNetworkProtocol).SetReason(resp_header.Reason());
        co_return s;
    }
    auto stream_client = std::make_unique<RpcClientStream>(std::move(ctx));
    s.SetValue(std::move(stream_client));
    co_return s;
}

seastar::future<Status<std::unique_ptr<RpcClientStream>>> RpcClient::CreateStream(
    std::string_view trace_id, std::string_view host, uint16_t port, int32_t remote_path_index,
    const std::string &remote_path, const google::protobuf::Message *para) {
    Status<std::unique_ptr<RpcClientStream>> s;
    seastar::net::inet_address addr;
    try {
        addr = co_await seastar::net::dns::resolve_name(seastar::sstring(host));
    } catch (std::system_error &e) {
        LOG_ERROR("{} resolve host={} error: {}", trace_id, host, e.what());
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    } catch (std::exception &e) {
        LOG_ERROR("{} resolve host={} error: {}", trace_id, host, e.what());
        s.SetCode(ErrCode::ErrNetwork).SetReason(e.what());
        co_return s;
    }
    const seastar::socket_address sa(addr, port);
    s = co_await CreateStream(trace_id, sa, remote_path_index, remote_path, para);
    if (!s) {
        LOG_ERROR("{} create stream failed, {}", trace_id, s.Reason());
        co_return s;
    }
    co_return s;
}

seastar::future<> RpcClient::Close() {
    auto fn = [this]() -> seastar::future<> {
        unsigned shard = seastar::this_shard_id();
        co_await client_mgr_vec_[shard]->Close();
        co_return;
    };
    co_await seastar::smp::invoke_on_all(std::ref(fn));
    co_return;
}

}  // namespace net
}  // namespace blobstore
