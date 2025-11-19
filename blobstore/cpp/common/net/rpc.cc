#include "rpc.h"

#include <isa-l.h>

#include <seastar/core/coroutine.hh>

#include "common/logger.h"
#include "common/net/byteorder.h"
#include "rpc_stream.h"

namespace blobstore {
namespace net {

RpcServerContext::RpcServerContext(Stream* stream, RpcRequestHeader req_header)
    : stream_(stream),
      req_header_(std::move(req_header)),
      trace_(req_header_.Traceid()),
      recv_len_(0),
      has_pending_body_(false) {}

seastar::future<Status<>> RpcServerContext::WriteBody(const char* b, size_t n) {
    return stream_->WriteFrame(b, n);
}

seastar::future<Status<>> RpcServerContext::WriteBody(std::vector<iovec> iov) {
    return stream_->WriteFrame(std::move(iov));
}

seastar::future<Status<>> RpcServerContext::WriteHeader(RpcResponseHeader header) {
    header.SetVersion(BLOBSTORE_NET_RPC_HEADER_VERSION);
    header.SetMagic(BLOBSTORE_NET_RPC_HEADER_MAGIC);
    Buffer buf = SerializeRpcHeader(header);
    if (buf.size() == 0) {
        Status<> s;
        s.SetCode(ErrCode::ErrNetworkProtocol).SetReason("net: serialize header error");
        return seastar::make_ready_future<Status<>>(std::move(s));
    }
    return stream_->WriteFrame(std::move(buf));
}

seastar::future<Status<Buffer>> RpcServerContext::SimpleRead(std::chrono::milliseconds timeout) {
    Status<Buffer> s;
    if (recv_len_ == req_header_.ContentLength()) {
        has_fin_ = true;
        co_return s;
    }

    // has pending body
    if (has_pending_body_ && pending_body_.size() > 0) {
        size_t pending_size = pending_body_.size();
        int64_t remaining = req_header_.ContentLength() - recv_len_;

        has_pending_body_ = false;

        if (pending_size > static_cast<size_t>(remaining)) {
            pending_body_ = Buffer();
            s.SetCode(ErrCode::ErrTooLarge).SetReason("net: pending size too large");
            co_return s;
        }

        recv_len_ += pending_size;
        if (recv_len_ == req_header_.ContentLength()) {
            has_fin_ = true;
        }
        s.SetValue(std::move(pending_body_));
        pending_body_ = Buffer();
        co_return s;
    }

    s = co_await stream_->ReadFrame(timeout);
    if (!s) {
        co_return s;
    }
    if (s.Value().size() + recv_len_ > req_header_.ContentLength()) {
        s.SetCode(ErrCode::ErrTooLarge);
        co_return s;
    }
    recv_len_ += s.Value().size();
    co_return s;
}

seastar::future<Status<Buffer>> RpcServerContext::StreamRead(std::chrono::milliseconds timeout) {
    Status<Buffer> s;
    if (recv_len_ == req_header_.ContentLength()) {
        // read next frame
        s = co_await stream_->ReadFrame(timeout);
        if (!s) {
            co_return s;
        }
        recv_len_ = 0;
        req_header_.Clear();
        Buffer b = std::move(s.Value());

        // Parse frame header
        size_t body_offset = 0;
        if (!DeserializeRpcHeader(b, req_header_, body_offset)) {
            s.SetCode(ErrCode::ErrNetworkProtocol)
                .SetReason("net: deserialize stream header error");
            co_return s;
        }

        proto::StreamCmd cmd = req_header_.StreamCmd();
        if (cmd == proto::StreamCmd::FIN) {
            has_fin_ = true;
            LOG_INFO("net: client has closed stream send");
            co_return s;
        }
        if (cmd != proto::StreamCmd::PSH) {
            s.SetCode(ErrCode::ErrNetworkProtocol).SetReason("net: invalid stream cmd");
            co_return s;
        }

        if (req_header_.ContentLength() == 0) {
            co_return s;
        }

        // Check if there's pending body in the same frame
        if (b.size() > body_offset && req_header_.ContentLength() > 0) {
            // Has body in the same frame, save it as pending
            size_t body_size = b.size() - body_offset;
            Buffer body_buf = b.share(body_offset, body_size);
            pending_body_ = std::move(body_buf);
            has_pending_body_ = true;
        }
    }

    if (has_pending_body_ && pending_body_.size() > 0) {
        size_t pending_size = pending_body_.size();
        int64_t remaining = req_header_.ContentLength() - recv_len_;

        has_pending_body_ = false;
        if (pending_size > static_cast<size_t>(remaining)) {
            pending_body_ = Buffer();
            s.SetCode(ErrCode::ErrTooLarge);
            co_return s;
        }

        recv_len_ += pending_size;
        s.SetValue(std::move(pending_body_));
        pending_body_ = Buffer();
        co_return s;
    }

    s = co_await stream_->ReadFrame(timeout);
    if (!s) {
        co_return s;
    }
    if (s.Value().size() + recv_len_ > req_header_.ContentLength()) {
        s.SetCode(ErrCode::ErrTooLarge);
        co_return s;
    }
    recv_len_ += s.Value().size();
    co_return s;
}

seastar::future<Status<Buffer>> RpcServerContext::ReadBody(std::chrono::milliseconds timeout) {
    Status<Buffer> s;
    switch (req_header_.StreamCmd()) {
        case proto::StreamCmd::NOT:
            return SimpleRead(timeout);
        case proto::StreamCmd::SYN:
        case proto::StreamCmd::PSH:
            return StreamRead(timeout);
        case proto::StreamCmd::FIN:
            break;
        default:
            s.SetCode(ErrCode::ErrNetworkProtocol).SetReason("net: unknown command type");
            break;
    }
    return seastar::make_ready_future<Status<Buffer>>(std::move(s));
}

std::unique_ptr<RpcServerStream> RpcServerContext::CreateServerStream() {
    return std::make_unique<RpcServerStream>(this);
}
}  // namespace net
}  // namespace blobstore
