#include "rpc.h"

#include <isa-l.h>

#include <seastar/core/coroutine.hh>

#include "common/net/byteorder.h"

namespace blobstore {
namespace net {

RpcServerContext::RpcServerContext(Stream* stream, RpcRequestHeader req_header)
    : stream_(stream),
      req_header_(std::move(req_header)),
      trace_(req_header_.Traceid()),
      recv_len_(0) {}

seastar::future<Status<>> RpcServerContext::WriteBody(const char* b, size_t n) {
    return stream_->WriteFrame(b, n);
}

seastar::future<Status<>> RpcServerContext::WriteBody(std::vector<iovec> iov) {
    return stream_->WriteFrame(std::move(iov));
}

seastar::future<Status<>> RpcServerContext::WriteHeader(RpcResponseHeader header) {
    Buffer buf(header.ByteSizeLong());
    header.SerializeToArray(buf.get_write(), buf.size());
    return stream_->WriteFrame(std::move(buf));
}

seastar::future<Status<Buffer>> RpcServerContext::SimpleRead(std::chrono::milliseconds timeout) {
    Status<Buffer> s;
    if (recv_len_ == req_header_.ContentLength()) {
        has_fin_ = true;
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
        if (!req_header_.ParseFrom(b.get(), b.size())) {
            s.SetCode(ErrCode::ErrInvalid);
            co_return s;
        }
        proto::StreamCmd cmd = req_header_.StreamCmd();
        if (cmd == proto::StreamCmd::FIN) {
            has_fin_ = true;
            co_return s;
        } else if (cmd != proto::StreamCmd::PSH) {
            s.SetCode(ErrCode::ErrInvalid);
            co_return s;
        }

        int n = req_header_.ByteSizeLong();
        if (req_header_.ContentLength() == 0) {
            co_return s;
        }
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
    proto::StreamCmd cmd = req_header_.StreamCmd();
    switch (cmd) {
        case proto::StreamCmd::NOT:
            return SimpleRead(timeout);
        case proto::StreamCmd::PSH:
            return StreamRead(timeout);
        case proto::StreamCmd::FIN:
            break;
        default:
            s.SetCode(EINVAL);
            break;
    }
    return seastar::make_ready_future<Status<Buffer>>(std::move(s));
}

}  // namespace net
}  // namespace blobstore
