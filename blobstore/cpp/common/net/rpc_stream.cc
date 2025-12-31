#include "rpc_stream.h"

#include "common/byteorder.h"
#include "common/logger.h"

namespace blobstore {
namespace net {

RpcClientStream::RpcClientStream(std::unique_ptr<RpcClientContext> ctx)
    : ctx_(std::move(ctx)) {}

RpcClientStream::~RpcClientStream() {
    if (ctx_ && !send_closed_) {
        (void)CloseSend();
    }
}

seastar::future<Status<>> RpcClientStream::Send(Buffer data) {
    Status<> s;
    if (send_closed_) {
        s.SetCode(ErrCode::ErrClosed).SetReason(kStreamClosed);
        co_return s;
    }

    size_t data_size = data.size();
    RpcRequestHeader req_header;
    req_header.SetStreamCmd(proto::StreamCmd::PSH);
    req_header.SetContentLength(data_size);

    s = co_await ctx_->WriteHeader(std::move(req_header));
    if (!s) {
        co_return s;
    }

    s = co_await ctx_->WriteBody(std::move(data));
    co_return s;
}

seastar::future<Status<Buffer>> RpcClientStream::Recv() {
    Status<Buffer> s;
    if (recv_closed_) {
        s.SetCode(ErrCode::ErrEOF);
        co_return s;
    }

    auto header_res = co_await ctx_->ReadHeader();
    if (!header_res) {
        s.SetCode(header_res.Code()).SetReason(header_res.Reason());
        co_return s;
    }

    RpcResponseHeader resp_header = std::move(header_res.Value());

    if (resp_header.Status() == static_cast<int32_t>(ErrCode::OK)) {
        recv_closed_ = true;
        s.SetCode(ErrCode::ErrEOF);
        co_return s;
    }
    if (resp_header.Status() > 0) {
        s.SetCode(ErrCode::ErrUnknown).SetReason(resp_header.Reason());
        co_return s;
    }

    if (resp_header.ContentLength() == 0) {
        s.SetValue(Buffer());
        co_return s;
    }

    auto body_res = co_await ctx_->ReadBody();
    if (!body_res) {
        s.SetCode(body_res.Code()).SetReason(body_res.Reason());
        co_return s;
    }

    s.SetValue(std::move(body_res.Value()));
    co_return s;
}

seastar::future<Status<>> RpcClientStream::CloseSend() {
    Status<> s;
    if (send_closed_) {
        co_return s;
    }

    RpcRequestHeader req_header;
    req_header.SetStreamCmd(proto::StreamCmd::FIN);
    req_header.SetContentLength(0);

    s = co_await ctx_->WriteHeader(std::move(req_header));
    if (!s) {
        co_return s;
    }
    send_closed_ = true;
    co_return s;
}

// RpcStreamServer Implementation
RpcServerStream::RpcServerStream(RpcServerContext *ctx) : ctx_(ctx) {
    response_header_.SetStatus(ErrCode::OK);
}

void RpcServerStream::SetHeader(const std::string &key, const std::string &value) {
    if (!header_sent_) {
        response_header_.Header().Set(key, value);
    }
}

// while create stream service should call before any other method
seastar::future<Status<>> RpcServerStream::SendHeader(int32_t status, const std::string &reason) {
    Status<> s;
    if (header_sent_) {
        co_return s;
    }

    header_sent_ = true;
    response_header_.SetStatus(status);
    response_header_.SetReason(reason);

    s = co_await ctx_->WriteHeader(std::move(response_header_));
    co_return s;
}

seastar::future<Status<>> RpcServerStream::Send(Buffer data) {
    size_t data_size = data.size();

    RpcResponseHeader resp_header;
    resp_header.SetContentLength(data_size);
    resp_header.SetStatus(0);  // 0 indicate normal stream message

    Status<> s = co_await ctx_->WriteHeader(std::move(resp_header));
    if (!s) {
        co_return s;
    }

    s = co_await ctx_->WriteBody(data.get(), data_size);
    co_return s;
}

seastar::future<Status<Buffer>> RpcServerStream::Recv() {
    Status<Buffer> s;

    if (ctx_->HasFin()) {
        s.SetCode(ErrCode::ErrEOF);
        co_return s;
    }

    auto body_res = co_await ctx_->ReadBody();
    if (!body_res) {
        s.SetCode(body_res.Code()).SetReason(body_res.Reason());
        co_return s;
    }

    if (ctx_->HasFin()) {
        s.SetCode(ErrCode::ErrEOF);
        co_return s;
    }

    s.SetValue(std::move(body_res.Value()));
    co_return s;
}

}  // namespace net
}  // namespace blobstore
