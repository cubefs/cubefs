#pragma once

#include <google/protobuf/message.h>

#include <seastar/core/future.hh>

#include "common/net/rpc.h"
#include "common/net/rpc_client.h"

namespace blobstore {
namespace net {

static const seastar::sstring kStreamClosed = "net: rpc stream already closed";
static const seastar::sstring kStreamMessageSerializeFailed =
    "net: serialize stream message failed";

class RpcClientStream {
   public:
    explicit RpcClientStream(std::unique_ptr<RpcClientContext> ctx);
    ~RpcClientStream();

    RpcClientStream(const RpcClientStream&) = delete;
    RpcClientStream& operator=(const RpcClientStream&) = delete;

    // Send a protobuf message
    template <ProtobufMessageSerdes T>
    seastar::future<Status<>> Send(const T* msg);

    // Send raw data
    seastar::future<Status<>> Send(Buffer data);

    // Receive message
    seastar::future<Status<Buffer>> Recv();

    template <ProtobufMessageSerdes T>
    seastar::future<Status<>> Recv(T* msg);

    // Close send direction (send FIN)
    seastar::future<Status<>> CloseSend();

    // Get response header
    const RpcResponseHeader& GetHeader() const { return response_header_; }

   private:
    std::unique_ptr<RpcClientContext> ctx_;
    RpcResponseHeader response_header_;
    bool send_closed_ = false;
    bool recv_closed_ = false;
};

template <ProtobufMessageSerdes T>
seastar::future<Status<>> RpcClientStream::Send(const T* msg) {
    Status<> s;
    size_t msg_size = msg->ByteSizeLong();
    Buffer data(msg_size);
    if (!msg->SerializeToArray(data.get_write(), msg_size)) {
        s.SetCode(ErrCode::ErrUnknown).SetReason(kStreamMessageSerializeFailed);
        co_return s;
    }
    s = co_await Send(std::move(data));
    co_return s;
}

template <ProtobufMessageSerdes T>
seastar::future<Status<>> RpcClientStream::Recv(T* msg) {
    Status<> s;

    auto res = co_await Recv();
    if (!res) {
        s.SetCode(s.Code()).SetReason(res.Reason());
        co_return s;
    }

    const Buffer data = std::move(res.Value());
    msg->ParseFromArray(data.get(), data.size());

    co_return s;
}

class RpcServerStream {
   public:
    explicit RpcServerStream(RpcServerContext* ctx);

    RpcServerStream(const RpcServerStream&) = delete;
    RpcServerStream& operator=(const RpcServerStream&) = delete;

    // Set response header
    void SetHeader(const std::string& key, const std::string& value);

    // Send response header (must be called once before Send)
    seastar::future<Status<>> SendHeader(int32_t status = 200, const std::string& reason = "OK");

    template <ProtobufMessageSerdes T>
    seastar::future<Status<>> Send(const T* msg);

    // Send raw data
    seastar::future<Status<>> Send(Buffer data);

    // Receive message
    seastar::future<Status<Buffer>> Recv();

    template <ProtobufMessageSerdes T>
    seastar::future<Status<>> Recv(T* msg);

   private:
    RpcServerContext* ctx_;
    RpcResponseHeader response_header_;
    bool header_sent_ = false;
};

template <ProtobufMessageSerdes T>
seastar::future<Status<>> RpcServerStream::Send(const T* msg) {
    size_t msg_size = msg->ByteSizeLong();
    Buffer data(msg_size);
    if (!msg->SerializeToArray(data.get_write(), msg_size)) {
        Status<> s;
        s.SetCode(ErrCode::ErrUnknown).SetReason(kStreamMessageSerializeFailed);
        co_return s;
    }
    co_return co_await Send(std::move(data));
}

template <ProtobufMessageSerdes T>
seastar::future<Status<>> RpcServerStream::Recv(T* msg) {
    Status<> s;
    auto res = co_await Recv();
    if (!res) {
        s.SetCode(res.Code());
        s.SetReason(res.Reason());
        co_return s;
    }
    Buffer data = std::move(res.Value());
    msg->ParseFromArray(data.get(), data.size());
    co_return s;
}

}  // namespace net
}  // namespace blobstore
