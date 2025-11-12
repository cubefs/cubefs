#pragma once

#include <google/protobuf/message.h>

#include <functional>
#include <memory>
#include <seastar/core/future.hh>
#include <tuple>

#include "common/net/session.h"
#include "common/proto/rpc.pb.h"
#include "common/status.h"
#include "common/trace.h"
#include "common/util.h"

namespace blobstore {
namespace net {

class Stream;

class RpcMessageHeader {
    proto::Header* header_;

   public:
    explicit RpcMessageHeader(proto::Header* header) noexcept : header_(header) {}

    RpcMessageHeader(const RpcMessageHeader&) = delete;
    RpcMessageHeader& operator=(const RpcMessageHeader&) = delete;

    RpcMessageHeader(RpcMessageHeader&& x) noexcept : header_(std::exchange(x.header_, nullptr)) {}

    RpcMessageHeader& operator=(RpcMessageHeader&& x) noexcept {
        if (&x != this) {
            header_ = std::exchange(x.header_, nullptr);
        }
        return *this;
    }

    void Set(const std::string& key, const std::string& val) {
        if (!Stable()) {
            header_->mutable_m()->insert({key, val});
        }
    }

    std::string Get(const std::string& key) const {
        auto iter = header_->m().find(key);
        if (iter == header_->m().end()) {
            return "";
        }
        return iter->second;
    }

    void Delete(const std::string& key) { header_->mutable_m()->erase(key); }

    void SetStable(bool stable) { header_->set_stable(stable); }

    bool Stable() const { return header_->stable(); }

    void Range(std::function<void(const std::string&, const std::string&)> f) const {
        for (auto& it : header_->m()) {
            f(it.first, it.second);
        }
    }
};

class RpcRequestHeader {
    proto::RequestHeader req_header_;
    Buffer zero_copy_buf_;

   public:
    RpcRequestHeader() noexcept {}
    RpcRequestHeader(const RpcRequestHeader&) = delete;
    RpcRequestHeader& operator=(const RpcRequestHeader&) = delete;

    RpcRequestHeader(RpcRequestHeader&& x) noexcept {
        req_header_ = std::move(x.req_header_);
        zero_copy_buf_ = std::move(x.zero_copy_buf_);
    }
    RpcRequestHeader& operator=(RpcRequestHeader&& x) noexcept {
        if (this != &x) {
            req_header_ = std::move(x.req_header_);
            zero_copy_buf_ = std::move(x.zero_copy_buf_);
        }
        return *this;
    }

    bool ParseFrom(const char* b, size_t n) { return req_header_.ParseFromArray(b, n); }

    bool ParseFromZeroCopy(Buffer b) {
        zero_copy_buf_ = std::move(b);
        ::google::protobuf::io::ArrayInputStream in(zero_copy_buf_.get(), zero_copy_buf_.size());
        return req_header_.ParseFromZeroCopyStream(&in);
    }

    bool SerializeToArray(char* b, size_t n) { return req_header_.SerializeToArray(b, n); }

    int32_t Version() const { return req_header_.version(); }

    void SetVersion(int32_t v) { req_header_.set_version(v); }

    int32_t Magic() const { return req_header_.magic(); }

    void SetMagic(int32_t magic) { req_header_.set_magic(magic); }

    proto::StreamCmd StreamCmd() const { return req_header_.stream_cmd(); }

    void SetStreamCmd(proto::StreamCmd cmd) { req_header_.set_stream_cmd(cmd); }

    const std::string& RemotePath() const { return req_header_.remote_path(); }

    void SetRemotePath(const std::string& path) { req_header_.set_remote_path(path); }

    const std::string& Traceid() const { return req_header_.trace_id(); }

    void SetTraceid(const std::string& trace_id) { req_header_.set_trace_id(trace_id); }

    int64_t ContentLength() const { return req_header_.content_length(); }

    void SetContentLength(int64_t n) { req_header_.set_content_length(n); }

    int32_t RemotePathIndex() const { return req_header_.remote_path_index(); }
    void SetRemotePathIndex(int32_t index) { req_header_.set_remote_path_index(index); }

    RpcMessageHeader Header() {
        RpcMessageHeader header(req_header_.mutable_header());
        return header;
    }

    const std::string& Parameter() const { return req_header_.parameter(); }

    void SetParameter(const std::string& v) {
        std::string* p = req_header_.mutable_parameter();
        *p = v;
    }

    int64_t ByteSizeLong() const { return req_header_.ByteSizeLong(); }

    void Clear() { req_header_.Clear(); }
};

class RpcResponseHeader {
    proto::ResponseHeader resp_header_;
    Buffer zero_copy_buf_;

   public:
    RpcResponseHeader() noexcept { resp_header_.set_status((int)ErrCode::OK); }
    RpcResponseHeader(const RpcResponseHeader&) = delete;
    RpcResponseHeader& operator=(const RpcResponseHeader&) = delete;

    RpcResponseHeader(RpcResponseHeader&& x) noexcept {
        resp_header_ = std::move(x.resp_header_);
        zero_copy_buf_ = std::move(x.zero_copy_buf_);
    }
    RpcResponseHeader& operator=(RpcResponseHeader&& x) noexcept {
        if (this != &x) {
            resp_header_ = std::move(x.resp_header_);
            zero_copy_buf_ = std::move(x.zero_copy_buf_);
        }
        return *this;
    }

    int32_t Version() const { return resp_header_.version(); }

    void SetVersion(int32_t v) { resp_header_.set_version(v); }

    int32_t Magic() const { return resp_header_.magic(); }

    void SetMagic(int32_t magic) { resp_header_.set_magic(magic); }

    int32_t Code() const { return resp_header_.status(); }

    void SetCode(int32_t code) { resp_header_.set_status(code); }
    void SetCode(ErrCode code) { resp_header_.set_status((int)code); }

    void SetReason(std::string_view reason) {
        resp_header_.set_reason(std::string(reason.data(), reason.size()));
    }

    void SetReason(const std::string& reason) { resp_header_.set_reason(reason); }

    const std::string& Reason() const { return resp_header_.reason(); };

    int64_t ContentLength() const { return resp_header_.content_length(); }

    void SetContentLength(int64_t n) { resp_header_.set_content_length(n); }

    RpcMessageHeader Header() {
        RpcMessageHeader header(resp_header_.mutable_header());
        return header;
    }

    int64_t ByteSizeLong() const { return resp_header_.ByteSizeLong(); }

    bool ParseFrom(const char* b, size_t n) { return resp_header_.ParseFromArray(b, n); }

    bool ParseFromZeroCopy(Buffer b) {
        zero_copy_buf_ = std::move(b);
        ::google::protobuf::io::ArrayInputStream in(zero_copy_buf_.get(), zero_copy_buf_.size());
        return resp_header_.ParseFromZeroCopyStream(&in);
    }

    bool SerializeToArray(char* b, size_t n) { return resp_header_.SerializeToArray(b, n); }
    void Clear() { resp_header_.Clear(); }
};

class RpcServerContext {
    Stream* stream_;
    RpcRequestHeader req_header_;
    blobstore::Trace trace_;

    size_t recv_len_ = 0;
    bool stream_ctx_ = false;
    bool has_fin_ = false;

    friend class TcpRpcServer;

    seastar::future<Status<Buffer>> SimpleRead(std::chrono::milliseconds timeout);

    seastar::future<Status<Buffer>> StreamRead(std::chrono::milliseconds timeout);

   public:
    explicit RpcServerContext(Stream* stream, RpcRequestHeader req_header);

    RpcServerContext(const RpcServerContext&) = delete;
    RpcServerContext& operator=(const RpcServerContext&) = delete;

    inline RpcRequestHeader& GetRpcRequestHeader() { return req_header_; }

    inline bool StreamContext() const { return stream_ctx_; }

    inline bool HasFin() const { return has_fin_; }

    inline blobstore::Trace& Trace() { return trace_; }

    // write a body frame
    seastar::future<Status<>> WriteBody(const char* b, size_t n);
    seastar::future<Status<>> WriteBody(std::vector<iovec> iov);

    // send response head
    seastar::future<Status<>> WriteHeader(RpcResponseHeader header);

    seastar::future<Status<Buffer>> ReadBody(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    seastar::socket_address RemoteAddress() const { return stream_->RemoteAddress(); }

    seastar::future<> Close();
};

}  // namespace net
}  // namespace blobstore
