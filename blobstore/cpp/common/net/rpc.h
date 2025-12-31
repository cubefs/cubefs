#pragma once

#include <google/protobuf/message.h>

#include <concepts>
#include <functional>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <tuple>
#include <type_traits>
#include <utility>

#include "common/byteorder.h"
#include "common/concepts.h"
#include "common/net/session.h"
#include "common/proto/rpc.pb.h"
#include "common/status.h"
#include "common/trace.h"
#include "common/util.h"

namespace blobstore {
namespace net {

#define BLOBSTORE_NET_RPC_HEADER_VERSION 0
#define BLOBSTORE_NET_RPC_HEADER_MAGIC 0xee

#define BLOBSTORE_NET_RPC_HEADER_SIZE 4  // request, response header size

// C++20 Concepts: Request and Response header type
template <typename T>
concept RpcHeaderSerializable = requires(const T& header, char* ptr, size_t size) {
    { header.ByteSizeLong() } -> std::convertible_to<size_t>;
    { header.SerializeToArray(ptr, size) } -> std::convertible_to<bool>;
};

template <typename T>
concept RpcHeaderDeserializable = requires(T& header, const char* ptr, size_t size) {
    { header.ParseFrom(ptr, size) } -> std::convertible_to<bool>;
};

template <typename T>
concept RpcHeader = RpcHeaderSerializable<T> && RpcHeaderDeserializable<T>;

// Failed if Buffer(size == 0)
template <RpcHeaderSerializable HeaderType>
inline Buffer SerializeRpcHeader(const HeaderType& header) {
    size_t header_size = header.ByteSizeLong();
    Buffer buf(BLOBSTORE_NET_RPC_HEADER_SIZE + header_size);
    auto write_ptr = buf.get_write();
    LittleEndian::PutUint32(write_ptr, static_cast<uint32_t>(header_size));
    if (!header.SerializeToArray(write_ptr + BLOBSTORE_NET_RPC_HEADER_SIZE, header_size)) {
        return Buffer();
    }
    return buf;
}

// (header_size, body_offset)，failed is (0, 0)
inline std::pair<uint32_t, size_t> DeserializeRpcHeaderSize(const Buffer& buf) {
    if (buf.size() < BLOBSTORE_NET_RPC_HEADER_SIZE) {
        return {0, 0};
    }
    uint32_t header_size = LittleEndian::Uint32(buf.get());
    if (buf.size() < BLOBSTORE_NET_RPC_HEADER_SIZE + header_size) {
        return {0, 0};
    }
    size_t body_offset = BLOBSTORE_NET_RPC_HEADER_SIZE + header_size;
    return {header_size, body_offset};
}

// Parse header and body_offset
template <RpcHeaderDeserializable HeaderType>
inline bool DeserializeRpcHeader(const Buffer& buf, HeaderType& header, size_t& body_offset) {
    auto [header_size, offset] = DeserializeRpcHeaderSize(buf);
    if (header_size == 0) {
        return false;
    }
    if (!header.ParseFrom(buf.get() + BLOBSTORE_NET_RPC_HEADER_SIZE, header_size)) {
        return false;
    }
    body_offset = offset;
    return true;
}

class Stream;
class RpcServerStream;

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

    bool SerializeToArray(char* b, size_t n) const { return req_header_.SerializeToArray(b, n); }

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

    int32_t Status() const { return resp_header_.status(); }

    void SetStatus(int32_t status) { resp_header_.set_status(status); }
    void SetStatus(ErrCode status) { resp_header_.set_status(static_cast<int32_t>(status)); }

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

    bool SerializeToArray(char* b, size_t n) const { return resp_header_.SerializeToArray(b, n); }
    void Clear() { resp_header_.Clear(); }
};

class RpcServerContext {
    Stream* stream_;
    RpcRequestHeader req_header_;
    blobstore::Trace trace_;

    size_t recv_len_ = 0;
    bool stream_ctx_ = false;
    bool has_fin_ = false;

    Buffer pending_body_;  // has body in first frame
    bool has_pending_body_ = false;

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

    void SetPendingBody(Buffer body) {
        pending_body_ = std::move(body);
        has_pending_body_ = true;
    }

    // write a body frame
    seastar::future<Status<>> WriteBody(const char* b, size_t n);
    seastar::future<Status<>> WriteBody(std::vector<iovec> iov);

    // send response head
    seastar::future<Status<>> WriteHeader(RpcResponseHeader header);

    seastar::future<Status<Buffer>> ReadBody(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    seastar::socket_address RemoteAddress() const { return stream_->RemoteAddress(); }

    seastar::future<> Close();

    std::unique_ptr<RpcServerStream> CreateServerStream();

    // Parse Parameter
    template <::blobstore::ProtobufMessageDeserializable T>
    seastar::future<Status<>> ParseParameter(T* args);
};

template <::blobstore::ProtobufMessageDeserializable T>
seastar::future<Status<>> RpcServerContext::ParseParameter(T* args) {
    Status<> result;

    const std::string& param_data = req_header_.Parameter();
    if (param_data.size() != 0) {
        if (!args->ParseFromArray(param_data.data(), param_data.size())) {
            result.SetCode(ErrCode::ErrInvalid).SetReason("net: parameter parse failed");
        }
        co_return result;
    }

    // parameter data is empty
    if (req_header_.ContentLength() == 0) {
        co_return result;
    }

    //  read parameter data from body
    auto stream_read_status = co_await StreamRead(std::chrono::milliseconds::zero());
    if (!stream_read_status) {
        result.SetCode(stream_read_status.Code()).SetReason(stream_read_status.Reason());
        co_return result;
    }

    Buffer b = std::move(stream_read_status.Value());
    if (b.size() < req_header_.ContentLength()) {
        result.SetCode(ErrCode::ErrInvalid).SetReason("net: read parameter data length not match");
        co_return result;
    }

    if (!args->ParseFromArray(b.get(), b.size())) {
        result.SetCode(ErrCode::ErrInvalid).SetReason("net: parameter parse failed");
        co_return result;
    }

    co_return result;
}

}  // namespace net
}  // namespace blobstore
