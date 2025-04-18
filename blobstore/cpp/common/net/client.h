#pragma once

#include <list>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_mutex.hh>

#include "common/net/session.h"

namespace blobstore {
namespace net {

class Client;

using ClientPtr = seastar::lw_shared_ptr<Client>;

class ClientStream {
    StreamPtr stream_;
    ClientPtr client_;
    bool valid_ = true;
    friend class Client;

   public:
    ClientStream() = default;
    ClientStream(StreamPtr s, ClientPtr c);

    ClientStream(const ClientStream& x) = delete;
    ClientStream& operator=(const ClientStream& x) = delete;

    ClientStream(ClientStream&& x);
    ClientStream& operator=(ClientStream&& x);

    ~ClientStream();

    seastar::future<Status<Buffer>> ReadFrame(std::chrono::milliseconds timeout);

    seastar::future<Status<>> WriteFrame(const char* data, size_t size);
    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov);

    void SetValid(bool valid) { valid_ = valid; }
};

using ClientStreamPtr = std::unique_ptr<ClientStream>;

class Client : public seastar::enable_lw_shared_from_this<Client> {
    seastar::socket_address sa_;
    Option opt_;
    seastar::lowres_clock::time_point utime_;
    std::chrono::milliseconds connect_timeout_;  // ms
    net::BufferAllocator* allocator_;
    seastar::shared_mutex mu_;
    struct SessionImpl {
        SessionPtr sess_;
        std::queue<StreamPtr> streams_;
    };
    using SessionImplPtr = seastar::lw_shared_ptr<SessionImpl>;

    SessionImplPtr sess_impl_;
    SessionImplPtr old_sess_impl_;
    seastar::gate gate_;

    friend class ClientStream;

    void CleanupOldSession();

    seastar::future<Status<ClientStreamPtr>> TryGetExistingStream();

    seastar::future<Status<SessionImplPtr>> CreateNewConnection();

    seastar::future<> Release(net::StreamPtr s, bool close = false);

   public:
    explicit Client(const seastar::socket_address& sa, const Option opt = Option(),
                    std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(100),
                    net::BufferAllocator* allocator = nullptr);

    virtual ~Client() {}

    seastar::future<Status<ClientStreamPtr>> GetClientStream();

    seastar::lowres_clock::time_point GetUtime() const;

    void UpdateTime();

    const seastar::socket_address& RemoteAddress() const { return sa_; }

    seastar::future<> Close();
};

}  // namespace net
}  // namespace blobstore
