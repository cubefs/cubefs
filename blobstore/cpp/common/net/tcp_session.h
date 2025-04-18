#pragma once

#include <array>
#include <atomic>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <unordered_map>

#include "common/net/frame.h"
#include "common/net/session.h"
#include "common/net/tcp_connection.h"
#include "common/net/tcp_stream.h"
#include "common/status.h"

namespace blobstore {
namespace net {

class TcpSession;
using TcpSessionPtr = seastar::shared_ptr<TcpSession>;

class TcpSession : public Session, public seastar::weakly_referencable<TcpSession> {
    Option opt_;
    uint64_t sess_id_;
    TcpConnectionPtr conn_;
    bool client_;
    BufferAllocator *allocator_;
    uint32_t next_id_;

    std::unordered_map<uint32_t, TcpStreamPtr> streams_;
    seastar::gate gate_;

    Status<> status_;

    std::queue<StreamPtr> accept_q_;
    seastar::semaphore accept_sem_;
    ssize_t tokens_;

    seastar::condition_variable accept_cv_;
    seastar::condition_variable token_cv_;

    enum ClassID {
        CTRL = 0,
        DATA = 1,
    };

    struct write_request {
        Frame frame;
        std::optional<seastar::promise<Status<>>> pr;
    };

    std::array<std::queue<write_request *>, 2> write_q_;
    seastar::condition_variable w_cv_;
    seastar::timer<seastar::lowres_clock> keepalive_timer_;

    static std::atomic<uint64_t> session_id_;

   private:
    seastar::future<> RecvLoop();
    seastar::future<> SendLoop();

    void SetStatus(const Status<> &s) {
        if (status_) {
            status_ = s;
        }
    }

    void SetStatus(ErrCode code) {
        if (status_) {
            status_.SetCode(code);
        }
    }

    void SetStatus(ErrCode code, const seastar::sstring &reason) {
        if (status_) {
            status_.SetCode(code).SetReason(reason);
        }
    }
    void StartKeepalive();

    void ReturnTokens(uint32_t n);

    seastar::future<Status<>> WriteFrameInternal(Frame f, ClassID classid);

    void WritePingPong(bool ping);

    seastar::future<> CloseAllStreams();

    void ClearWriteq();

    seastar::future<> HandleSyn(Frame f) noexcept;

    void HandleFin(const Frame &f) noexcept;

    seastar::future<> HandlePsh(Frame f, uint32_t len,
                                seastar::timer<seastar::lowres_clock> *read_timer) noexcept;

    seastar::future<> HandleUpd(Frame f, uint32_t len,
                                seastar::timer<seastar::lowres_clock> *read_timer) noexcept;

    friend class TcpStream;

   public:
    explicit TcpSession(const Option &opt, TcpConnectionPtr conn, bool client,
                        BufferAllocator *allocator = nullptr);

    static SessionPtr MakeSession(Option opt, TcpConnectionPtr conn, bool client,
                                  BufferAllocator *allocator = nullptr);

    virtual ~TcpSession();

    inline uint64_t ID() const { return sess_id_; }

    bool Valid() const { return (!gate_.is_closed() && status_); }

    seastar::future<Status<StreamPtr>> OpenStream();

    seastar::future<Status<StreamPtr>> AcceptStream();

    inline size_t Streams() const { return streams_.size(); }

    inline seastar::socket_address LocalAddress() const { return conn_->LocalAddress(); }

    inline seastar::socket_address RemoteAddress() const { return conn_->RemoteAddress(); }

    seastar::future<> Close();
};

}  // namespace net
}  // namespace blobstore
