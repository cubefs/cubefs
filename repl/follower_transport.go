package repl

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/log"
)

type FollowerTransport struct {
	addr           string
	conn           net.Conn
	sendCh         chan *FollowerPacket
	recvCh         chan *FollowerPacket
	exitCh         chan struct{}
	exitedMu       sync.RWMutex
	isclosed       int32
	lastActiveTime int64
	replId         int64
	pkgOrder       int64

	// Error state
	errState int32
}

func NewFollowersTransport(addr string, replId int64) (ft *FollowerTransport, err error) {
	var (
		conn net.Conn
	)
	if conn, err = gConnPool.GetConnect(addr); err != nil {
		return
	}
	ft = new(FollowerTransport)
	ft.replId = replId
	ft.addr = addr
	ft.conn = conn
	ft.sendCh = make(chan *FollowerPacket, RequestChanSize)
	ft.recvCh = make(chan *FollowerPacket, RequestChanSize)
	ft.exitCh = make(chan struct{})
	ft.lastActiveTime = time.Now().Unix()
	go ft.serverWriteToFollower()
	go ft.serverReadFromFollower()

	return
}

func (ft *FollowerTransport) PutRequestToRecvCh(request *FollowerPacket) (err error) {
	select {
	case ft.recvCh <- request:
		return
	default:
		return fmt.Errorf("FollowerTransport(%v) RecvCh has full", ft.addr)
	}
}

func (ft *FollowerTransport) PutRequestToSendCh(request *FollowerPacket) (err error) {
	select {
	case ft.sendCh <- request:
		return
	default:
		return fmt.Errorf("FollowerTransport(%v) SendCh has full", ft.addr)
	}
}

func SetConnectPool(cp *connpool.ConnectPool) {
	gConnPool = cp
}

func (ft *FollowerTransport) serverWriteToFollower() {
	for {
		select {
		case p := <-ft.sendCh:
			ft.pkgOrder++
			atomic.StoreInt64(&ft.lastActiveTime, time.Now().Unix())
			if err := p.WriteToConn(ft.conn, proto.WriteDeadlineTime); err != nil {
				p.DecDataPoolRefCnt()
				p.Data = nil
				p.errorCh <- fmt.Errorf("send to follower %v failed: %v", ft.addr, err)
				_ = ft.conn.Close()
				if ft.markFirstError(err) {
					log.LogErrorf("replID(%v) pkgOrder(%v) firstErrorAndPkgInfo(%v,%v) request(%v) ActionSendToFollowers(%v) error(%v)", ft.replId, ft.pkgOrder, err, p, p.GetUniqueLogId(), ft.conn.RemoteAddr().String(), err.Error())
				}
				continue
			}
			p.Data = nil
			p.DecDataPoolRefCnt()
			if err := ft.PutRequestToRecvCh(p); err != nil {
				p.errorCh <- fmt.Errorf("send to follower %v failed: %v", ft.addr, err)
				_ = ft.conn.Close()
				if ft.markFirstError(err) {
					log.LogErrorf("replID(%v) pkgOrder(%v) firstErrorAndPkgInfo(%v,%v) request(%v) ActionSendToFollowers(%v) error(%v)", ft.replId, ft.pkgOrder, err, p, p.GetUniqueLogId(), ft.conn.RemoteAddr().String(), err.Error())
				}
				continue
			}

		case <-ft.exitCh:
			ft.exitedMu.Lock()
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				_ = ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			ft.exitedMu.Unlock()
			return
		}
	}
}

func (ft *FollowerTransport) markFirstError(err error) bool {
	if err == nil {
		return false
	}
	return atomic.CompareAndSwapInt32(&ft.errState, 0, 1)
}

func (ft *FollowerTransport) serverReadFromFollower() {
	for {
		select {
		case p := <-ft.recvCh:
			atomic.StoreInt64(&ft.lastActiveTime, time.Now().Unix())
			_ = ft.readFollowerResult(p)
		case <-ft.exitCh:
			ft.exitedMu.Lock()
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				_ = ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			ft.exitedMu.Unlock()
			return
		}
	}
}

func (ft *FollowerTransport) PutPacketToPool(p *Packet) {
	PutPacketToPool(p)
}
func (ft *FollowerTransport) GetPacketFromPool() (p *Packet) {
	return GetPacketFromPool()
}

// Read the response from the follower
func (ft *FollowerTransport) readFollowerResult(request *FollowerPacket) (err error) {
	reply := ft.GetPacketFromPool()
	defer func() {
		request.Data = nil
		request.errorCh <- err
		if err != nil {
			_ = ft.conn.Close()
			if ft.markFirstError(err) {
				log.LogErrorf("replID(%v) pkgOrder(%v) firstErrorAndPkgInfo(%v,%v) request(%v) readFollowerResult(%v) error(%v)", ft.replId, ft.pkgOrder, err, request, request.GetUniqueLogId(), ft.conn.RemoteAddr().String(), err)
			}
			return
		}
		ft.PutPacketToPool(reply)
	}()
	request.Data = nil
	if err = reply.ReadFromConn(ft.conn, proto.ReadDeadlineTime); err != nil {
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
		err = fmt.Errorf(ActionCheckReply+" request(%v), reply(%v)  ", request.GetUniqueLogId(),
			reply.GetUniqueLogId())
		return
	}

	if reply.IsErrPacket() {
		err = fmt.Errorf(string(reply.Data[:reply.Size]))
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("action[ActionReceiveFromFollower] %v.", reply.LogMessage(ActionReceiveFromFollower,
			ft.addr, request.StartT, err))
	}
	return
}

func (ft *FollowerTransport) cleanSendChan() {
	for {
		select {
		case r := <-ft.sendCh:
			if r == nil {
				return
			}
			r.Data = nil
		default:
			return
		}
	}
}

func (ft *FollowerTransport) cleanRecvChan() {
	for {
		select {
		case r := <-ft.recvCh:
			if r == nil {
				return
			}
			r.Data = nil
		default:
			return
		}
	}
}

func (ft *FollowerTransport) Destory() {
	ft.exitedMu.Lock()
	atomic.StoreInt32(&ft.isclosed, FollowerTransportExiting)
	close(ft.exitCh)
	ft.exitedMu.Unlock()
	for {
		if atomic.LoadInt32(&ft.isclosed) == FollowerTransportExited {
			break
		}
		time.Sleep(time.Millisecond)
	}
	ft.cleanSendChan()
	ft.cleanRecvChan()
	close(ft.sendCh)
	close(ft.recvCh)
}

func (ft *FollowerTransport) needAutoDestory() (release bool) {
	if time.Now().Unix()-atomic.LoadInt64(&ft.lastActiveTime) < FollowerTransportIdleTime {
		return false
	}
	ft.Destory()
	return true
}

func (ft *FollowerTransport) Write(p *FollowerPacket) (err error) {
	return ft.PutRequestToSendCh(p)
}
