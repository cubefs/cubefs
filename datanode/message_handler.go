package datanode

import (
	"container/list"
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"net"
	"sync"
)

var single = struct{}{}

type MessageHandler struct {
	listMux     sync.RWMutex
	sentList    *list.List
	handleCh    chan struct{}
	requestCh   chan *Packet
	replyCh     chan *Packet
	inConn      *net.TCPConn
	isClean     bool
	exitC       chan bool
	exited      bool
	exitedMu    sync.RWMutex
	connectMap  map[string]*net.TCPConn
	connectLock sync.RWMutex
}

func NewMsgHandler(inConn *net.TCPConn) *MessageHandler {
	m := new(MessageHandler)
	m.sentList = list.New()
	m.handleCh = make(chan struct{}, RequestChanSize)
	m.requestCh = make(chan *Packet, RequestChanSize)
	m.replyCh = make(chan *Packet, RequestChanSize)
	m.exitC = make(chan bool, 100)
	m.inConn = inConn
	m.connectMap = make(map[string]*net.TCPConn)

	return m
}

func (msgH *MessageHandler) RenewList(isHeadNode bool) {
	if !isHeadNode {
		msgH.sentList = list.New()
	}
}

func (msgH *MessageHandler) Stop() {
	msgH.exitedMu.Lock()
	defer msgH.exitedMu.Unlock()
	if !msgH.exited {
		if msgH.exitC != nil {
			close(msgH.exitC)
		}
		msgH.exited = true
	}

}

func (msgH *MessageHandler) ExitSign() {
	msgH.exitedMu.Lock()
	defer msgH.exitedMu.Unlock()
	if !msgH.exited {
		if msgH.exitC != nil {
			close(msgH.exitC)
		}
		msgH.exited = true
	}
}

func (msgH *MessageHandler) AllocateNextConn(pkg *Packet) (err error) {
	var conn *net.TCPConn
	if pkg.StoreMode == proto.ExtentStoreMode && pkg.IsWriteOperation() {
		key := fmt.Sprintf("%v_%v", pkg.PartitionID, pkg.FileID)
		msgH.connectLock.RLock()
		conn := msgH.connectMap[key]
		msgH.connectLock.RUnlock()
		if conn == nil {
			conn, err = gConnPool.Get(pkg.NextAddr)
			if err != nil {
				return
			}
			msgH.connectLock.Lock()
			msgH.connectMap[key] = conn
			msgH.connectLock.Unlock()
		}
		pkg.useConnectMap = true
		pkg.NextConn = conn
	} else {
		conn, err = gConnPool.Get(pkg.NextAddr)
		if err != nil {
			return
		}
		pkg.NextConn = conn
	}
	return nil
}

func (msgH *MessageHandler) checkReplyAvail(reply *Packet) (err error) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()

	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID == request.ReqID {
			return
		}
		return fmt.Errorf(ActionCheckReplyAvail+" request [%v] reply[%v] from %v localaddr %v"+
			" remoteaddr %v requestCrc[%v] replyCrc[%v]", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.NextAddr,
			request.NextConn.LocalAddr().String(), request.NextConn.RemoteAddr().String(), request.Crc, reply.Crc)
	}

	return
}

func (msgH *MessageHandler) GetListElement() (e *list.Element) {
	msgH.listMux.RLock()
	e = msgH.sentList.Front()
	msgH.listMux.RUnlock()

	return
}

func (msgH *MessageHandler) PushListElement(e *Packet) {
	msgH.listMux.Lock()
	msgH.sentList.PushBack(e)
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) ClearReqs(s *DataNode) {
	msgH.listMux.Lock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if request.NextAddr != "" {
			if request.useConnectMap {
				request.NextConn.Close()
			} else {
				gConnPool.Put(request.NextConn, true)
			}
			s.headNodePutChunk(request)
		}
	}
	replys := len(msgH.replyCh)
	for i := 0; i < replys; i++ {
		<-msgH.replyCh
	}
	msgH.sentList = list.New()
	for _, conn := range msgH.connectMap {
		conn.Close()
	}
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) ClearReplys() {
	replys := len(msgH.replyCh)
	for i := 0; i < replys; i++ {
		<-msgH.replyCh
	}
}

func (msgH *MessageHandler) DelListElement(reply *Packet, e *list.Element, isForClose bool) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID == request.ReqID && reply.PartitionID == request.PartitionID &&
			reply.FileID == request.FileID && reply.Offset == request.Offset {
			msgH.sentList.Remove(e)
			if request.useConnectMap {
				if request.NextConn != nil && isForClose {
					request.NextConn.Close()
				}
			} else {
				gConnPool.Put(request.NextConn, isForClose)
			}
			pkg := e.Value.(*Packet)
			msgH.replyCh <- pkg
			break
		} else {
			gConnPool.Put(request.NextConn, true)
		}
	}

	return
}
