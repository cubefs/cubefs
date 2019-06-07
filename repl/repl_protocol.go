// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package repl

import (
	"container/list"
	"fmt"
	"net"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"sync/atomic"
	"time"
)

var (
	gConnPool = util.NewConnectPool()
)

// ReplProtocol defines the struct of the replication protocol.
// 1. ServerConn reads a packet from the client socket, and analyzes the addresses of the followers.
// 2. After the preparation, the packet is send to toBeProcessedCh. If failure happens, send it to the response channel.
// 3. OperatorAndForwardPktGoRoutine fetches a packet from toBeProcessedCh, and determine if it needs to be forwarded to the followers.
// 4. receiveResponse fetches a reply from responseCh, executes postFunc, and writes a response to the client if necessary.
type ReplProtocol struct {
	packetListLock sync.RWMutex

	packetList *list.List    // stores all the received packets from the client
	ackCh      chan struct{} // if sending to all the replicas succeeds, then a signal to this channel

	toBeProcessedCh chan *Packet // the goroutine receives an available packet and then sends it to this channel
	responseCh      chan *Packet // this chan is used to write response to the client

	sourceConn *net.TCPConn
	exitC      chan bool
	exited     int32

	followerConnects map[string]*FollowerTransport
	lock             sync.RWMutex

	prepareFunc  func(p *Packet) error                 // prepare packet
	operatorFunc func(p *Packet, c *net.TCPConn) error // operator
	postFunc     func(p *Packet) error                 // post-processing packet

	isError int32
	replId  int64
}

type FollowerTransport struct {
	addr     string
	conn     net.Conn
	sendCh   chan *FollowerPacket
	recvCh   chan *FollowerPacket
	exitCh   chan struct{}
	isclosed int32
}

func NewFollowersTransport(addr string) (ft *FollowerTransport, err error) {
	var (
		conn net.Conn
	)
	if conn, err = gConnPool.GetConnect(addr); err != nil {
		return
	}
	ft = new(FollowerTransport)
	ft.addr = addr
	ft.conn = conn
	ft.sendCh = make(chan *FollowerPacket, 200)
	ft.recvCh = make(chan *FollowerPacket, 200)
	ft.exitCh = make(chan struct{})
	go ft.serverWriteToFollower()
	go ft.serverReadFromFollower()

	return
}

func (ft *FollowerTransport) serverWriteToFollower() {
	for {
		select {
		case p := <-ft.sendCh:
			defer func() {
				if r := recover(); r != nil {
					fmt.Println(fmt.Sprintf("follower packet is %v lendata is %v data is %v", p.GetUniqueLogId(), len(p.Data), string(p.Data)))
					panic(r)
				}
			}()
			if err := p.WriteToConn(ft.conn); err != nil {
				p.PackErrorBody(ActionSendToFollowers, err.Error())
				continue
			}
			ft.recvCh <- p
		case <-ft.exitCh:
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			return
		}
	}
}

func (ft *FollowerTransport) serverReadFromFollower() {
	for {
		select {
		case p := <-ft.recvCh:
			ft.readFollowerResult(p)
		case <-ft.exitCh:
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			return
		}
	}
}

// Read the response from the follower
func (ft *FollowerTransport) readFollowerResult(request *FollowerPacket) (err error) {
	reply := NewPacket()
	defer func() {
		reply.clean()
		request.respCh <- err
	}()
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

	log.LogDebugf("action[ActionReceiveFromFollower] %v.", reply.LogMessage(ActionReceiveFromFollower,
		ft.addr, request.StartT, err))
	return
}

func (ft *FollowerTransport) Destory() {
	atomic.StoreInt32(&ft.isclosed, FollowerTransportExiting)
	close(ft.exitCh)
	for {
		if atomic.LoadInt32(&ft.isclosed) == FollowerTransportExited {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(ft.sendCh)
	close(ft.recvCh)
}

func (ft *FollowerTransport) Write(p *FollowerPacket) {
	ft.sendCh <- p
}

func NewReplProtocol(inConn *net.TCPConn, prepareFunc func(p *Packet) error,
	operatorFunc func(p *Packet, c *net.TCPConn) error, postFunc func(p *Packet) error) *ReplProtocol {
	rp := new(ReplProtocol)
	rp.packetList = list.New()
	rp.ackCh = make(chan struct{}, RequestChanSize)
	rp.toBeProcessedCh = make(chan *Packet, RequestChanSize)
	rp.responseCh = make(chan *Packet, RequestChanSize)
	rp.exitC = make(chan bool, 1)
	rp.sourceConn = inConn
	rp.followerConnects = make(map[string]*FollowerTransport)
	rp.prepareFunc = prepareFunc
	rp.operatorFunc = operatorFunc
	rp.postFunc = postFunc
	rp.exited = ReplRuning
	rp.replId = proto.GenerateRequestID()
	go rp.OperatorAndForwardPktGoRoutine()
	go rp.ReceiveResponseFromFollowersGoRoutine()
	go rp.writeResponseToClientGoRroutine()

	return rp
}

// ServerConn keeps reading data from the socket to analyze the follower address, execute the prepare function,
// and throw the packets to the to-be-processed channel.
func (rp *ReplProtocol) ServerConn() {
	var (
		err error
	)
	defer func() {
		rp.Stop()
		if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
			rp.sourceConn.Close()
			rp.cleanResource()
		}
	}()
	for {
		select {
		case <-rp.exitC:
			return
		default:
			if err = rp.readPkgAndPrepare(); err != nil {
				return
			}
		}
	}

}

// Receive response from all followers.
func (rp *ReplProtocol) ReceiveResponseFromFollowersGoRoutine() {
	for {
		select {
		case <-rp.ackCh:
			rp.checkLocalResultAndReciveAllFollowerResponse()
		case <-rp.exitC:
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			return
		}
	}
}

func (rp *ReplProtocol) setReplProtocolError(request *Packet, index int) {
	atomic.StoreInt32(&rp.isError, ReplProtocolError)
}

func (rp *ReplProtocol) hasError() bool {
	return atomic.LoadInt32(&rp.isError) == ReplProtocolError
}

func (rp *ReplProtocol) readPkgAndPrepare() (err error) {
	request := NewPacket()
	if err = request.ReadFromConnFromCli(rp.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readPkgAndPrepare] packet(%v) from remote(%v) localAddr(%v).",
		request.GetUniqueLogId(), rp.sourceConn.RemoteAddr().String(), rp.sourceConn.LocalAddr().String())
	if err = request.resolveFollowersAddr(); err != nil {
		err = rp.putResponse(request)
		return
	}
	if err = rp.prepareFunc(request); err != nil {
		err = rp.putResponse(request)
		return
	}
	err = rp.putToBeProcess(request)

	return
}

func (rp *ReplProtocol) sendRequestToAllFollowers(request *Packet) (index int, err error) {
	for index = 0; index < len(request.followersAddrs); index++ {
		var transport *FollowerTransport
		if transport, err = rp.allocateFollowersConns(request, index); err != nil {
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
		followerRequest := NewFollowerPacket()
		copyPacket(request, followerRequest)
		followerRequest.RemainingFollowers = 0
		request.followerPackets[index] = followerRequest
		transport.Write(followerRequest)
	}

	return
}

// OperatorAndForwardPktGoRoutine reads packets from the to-be-processed channel and writes responses to the client.
// 1. Read a packet from toBeProcessCh, and determine if it needs to be forwarded or not. If the answer is no, then
// 	  process the packet locally and put it into responseCh.
// 2. If the packet needs to be forwarded, the first send it to the followers, and execute the operator function.
//    Then notify receiveResponse to read the followers' responses.
// 3. Read a reply from responseCh, and write to the client.
func (rp *ReplProtocol) OperatorAndForwardPktGoRoutine() {
	for {
		select {
		case request := <-rp.toBeProcessedCh:
			if !request.isForwardPacket() {
				rp.operatorFunc(request, rp.sourceConn)
				rp.putResponse(request)
			} else {
				index, err := rp.sendRequestToAllFollowers(request)
				if err != nil {
					rp.setReplProtocolError(request, index)
					rp.putResponse(request)
				} else {
					rp.pushPacketToList(request)
					rp.operatorFunc(request, rp.sourceConn)
					rp.putAck()
				}
			}
		case <-rp.exitC:
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			return
		}
	}

}

func (rp *ReplProtocol) writeResponseToClientGoRroutine() {
	for {
		select {
		case request := <-rp.responseCh:
			rp.writeResponse(request)
		case <-rp.exitC:
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			return
		}
	}

}

func (rp *ReplProtocol) operatorFuncWithWaitGroup(wg *sync.WaitGroup, request *Packet) {
	defer wg.Done()
	rp.operatorFunc(request, rp.sourceConn)
}

// Read a packet from the list, scan all the connections of the followers of this packet and read the responses.
// If failed to read the response, then mark the packet as failure, and delete it from the list.
// If all the reads succeed, then mark the packet as success.
func (rp *ReplProtocol) checkLocalResultAndReciveAllFollowerResponse() {
	var (
		e *list.Element
	)

	if e = rp.getNextPacket(); e == nil {
		return
	}
	request := e.Value.(*Packet)
	defer func() {
		rp.deletePacket(request, e)
	}()
	if request.IsErrPacket() {
		return
	}
	for index := 0; index < len(request.followersAddrs); index++ {
		followerPacket := request.followerPackets[index]
		err := <-followerPacket.respCh
		if err != nil {
			request.PackErrorBody(ActionReceiveFromFollower, err.Error())
			return
		}

	}
	return
}

// Write a reply to the client.
func (rp *ReplProtocol) writeResponse(reply *Packet) {
	var err error
	defer func() {
		reply.clean()
	}()
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogErrorf(err.Error())
		rp.Stop()
	}

	// execute the post-processing function
	rp.postFunc(reply)
	if !reply.NeedReply {
		return
	}

	if err = reply.WriteToConn(rp.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, fmt.Sprintf("local(%v)->remote(%v)", rp.sourceConn.LocalAddr().String(),
			rp.sourceConn.RemoteAddr().String()), reply.StartT, err))
		log.LogErrorf(err.Error())
		rp.Stop()
	}
	log.LogDebugf(reply.LogMessage(ActionWriteToClient,
		rp.sourceConn.RemoteAddr().String(), reply.StartT, err))
}

// Stop stops the replication protocol.
func (rp *ReplProtocol) Stop() {
	if atomic.LoadInt32(&rp.exited) == ReplRuning {
		if rp.exitC != nil {
			close(rp.exitC)
		}
		atomic.StoreInt32(&rp.exited, ReplExiting)
	}

}

// Allocate the connections to the followers. We use partitionId + extentId + followerAddr as the key.
// Note that we need to ensure the order of packets sent to the datanode is consistent here.
func (rp *ReplProtocol) allocateFollowersConns(p *Packet, index int) (transport *FollowerTransport, err error) {
	rp.lock.RLock()
	transport = rp.followerConnects[p.followersAddrs[index]]
	rp.lock.RUnlock()
	if transport == nil {
		transport, err = NewFollowersTransport(p.followersAddrs[index])
		if err != nil {
			return
		}
		rp.lock.Lock()
		rp.followerConnects[p.followersAddrs[index]] = transport
		rp.lock.Unlock()
	}

	return
}

func (rp *ReplProtocol) getNextPacket() (e *list.Element) {
	rp.packetListLock.RLock()
	e = rp.packetList.Front()
	rp.packetListLock.RUnlock()

	return
}

func (rp *ReplProtocol) pushPacketToList(e *Packet) {
	rp.packetListLock.Lock()
	rp.packetList.PushBack(e)
	rp.packetListLock.Unlock()
}

func (rp *ReplProtocol) cleanToBeProcessCh() {
	request := len(rp.toBeProcessedCh)
	for i := 0; i < request; i++ {
		select {
		case p := <-rp.toBeProcessedCh:
			rp.postFunc(p)
			p.clean()
		default:
			return
		}
	}
}

func (rp *ReplProtocol) cleanResponseCh() {
	replys := len(rp.responseCh)
	for i := 0; i < replys; i++ {
		select {
		case p := <-rp.responseCh:
			rp.postFunc(p)
			p.clean()
		default:
			return
		}
	}
}

// If the replication protocol exits, then clear all the packet resources.
func (rp *ReplProtocol) cleanResource() {
	rp.packetListLock.Lock()
	for e := rp.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		rp.postFunc(request)
		request.clean()
	}
	rp.cleanToBeProcessCh()
	rp.cleanResponseCh()
	rp.packetList = list.New()
	rp.lock.RLock()
	for _, transport := range rp.followerConnects {
		transport.Destory()
	}
	rp.lock.RUnlock()
	close(rp.responseCh)
	close(rp.toBeProcessedCh)
	close(rp.ackCh)
	rp.packetList = nil
	rp.followerConnects = nil
	rp.packetListLock.Unlock()
}

func (rp *ReplProtocol) deletePacket(reply *Packet, e *list.Element) (success bool) {
	rp.packetListLock.Lock()
	defer rp.packetListLock.Unlock()
	rp.packetList.Remove(e)
	success = true
	rp.putResponse(reply)
	return
}

func (rp *ReplProtocol) putResponse(reply *Packet) (err error) {
	select {
	case rp.responseCh <- reply:
		return
	default:
		return fmt.Errorf("response Chan has full (%v)", len(rp.responseCh))
	}
}

func (rp *ReplProtocol) putToBeProcess(request *Packet) (err error) {
	select {
	case rp.toBeProcessedCh <- request:
		return
	default:
		return fmt.Errorf("toBeProcessedCh Chan has full (%v)", len(rp.toBeProcessedCh))
	}
}

func (rp *ReplProtocol) putAck() (err error) {
	select {
	case rp.ackCh <- struct{}{}:
		return
	default:
		return fmt.Errorf("ack Chan has full (%v)", len(rp.ackCh))
	}
}
