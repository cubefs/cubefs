// Copyright 2018 The CubeFS Authors.
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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

var gConnPool = util.NewConnectPool()

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

	sourceConn net.Conn
	exitC      chan bool
	exited     int32
	exitedMu   sync.RWMutex

	followerConnects map[string]*FollowerTransport
	lock             sync.RWMutex

	prepareFunc  func(p *Packet) error             // prepare packet
	operatorFunc func(p *Packet, c net.Conn) error // operator
	postFunc     func(p *Packet) error             // post-processing packet

	getSmuxConn func(addr string) (c net.Conn, err error)
	putSmuxConn func(conn net.Conn, force bool)

	isError int32
	replId  int64
}

type FollowerTransport struct {
	addr     string
	conn     net.Conn
	sendCh   chan *FollowerPacket
	recvCh   chan *FollowerPacket
	exitCh   chan struct{}
	exitedMu sync.RWMutex
	isclosed int32
}

func NewFollowersTransport(addr string, c net.Conn) (ft *FollowerTransport, err error) {
	ft = new(FollowerTransport)
	ft.addr = addr
	ft.conn = c
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
			if err := p.WriteToConn(ft.conn); err != nil {
				p.PackErrorBody(ActionSendToFollowers, err.Error())
				p.respCh <- fmt.Errorf(string(p.Data[:p.Size]))
				log.LogErrorf("serverWriteToFollower ft.addr(%v), req(%s), err (%v)", ft.addr, p.String(), err.Error())
				ft.conn.Close()
				continue
			}
			ft.recvCh <- p
		case <-ft.exitCh:
			ft.exitedMu.Lock()
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			ft.exitedMu.Unlock()
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
			ft.exitedMu.Lock()
			if atomic.AddInt32(&ft.isclosed, -1) == FollowerTransportExited {
				ft.conn.Close()
				atomic.StoreInt32(&ft.isclosed, FollowerTransportExited)
			}
			ft.exitedMu.Unlock()
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
		if err != nil {
			ft.conn.Close()
		}
	}()

	if request.IsErrPacket() {
		err = fmt.Errorf(string(request.Data[:request.Size]))
		return
	}
	timeOut := proto.ReadDeadlineTime
	if request.IsBatchDeleteExtents() || request.IsBatchLockNormalExtents() || request.IsBatchUnlockNormalExtents() {
		timeOut = proto.BatchDeleteExtentReadDeadLineTime
	}
	if err = reply.ReadFromConnWithVer(ft.conn, timeOut); err != nil {
		if request.Opcode == proto.OpStreamFollowerRead && strings.Contains(err.Error(), "timeout") {
			log.LogWarnf("readFollowerResult ft.addr(%v), err(%v)", ft.addr, err.Error())
		} else {
			log.LogErrorf("readFollowerResult ft.addr(%v), err(%v)", ft.addr, err.Error())
		}
		return
	}

	if reply.GetReqID() != request.ReqID || reply.GetPartitionID() != request.PartitionID ||
		reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
		err = fmt.Errorf(ActionCheckReply+" request(%v), reply(%v)  ", request.GetUniqueLogId(),
			reply.GetUniqueLogId())
		return
	}

	if reply.IsErrPacket() {
		err = fmt.Errorf(string(reply.Data[:reply.Size]))
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("action[ActionReceiveFromFollower] %v.", reply.LogMessage(ActionReceiveFromFollower,
			ft.addr, request.StartT, err))
	}
	return
}

func cleanFollowerCh(ch chan *FollowerPacket) {
	cnt := len(ch)
	for idx := 0; idx < cnt; idx++ {
		select {
		case p := <-ch:
			p.Data = nil
			continue
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

	cleanFollowerCh(ft.sendCh)
	cleanFollowerCh(ft.recvCh)

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

func NewReplProtocol(inConn net.Conn, prepareFunc func(p *Packet) error,
	operatorFunc func(p *Packet, c net.Conn) error, postFunc func(p *Packet) error,
) *ReplProtocol {
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

func (rp *ReplProtocol) SetSmux(f func(addr string) (net.Conn, error), putSmux func(conn net.Conn, force bool)) {
	rp.getSmuxConn = f
	rp.putSmuxConn = putSmux
}

// ServerConn keeps reading data from the socket to analyze the follower address, execute the prepare function,
// and throw the packets to the to-be-processed channel.
func (rp *ReplProtocol) ServerConn() {
	var err error
	defer func() {
		rp.Stop()
		rp.exitedMu.Lock()
		if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
			rp.sourceConn.Close()
			rp.cleanResource()
		}
		rp.exitedMu.Unlock()
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
			rp.exitedMu.Lock()
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			rp.exitedMu.Unlock()
			return
		}
	}
}

func (rp *ReplProtocol) setReplProtocolError(request *Packet, index int) {
	atomic.StoreInt32(&rp.isError, ReplProtocolError)
}

func (rp *ReplProtocol) readPkgAndPrepare() (err error) {
	request := NewPacket()
	if err = request.ReadFromConnWithVer(rp.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	// log.LogDebugf("action[readPkgAndPrepare] packet(%v) op %v from remote(%v) conn(%v) ",
	// 	request.GetUniqueLogId(), request.Opcode, rp.sourceConn.RemoteAddr().String(), rp.sourceConn)

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
//  1. Read a packet from toBeProcessCh, and determine if it needs to be forwarded or not. If the answer is no, then
//     process the packet locally and put it into responseCh.
//  2. If the packet needs to be forwarded, the first send it to the followers, and execute the operator function.
//     Then notify receiveResponse to read the followers' responses.
//  3. Read a reply from responseCh, and write to the client.
func (rp *ReplProtocol) OperatorAndForwardPktGoRoutine() {
	for {
		select {
		case request := <-rp.toBeProcessedCh:
			if !request.IsForwardPacket() {
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
			rp.exitedMu.Lock()
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			rp.exitedMu.Unlock()
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
			rp.exitedMu.Lock()
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResource()
			}
			rp.exitedMu.Unlock()
			return
		}
	}
}

// func (rp *ReplProtocol) operatorFuncWithWaitGroup(wg *sync.WaitGroup, request *Packet) {
// 	defer wg.Done()
// 	rp.operatorFunc(request, rp.sourceConn)
// }

// Read a packet from the list, scan all the connections of the followers of this packet and read the responses.
// If failed to read the response, then mark the packet as failure, and delete it from the list.
// If all the reads succeed, then mark the packet as success.
func (rp *ReplProtocol) checkLocalResultAndReciveAllFollowerResponse() {
	var e *list.Element

	if e = rp.getNextPacket(); e == nil {
		return
	}
	response := e.Value.(*Packet)
	defer func() {
		rp.deletePacket(response, e)
	}()
	if response.IsErrPacket() {
		return
	}
	// NOTE: wait for all followers
	for index := 0; index < len(response.followersAddrs); index++ {
		followerPacket := response.followerPackets[index]
		err := <-followerPacket.respCh
		if err != nil {
			// NOTE: we meet timeout error
			// set the request status to be timeout
			if err == os.ErrDeadlineExceeded {
				response.PackErrorBody(ActionReceiveFromFollower, err.Error())
				return
			}
			// NOTE: other errors, continue to receive response from followers
			response.PackErrorBody(ActionReceiveFromFollower, err.Error())
			continue
		}
	}
}

// Write a reply to the client.
func (rp *ReplProtocol) writeResponse(reply *Packet) {
	var err error
	defer func() {
		reply.clean()
	}()
	if log.EnableDebug() {
		log.LogDebugf("writeResponse.opcode %v reply %v conn(%v)", reply.Opcode, reply.GetUniqueLogId(),
			rp.sourceConn.RemoteAddr().String())
	}
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		if reply.IsWriteOpOfPacketProtoVerForbidden() {
			log.LogDebugf(err.Error())
		} else if reply.ResultCode == proto.OpNotExistErr || reply.ResultCode == proto.ErrCodeVersionOpError || reply.ResultCode == proto.OpTinyRecoverErr || reply.ResultCode == proto.OpLimitedIoErr || reply.ResultCode == proto.OpDpDecommissionRepairErr || reply.ResultCode == proto.OpDpRepairErr {
			log.LogInfof(err.Error())
		} else if (reply.ResultCode == proto.OpTryOtherAddr && reply.Opcode == proto.OpWrite) ||
			reply.Opcode == proto.OpReadTinyDeleteRecord ||
			(reply.Opcode == proto.OpWrite && proto.IsTinyExtentType(reply.ExtentType) && strings.Contains(err.Error(), "GetAvailableTinyExtent error no available extent")) ||
			(reply.Opcode == proto.OpWrite && reply.ResultCode == proto.OpLimitedIoErr && strings.Contains(err.Error(), ActionReceiveFromFollower)) ||
			strings.Contains(err.Error(), proto.ErrDataPartitionNotExists.Error()) {
			log.LogWarnf(err.Error())
		} else {
			log.LogErrorf(err.Error())
		}
		rp.Stop()
	}
	if log.EnableDebug() {
		log.LogDebugf("try rsp opcode %v %v %v", rp.replId, reply.Opcode, rp.sourceConn.RemoteAddr().String())
	}
	// execute the post-processing function
	rp.postFunc(reply)
	if !reply.NeedReply {
		if reply.Opcode == proto.OpTryWriteAppend || reply.Opcode == proto.OpSyncTryWriteAppend {
			log.LogDebugf("try rsp opcode %v", reply.Opcode)
		}
		return
	}

	if err = reply.WriteToConn(rp.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, fmt.Sprintf("local(%v)->remote(%v)", rp.sourceConn.LocalAddr().String(),
			rp.sourceConn.RemoteAddr().String()), reply.StartT, err))
		log.LogErrorf(err.Error())
		rp.Stop()
	}
	if log.EnableDebug() {
		log.LogDebugf(reply.LogMessage(ActionWriteToClient,
			rp.sourceConn.RemoteAddr().String(), reply.StartT, err))
	}
}

// Stop stops the replication protocol.
func (rp *ReplProtocol) Stop() {
	rp.exitedMu.Lock()
	defer rp.exitedMu.Unlock()
	if atomic.LoadInt32(&rp.exited) == ReplRuning {
		if rp.exitC != nil {
			close(rp.exitC)
		}
		atomic.StoreInt32(&rp.exited, ReplExiting)
	}
}

type SmuxConn struct {
	once sync.Once
	net.Conn
	put func(conn net.Conn, force bool)
}

func (d *SmuxConn) Close() error {
	d.once.Do(func() {
		d.put(d.Conn, true)
	})
	return nil
}

// Allocate the connections to the followers. We use partitionId + extentId + followerAddr as the key.
// Note that we need to ensure the order of packets sent to the datanode is consistent here.
func (rp *ReplProtocol) allocateFollowersConns(p *Packet, index int) (transport *FollowerTransport, err error) {
	rp.lock.RLock()
	transport = rp.followerConnects[p.followersAddrs[index]]
	rp.lock.RUnlock()
	if transport == nil {
		addr := p.followersAddrs[index]

		var conn net.Conn
		if (p.IsMarkDeleteExtentOperation() || p.IsBatchDeleteExtents()) && rp.getSmuxConn != nil {
			var smuxCon net.Conn
			smuxCon, err = rp.getSmuxConn(addr)
			if err != nil {
				return
			}

			conn = &SmuxConn{
				Conn: smuxCon,
				put:  rp.putSmuxConn,
			}

		} else {
			conn, err = gConnPool.GetConnect(addr)
			if err != nil {
				return
			}
		}

		transport, err = NewFollowersTransport(addr, conn)
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
		err = fmt.Errorf("response Chan has full (%v)", len(rp.responseCh))
		log.LogError(err)
		return err
	}
}

func (rp *ReplProtocol) putToBeProcess(request *Packet) (err error) {
	select {
	case rp.toBeProcessedCh <- request:
		return
	default:
		err = fmt.Errorf("toBeProcessedCh Chan has full (%v)", len(rp.toBeProcessedCh))
		log.LogError(err)
		return err
	}
}

func (rp *ReplProtocol) putAck() (err error) {
	select {
	case rp.ackCh <- struct{}{}:
		return
	default:
		err = fmt.Errorf("ack Chan has full (%v)", len(rp.ackCh))
		log.LogError(err)
		return err
	}
}
