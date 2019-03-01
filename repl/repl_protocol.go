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

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"sync/atomic"
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
	exitedMu   sync.RWMutex

	followerConnects *sync.Map

	prepareFunc  func(p *Packet) error                 // prepare packet
	operatorFunc func(p *Packet, c *net.TCPConn) error // operator
	postFunc     func(p *Packet) error                 // post-processing packet

	isError   int32
	sendError []error
	replId    int64
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
	rp.followerConnects = new(sync.Map)
	rp.prepareFunc = prepareFunc
	rp.operatorFunc = operatorFunc
	rp.postFunc = postFunc
	rp.exited = ReplRuning
	rp.replId = proto.GenerateRequestID()
	go rp.OperatorAndForwardPktGoRoutine()
	go rp.ReceiveResponseFromFollowersGoRoutine()
	go rp.WriteResponseToClientGoRoutine()

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
		if atomic.LoadInt32(&rp.exited) == ReplHasExited {
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
			rp.reciveAllFollowerResponse()
		case <-rp.exitC:
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResponseCh()
			}
			return
		}
	}
}

func (rp *ReplProtocol) WriteResponseToClientGoRoutine() {
	for {
		select {
		case request := <-rp.responseCh:
			rp.writeResponseToClient(request)
		case <-rp.exitC:
			if atomic.AddInt32(&rp.exited, -1) == ReplHasExited {
				rp.sourceConn.Close()
				rp.cleanResponseCh()
			}
			return
		}
	}
}

func (rp *ReplProtocol) setReplProtocolError() {
	atomic.StoreInt32(&rp.isError, ReplProtocolError)
}

func (rp *ReplProtocol) hasError() bool {
	return atomic.LoadInt32(&rp.isError) == ReplProtocolError
}

func (rp *ReplProtocol) readPkgAndPrepare() (err error) {
	p := NewPacket()
	if err = p.ReadFromConnFromCli(rp.sourceConn, util.ConnectIdleTime); err != nil {
		return
	}
	log.LogDebugf("action[readPkgAndPrepare] packet(%v) from remote(%v) localAddr(%v).",
		p.GetUniqueLogId(), rp.sourceConn.RemoteAddr().String(), rp.sourceConn.LocalAddr().String())
	if err = p.resolveFollowersAddr(); err != nil {
		rp.responseCh <- p
		return
	}
	if err = rp.prepareFunc(p); err != nil {
		rp.responseCh <- p
		return
	}
	rp.toBeProcessedCh <- p

	return
}

func (rp *ReplProtocol) sendRequestToFollower(wg *sync.WaitGroup, followerRequest *Packet, index int) {
	defer func() {
		wg.Done()
	}()
	followerRequest.RemainingFollowers = 0
	rp.sendError[index] = rp.allocateFollowersConns(followerRequest, index)
	if rp.sendError[index] != nil {
		return
	}
	rp.sendError[index] = followerRequest.WriteToConn(followerRequest.followerConns[index])

}

func (rp *ReplProtocol) sendRequestToAllFollowers(wg *sync.WaitGroup, request *Packet) {
	rp.sendError = make([]error, len(request.followersAddrs))
	followerRequests:=make([]*Packet,len(request.followersAddrs))
	for index := 0; index < len(request.followersAddrs); index++ {
		followerRequests[index] = new(Packet)
		copyPacket(request,followerRequests[index])
	}
	for index := 0; index < len(request.followersAddrs); index++ {
		wg.Add(1)
		go rp.sendRequestToFollower(wg, followerRequests[index], index)
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
				rp.responseCh <- request
			} else {
				wg := new(sync.WaitGroup)
				orgRemainNodes := request.RemainingFollowers
				rp.pushPacketToList(request)
				rp.sendRequestToAllFollowers(wg, request)
				wg.Add(1)
				request.RemainingFollowers = orgRemainNodes
				go rp.operatorFuncWithWaitGroup(wg, request)
				wg.Wait()
				rp.checkSendErrors(request)
				rp.ackCh <- struct{}{}
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

func (rp *ReplProtocol) operatorFuncWithWaitGroup(wg *sync.WaitGroup, request *Packet) {
	defer wg.Done()
	rp.operatorFunc(request, rp.sourceConn)
}

func (rp *ReplProtocol) checkSendErrors(request *Packet) (hasError bool) {
	for index := 0; index < len(request.followersAddrs); index++ {
		if rp.sendError[index] != nil {
			rp.setReplProtocolError()
			hasError = true
			err := errors.Annotatef(rp.sendError[index], "sendRequestToAllFollowers to (%v)", request.followersAddrs[index])
			request.PackErrorBody(ActionSendToFollowers, rp.sendError[index].Error())
			log.LogErrorf(err.Error())
			return
		}
	}
	return
}

// Read a packet from the list, scan all the connections of the followers of this packet and read the responses.
// If failed to read the response, then mark the packet as failure, and delete it from the list.
// If all the reads succeed, then mark the packet as success.
func (rp *ReplProtocol) reciveAllFollowerResponse() {
	var (
		e *list.Element
	)

	if e = rp.getNextPacket(); e == nil {
		return
	}
	request := e.Value.(*Packet)
	defer func() {
		rp.deletePacket(request)
	}()
	for index := 0; index < len(request.followersAddrs); index++ {
		err := rp.receiveFromFollower(request, index)
		if err != nil {
			rp.setReplProtocolError()
			request.PackErrorBody(ActionReceiveFromFollower, err.Error())
			return
		}
	}
	request.PacketOkReply()
	return
}

// Read the response from the follower
func (rp *ReplProtocol) receiveFromFollower(request *Packet, index int) (err error) {
	// Receive p response from one member
	if request.followerConns[index] == nil {
		err = fmt.Errorf(ConnIsNullErr)
		return
	}

	// Check local execution result.
	if request.IsErrPacket() {
		err = fmt.Errorf(request.getErrMessage())
		log.LogErrorf("action[ActionReceiveFromFollower] %v.",
			request.LogMessage(ActionReceiveFromFollower, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErrMessage())))
		return
	}

	reply := NewPacket()
	defer func() {
		reply.clean()
		if err != nil {
			request.followerConns[index].Close()
		}
	}()
	if err = reply.ReadFromConn(request.followerConns[index], proto.ReadDeadlineTime); err != nil {
		log.LogErrorf("action[ActionReceiveFromFollower] %v.", request.LogMessage(ActionReceiveFromFollower, request.followersAddrs[index], request.StartT, err))
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
		err = fmt.Errorf(ActionCheckReply+" request (%v) reply(%v) %v from localAddr(%v)"+
			" remoteAddr(%v) requestCrc(%v) replyCrc(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.followersAddrs[index],
			request.followerConns[index].LocalAddr().String(), request.followerConns[index].RemoteAddr().String(), request.CRC, reply.CRC)
		log.LogErrorf("action[receiveFromReplicate] %v.", err.Error())
		return
	}

	if reply.IsErrPacket() {
		err = fmt.Errorf(ActionReceiveFromFollower+"remote (%v) do failed(%v)",
			request.followersAddrs[index], string(reply.Data[:reply.Size]))
		err = errors.Annotatef(err, "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		return
	}

	log.LogDebugf("action[ActionReceiveFromFollower] %v.", reply.LogMessage(ActionReceiveFromFollower, request.followersAddrs[index], request.StartT, err))
	return
}

// Write a reply to the client.
func (rp *ReplProtocol) writeResponseToClient(reply *Packet) {
	var err error
	defer func() {
		reply.clean()
	}()
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		rp.setReplProtocolError()
		log.LogErrorf(err.Error())
		rp.Stop()
	}

	// execute the post-processing function
	rp.postFunc(reply)
	if !reply.NeedReply {
		log.LogDebugf(reply.LogMessage(ActionWriteToClient,
			rp.sourceConn.RemoteAddr().String(), reply.StartT, err))
		return
	}

	if err = reply.WriteToConn(rp.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf(err.Error())
		rp.setReplProtocolError()
		rp.Stop()
	}
	log.LogDebugf(reply.LogMessage(ActionWriteToClient,
		rp.sourceConn.RemoteAddr().String(), reply.StartT, err))
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

// Allocate the connections to the followers. We use partitionId + extentId + followerAddr as the key.
// Note that we need to ensure the order of packets sent to the datanode is consistent here.
func (rp *ReplProtocol) allocateFollowersConns(p *Packet, index int) (err error) {
	var conn *net.TCPConn
	key := fmt.Sprintf("%v_%v_%v", p.PartitionID, p.ExtentID, p.followersAddrs[index])
	value, ok := rp.followerConnects.Load(key)
	if ok {
		p.followerConns[index] = value.(*net.TCPConn)

	} else {
		conn, err = gConnPool.GetConnect(p.followersAddrs[index])
		if err != nil {
			return
		}
		rp.followerConnects.Store(key, conn)
		p.followerConns[index] = conn
	}

	return nil
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
	}
	rp.cleanToBeProcessCh()
	rp.cleanResponseCh()
	rp.packetList = list.New()
	rp.followerConnects.Range(
		func(key, value interface{}) bool {
			conn := value.(*net.TCPConn)
			conn.Close()
			return true
		})
	rp.packetListLock.Unlock()
}

func (rp *ReplProtocol) deletePacket(reply *Packet) (success bool) {
	rp.packetListLock.Lock()
	defer rp.packetListLock.Unlock()
	for e := rp.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
			rp.setReplProtocolError()
			request.PackErrorBody(ActionReceiveFromFollower, fmt.Sprintf("unknow expect reply"))
			break
		}
		rp.packetList.Remove(e)
		success = true
		rp.responseCh <- reply
	}

	return
}
