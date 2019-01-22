// Copyright 2018 The Container File System Authors.
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
	"io"
	"strings"
)

var (
	gConnPool = util.NewConnectPool()
)

// ReplProtocol defines the struct of the replication protocol.
// 1. ServerConn reads a packet from the client socket, and analyzes the addresses of the followers.
// 2. After the preparation, the packet is send to toBeProcessedCh. If failure happens, send it to the response channel.
// 3. OperatorAndForwardPkt fetches a packet from toBeProcessedCh, and determine if it needs to be forwarded to the followers.
// 4. receiveResponse fetches a reply from responseCh, executes postFunc, and writes a response to the client if necessary.
type ReplProtocol struct {
	packetListLock sync.RWMutex

	packetList *list.List    // stores all the received packets from the client
	ackCh      chan struct{} // if sending to all the replicas succeeds, then a signal to this channel

	toBeProcessedCh chan *Packet // the goroutine receives an available packet and then sends it to this channel
	responseCh      chan *Packet // this chan is used to write response to the client

	sourceConn *net.TCPConn
	exitC      chan bool
	exited     bool
	exitedMu   sync.RWMutex

	followerConnects *sync.Map

	prepareFunc  func(p *Packet) error                 // prepare packet
	operatorFunc func(p *Packet, c *net.TCPConn) error // operator
	postFunc     func(p *Packet) error                 // post-processing packet

	isError bool
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
	go rp.OperatorAndForwardPkt()
	go rp.ReceiveResponse()

	return rp
}

// ServerConn keeps reading data from the socket to analyze the follower address, execute the prepare function,
// and throw the packets to the to-be-processed channel.
func (rp *ReplProtocol) ServerConn() {
	var (
		err error
	)
	defer func() {
		if err != nil && err != io.EOF &&
			!strings.Contains(err.Error(), "closed connection") &&
			!strings.Contains(err.Error(), "reset by peer") {
			log.LogErrorf("action[serveConn] err(%v).", err)
		}
		rp.Stop()
		rp.sourceConn.Close()
	}()
	for {
		select {
		case <-rp.exitC:
			log.LogDebugf("action[DataNode.serveConn] event loop for %v exit.", rp.sourceConn.RemoteAddr())
			return
		default:
			if err = rp.readPkgAndPrepare(); err != nil {
				return
			}
		}
	}

}

func (rp *ReplProtocol) readPkgAndPrepare() (err error) {
	p := NewPacket()
	if err = p.ReadFromConnFromCli(rp.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readPkgAndPrepare] read packet(%v) from remote(%v).",
		p.GetUniqueLogId(), rp.sourceConn.RemoteAddr().String())
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

// OperatorAndForwardPkt reads packets from the to-be-processed channel and writes responses to the client.
// 1. Read a packet from toBeProcessCh, and determine if it needs to be forwarded or not. If the answer is no, then
// 	  process the packet locally and put it into responseCh.
// 2. If the packet needs to be forwarded, the first send it to the followers, and execute the operator function.
//    Then notify receiveResponse to read the followers' responses.
// 3. Read a reply from responseCh, and write to the client.
func (rp *ReplProtocol) OperatorAndForwardPkt() {
	for {
		select {
		case request := <-rp.toBeProcessedCh:
			if !request.isForwardPacket() {
				rp.operatorFunc(request, rp.sourceConn)
				rp.responseCh <- request
			} else {
				_, err := rp.sendRequestToAllFollowers(request)
				if err == nil {
					rp.operatorFunc(request, rp.sourceConn)
				} else {
					rp.isError = true
					log.LogErrorf(err.Error())
				}
				rp.ackCh <- struct{}{}
			}
		case request := <-rp.responseCh:
			rp.writeResponseToClient(request)
		case <-rp.exitC:
			rp.cleanResource()
			return
		}
	}

}

// Receive response from all followers.
func (rp *ReplProtocol) ReceiveResponse() {
	for {
		select {
		case <-rp.ackCh:
			rp.reciveAllFollowerResponse()
		case <-rp.exitC:
			return
		}
	}
}

func (rp *ReplProtocol) sendRequestToAllFollowers(request *Packet) (index int, err error) {
	rp.pushPacketToList(request)
	for index = 0; index < len(request.followerConns); index++ {
		err = rp.allocateFollowersConns(request, index)
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", rp.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendRequestToAllFollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
		nodes := request.RemainingFollowers
		request.RemainingFollowers = 0
		if err == nil {
			err = request.WriteToConn(request.followerConns[index])
		}
		request.RemainingFollowers = nodes
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", rp.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendRequestToAllFollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			rp.isError = true

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
			request.PackErrorBody(ActionReceiveFromFollower, err.Error())
			rp.isError = true
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
		err = errors.Annotatef(fmt.Errorf(ConnIsNullErr), "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		return
	}

	// Check local execution result.
	if request.IsErrPacket() {
		err = errors.Annotatef(fmt.Errorf(request.getErrMessage()), "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		log.LogErrorf("action[ActionReceiveFromFollower] %v.",
			request.LogMessage(ActionReceiveFromFollower, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErrMessage())))
		return
	}

	reply := NewPacket()

	if err = reply.ReadFromConn(request.followerConns[index], proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
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
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		rp.isError = true
		log.LogErrorf(ActionWriteToClient+" %v", err)
	}

	// execute the post-processing function
	rp.postFunc(reply)
	if !reply.NeedReply {
		log.LogDebugf(ActionWriteToClient+" %v", reply.LogMessage(ActionWriteToClient,
			rp.sourceConn.RemoteAddr().String(), reply.StartT, err))
		return
	}

	if err = reply.WriteToConn(rp.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf(ActionWriteToClient+" %v", err)
		rp.isError = true
		rp.Stop()
	}
	log.LogDebugf(ActionWriteToClient+" %v", reply.LogMessage(ActionWriteToClient,
		rp.sourceConn.RemoteAddr().String(), reply.StartT, err))

}

// Stop stops the replication protocol.
func (rp *ReplProtocol) Stop() {
	rp.exitedMu.Lock()
	defer rp.exitedMu.Unlock()
	if !rp.exited {
		if rp.exitC != nil {
			close(rp.exitC)
		}
		rp.exited = true
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
			if rp.isError {
				conn.Close()
			} else {
				gConnPool.PutConnect(conn, NoClosedConn)
			}
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
			rp.isError = true
			request.PackErrorBody(ActionReceiveFromFollower, fmt.Sprintf("unknow expect reply"))
			break
		}
		rp.packetList.Remove(e)
		success = true
		rp.responseCh <- reply
	}

	return
}
