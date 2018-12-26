// Copyright 2018 The Containerfs Authors.
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

/*
 this struct is used for packet rp framwroek
 has three goroutine:
    a. ServerConn goroutine recive pkg from client,and check it avali,then send it to toBeProcessCh

    b. operatorAndForwardPkg goroutine read from toBeProcessCh,send it to all replicates,and do local,then send a sigle to notifyReciveCh
       and read pkg from responseCh,and write its response to client

    c. receiveResponse goroutine read from notifyReciveCh,recive all replicates  pkg response,and send this pkg to responseCh

	if any step error,then change request to error Packet,and send it to responseCh, the operatorAndForwardPkg can send it to client

*/
type ReplProtocol struct {
	packetListLock sync.RWMutex

	packetList     *list.List    //store all recived pkg from client
	notifyReciveCh chan struct{} //if sendto all replicates success,then send a sigle to this chan
	//the receiveResponse goroutine can recive response from allreplicates

	toBeProcessCh chan *Packet // the recive pkg goroutine recive a avali pkg,then send to this chan
	responseCh    chan *Packet //this chan used to write client

	sourceConn *net.TCPConn //in connect
	exitC      chan bool
	exited     bool
	exitedMu   sync.RWMutex

	followerConnects *sync.Map

	prepareFunc  func(pkg *Packet) error                 //this func is used for prepare packet
	operatorFunc func(pkg *Packet, c *net.TCPConn) error //this func is used for operator func
	postFunc     func(pkg *Packet) error                 //this func is used from post packet
}

func NewReplProtocol(inConn *net.TCPConn, prepareFunc func(pkg *Packet) error,
	operatorFunc func(pkg *Packet, c *net.TCPConn) error, postFunc func(pkg *Packet) error) *ReplProtocol {
	rp := new(ReplProtocol)
	rp.packetList = list.New()
	rp.notifyReciveCh = make(chan struct{}, RequestChanSize)
	rp.toBeProcessCh = make(chan *Packet, RequestChanSize)
	rp.responseCh = make(chan *Packet, RequestChanSize)
	rp.exitC = make(chan bool, 1)
	rp.sourceConn = inConn
	rp.followerConnects = new(sync.Map)
	rp.prepareFunc = prepareFunc
	rp.operatorFunc = operatorFunc
	rp.postFunc = postFunc
	go rp.operatorAndForwardPkg()
	go rp.receiveResponse()

	return rp
}

/*
  this func is server client connnect ,read pkg from socket,and do prepare pkg
*/
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
		rp.sourceConn.Close()
	}()
	for {
		select {
		case <-rp.exitC:
			log.LogDebugf("action[DataNode.serveConn] event loop for %v exit.", rp.sourceConn.RemoteAddr())
			return
		default:
			if err = rp.readPkgAndPrepare(); err != nil {
				rp.Stop()
				return
			}
		}
	}

}

/*
   read pkg from client socket,and resolve followers addr,then prepare pkg
*/
func (rp *ReplProtocol) readPkgAndPrepare() (err error) {
	pkg := NewPacket()
	if err = pkg.ReadFromConnFromCli(rp.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readPkgAndPrepare] read packet(%v) from remote(%v).",
		pkg.GetUniqueLogId(), rp.sourceConn.RemoteAddr().String())
	if err = pkg.resolveFollowersAddr(); err != nil {
		rp.responseCh <- pkg
		return
	}
	if err = rp.prepareFunc(pkg); err != nil {
		rp.responseCh <- pkg
		return
	}
	rp.toBeProcessCh <- pkg

	return
}

/*
   read pkg from toBeProcessCh,and if pkg need forward to all followers,send it to all followers
   if send to followers,then do pkg by opcode,then notify receiveResponse gorotine,recive response
   if packet donnot need forward,do pkg by opcode

   read response from responseCh,and write response to client
*/
func (rp *ReplProtocol) operatorAndForwardPkg() {
	for {
		select {
		case request := <-rp.toBeProcessCh:
			if !request.isForwardPacket() {
				rp.operatorFunc(request, rp.sourceConn)
				rp.responseCh <- request
			} else {
				_, err := rp.sendRequestToAllfollowers(request)
				if err == nil {
					rp.operatorFunc(request, rp.sourceConn)
				} else {
					log.LogErrorf(err.Error())
				}
				rp.notifyReciveCh <- struct{}{}
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
func (rp *ReplProtocol) receiveResponse() {
	for {
		select {
		case <-rp.notifyReciveCh:
			rp.reciveAllFollowerResponse()
		case <-rp.exitC:
			return
		}
	}
}

/*
  send pkg to all followers.
*/
func (rp *ReplProtocol) sendRequestToAllfollowers(request *Packet) (index int, err error) {
	rp.pushPacketToList(request)
	for index = 0; index < len(request.followerConns); index++ {
		err = rp.AllocateFollowersConnects(request, index)
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", rp.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendRequestToAllfollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
		nodes := request.RemainFollowers
		request.RemainFollowers = 0
		if err == nil {
			err = request.WriteToConn(request.followerConns[index])
		}
		request.RemainFollowers = nodes
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", rp.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendRequestToAllfollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
	}

	return
}

/*
	recive response from all followers,if any followers failed,then the packet is failed

*/
func (rp *ReplProtocol) reciveAllFollowerResponse() {
	var (
		e *list.Element
	)

	if e = rp.getFrontPacket(); e == nil {
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
			return
		}
	}
	request.PackOkReply()
	return
}

/*
  recive reply from followers
  1. check request local do is failed,if failed ,return error
  2. read from follower socket,if failed,return error
  3. check reply is avali,if reply is not avali,then return error
  4. check reply is a error packet,if reply error,return error
*/
func (rp *ReplProtocol) receiveFromFollower(request *Packet, index int) (err error) {
	// Receive pkg response from one member*/
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
		err = fmt.Errorf(ActionCheckReplyAvail+" request (%v) reply(%v) %v from localAddr(%v)"+
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

// Write response to client and recycle the connect.
func (rp *ReplProtocol) writeResponseToClient(reply *Packet) {
	var err error
	if reply.IsErrPacket() {
		if reply.Size <= uint32(len(reply.Data)) {
			err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
				reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		} else {
			err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
				reply.StartT, fmt.Errorf(string(reply.Data[:len(reply.Data)]))))
		}

		reply.forceDestoryWholeFollowersPool(err)
		log.LogErrorf(ActionWriteToClient+" %v", err)
	}
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
		reply.forceDestoryFollowerConnects()
		rp.Stop()
	}
	log.LogDebugf(ActionWriteToClient+" %v", reply.LogMessage(ActionWriteToClient,
		rp.sourceConn.RemoteAddr().String(), reply.StartT, err))

}

/*the rp stop*/
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

/*
 allocate followers connects,if it is extentStore and it is Write op,then use last connects
*/
func (rp *ReplProtocol) AllocateFollowersConnects(pkg *Packet, index int) (err error) {
	var conn *net.TCPConn
	if pkg.ExtentMode == proto.NormalExtentMode {
		key := fmt.Sprintf("%v_%v_%v", pkg.PartitionID, pkg.ExtentID, pkg.followersAddrs[index])
		value, ok := rp.followerConnects.Load(key)
		if ok {
			pkg.followerConns[index] = value.(*net.TCPConn)
		} else {
			conn, err = gConnPool.GetConnect(pkg.followersAddrs[index])
			if err != nil {
				return
			}
			rp.followerConnects.Store(key, conn)
			pkg.followerConns[index] = conn
		}
	} else {
		conn, err = gConnPool.GetConnect(pkg.followersAddrs[index])
		if err != nil {
			return
		}
		pkg.followerConns[index] = conn
	}
	return nil
}

/*get front packet*/
func (rp *ReplProtocol) getFrontPacket() (e *list.Element) {
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

/*if the rp exit,then clean all packet resource*/
func (rp *ReplProtocol) cleanResource() {
	rp.packetListLock.Lock()
	for e := rp.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		request.forceDestoryFollowerConnects()
		rp.postFunc(request)

	}
	replys := len(rp.responseCh)
	for i := 0; i < replys; i++ {
		pkg := <-rp.responseCh
		rp.postFunc(pkg)
	}
	request := len(rp.toBeProcessCh)
	for i := 0; i < request; i++ {
		pkg := <-rp.responseCh
		rp.postFunc(pkg)
	}
	rp.packetList = list.New()
	rp.followerConnects.Range(
		func(key, value interface{}) bool {
			conn := value.(*net.TCPConn)
			conn.Close()
			return true
		})
	rp.packetListLock.Unlock()
}

/*delete source packet*/
func (rp *ReplProtocol) deletePacket(reply *Packet) (success bool) {
	rp.packetListLock.Lock()
	defer rp.packetListLock.Unlock()
	for e := rp.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
			request.forceDestoryFollowerConnects()
			request.PackErrorBody(ActionReceiveFromFollower, fmt.Sprintf("unknow expect reply"))
			break
		}
		rp.packetList.Remove(e)
		success = true
		rp.responseCh <- reply
	}

	return
}
