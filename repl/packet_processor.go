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
 this struct is used for packet Processor framwroek
 has three goroutine:
    a. ServerConn goroutine recive pkg from client,and check it avali,then send it to toBeProcessCh

    b. operatorAndForwardPkg goroutine read from toBeProcessCh,send it to all replicates,and do local,then send a sigle to handleCh
       and read pkg from responseCh,and write its response to client

    c. receiveFollowerResponse goroutine read from handleCh,recive all replicates  pkg response,and send this pkg to responseCh

	if any step error,then change request to error Packet,and send it to responseCh, the InteractWithClient can send it to client

*/
type PacketProcessor struct {
	listMux sync.RWMutex

	packetList *list.List    //store all recived pkg from client
	handleCh   chan struct{} //if sendto all replicates success,then send a sigle to this chan
	//the receiveFollowerResponse goroutine can recive response from allreplicates

	toBeProcessCh chan *Packet // the recive pkg goroutine recive a avali pkg,then send to this chan
	responseCh    chan *Packet //this chan used to write client

	sourceConn *net.TCPConn //in connect
	exitC      chan bool
	exited     bool
	exitedMu   sync.RWMutex

	followerConnects    map[string]*net.TCPConn //all replicates connects
	followerConnectLock sync.RWMutex

	prepareFunc  func(pkg *Packet) error
	operatorFunc func(pkg *Packet, c *net.TCPConn) error
	postFunc     func(pkg *Packet) error
}

func NewPacketProcessor(inConn *net.TCPConn, prepareFunc func(pkg *Packet) error,
	operatorFunc func(pkg *Packet, c *net.TCPConn) error, postFunc func(pkg *Packet) error) *PacketProcessor {
	processor := new(PacketProcessor)
	processor.packetList = list.New()
	processor.handleCh = make(chan struct{}, RequestChanSize)
	processor.toBeProcessCh = make(chan *Packet, RequestChanSize)
	processor.responseCh = make(chan *Packet, RequestChanSize)
	processor.exitC = make(chan bool, 1)
	processor.sourceConn = inConn
	processor.followerConnects = make(map[string]*net.TCPConn, 0)
	processor.prepareFunc = prepareFunc
	processor.operatorFunc = operatorFunc
	processor.postFunc = postFunc
	go processor.operatorAndForwardPkg()
	go processor.receiveFollowerResponse()

	return processor
}

func (processor *PacketProcessor) ServerConn() {
	var (
		err error
	)
	defer func() {
		if err != nil && err != io.EOF &&
			!strings.Contains(err.Error(), "closed connection") &&
			!strings.Contains(err.Error(), "reset by peer") {
			log.LogErrorf("action[serveConn] err(%v).", err)
		}
		processor.sourceConn.Close()
	}()
	for {
		select {
		case <-processor.exitC:
			log.LogDebugf("action[DataNode.serveConn] event loop for %v exit.", processor.sourceConn.RemoteAddr())
			return
		default:
			if err = processor.readPkgFromSocket(); err != nil {
				processor.Stop()
				return
			}
		}
	}

}

func (processor *PacketProcessor) readPkgFromSocket() (err error) {
	pkg := NewPacket()
	if err = pkg.ReadFromConnFromCli(processor.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readPkgFromSocket] read packet(%v) from remote(%v).",
		pkg.GetUniqueLogId(), processor.sourceConn.RemoteAddr().String())
	if err = pkg.resolveReplicateAddrs(); err != nil {
		processor.responseCh <- pkg
		return
	}
	if err = processor.prepareFunc(pkg); err != nil {
		processor.responseCh <- pkg
		return
	}
	processor.toBeProcessCh <- pkg

	return
}

func (processor *PacketProcessor) operatorAndForwardPkg() {
	for {
		select {
		case request := <-processor.toBeProcessCh:
			if !request.isForwardPacket() {
				processor.operatorFunc(request, processor.sourceConn)
				processor.responseCh <- request
			} else {
				if _, err := processor.sendToAllfollowers(request); err == nil {
					processor.operatorFunc(request, processor.sourceConn)
				}
				processor.handleCh <- struct{}{}
			}
		case request := <-processor.responseCh:
			processor.writeResponseToClient(request)
		case <-processor.exitC:
			processor.CleanResource()
			return
		}
	}

}

// Receive response from all followers.
func (processor *PacketProcessor) receiveFollowerResponse() {
	for {
		select {
		case <-processor.handleCh:
			processor.reciveAllFollowerResponse()
		case <-processor.exitC:
			return
		}
	}
}

func (processor *PacketProcessor) sendToAllfollowers(request *Packet) (index int, err error) {
	processor.PushPacketToList(request)
	for index = 0; index < len(request.followersConns); index++ {
		err = processor.AllocateFollowersConnects(request, index)
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", processor.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllfollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
		nodes := request.RemainReplicates
		request.RemainReplicates = 0
		if err == nil {
			err = request.WriteToConn(request.followersConns[index])
		}
		request.RemainReplicates = nodes
		if err != nil {
			msg := fmt.Sprintf("request inconnect(%v) to(%v) err(%v)", processor.sourceConn.RemoteAddr().String(),
				request.followersAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllfollowers Error", request.GetUniqueLogId())
			request.PackErrorBody(ActionSendToFollowers, err.Error())
			return
		}
	}

	return
}

func (processor *PacketProcessor) reciveAllFollowerResponse() {
	var (
		e *list.Element
	)

	if e = processor.GetFrontPacket(); e == nil {
		return
	}
	request := e.Value.(*Packet)
	defer func() {
		processor.DelPacketFromList(request)
	}()
	for index := 0; index < len(request.followersAddrs); index++ {
		err := processor.receiveFromFollower(request, index)
		if err != nil {
			request.PackErrorBody(ActionReceiveFromFollower, err.Error())
			request.forceDestoryAllConnect()
			return
		}
	}
	request.PackOkReply()
	return
}

func (processor *PacketProcessor) receiveFromFollower(request *Packet, index int) (err error) {
	// Receive pkg response from one member*/
	if request.followersConns[index] == nil {
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

	if err = reply.ReadFromConn(request.followersConns[index], proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		log.LogErrorf("action[ActionReceiveFromFollower] %v.", request.LogMessage(ActionReceiveFromFollower, request.followersAddrs[index], request.StartT, err))
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
		err = fmt.Errorf(ActionCheckReplyAvail+" request (%v) reply(%v) %v from localAddr(%v)"+
			" remoteAddr(%v) requestCrc(%v) replyCrc(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.followersAddrs[index],
			request.followersConns[index].LocalAddr().String(), request.followersConns[index].RemoteAddr().String(), request.CRC, reply.CRC)
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
func (processor *PacketProcessor) writeResponseToClient(reply *Packet) {
	var err error
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, processor.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		reply.forceDestoryAllConnect()
		log.LogErrorf(ActionWriteToCli+" %v", err)
	}
	processor.postFunc(reply)
	if !reply.NeedReply {
		log.LogDebugf(ActionWriteToCli+" %v", reply.LogMessage(ActionWriteToCli,
			processor.sourceConn.RemoteAddr().String(), reply.StartT, err))
		return
	}

	if err = reply.WriteToConn(processor.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, processor.sourceConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf(ActionWriteToCli+" %v", err)
		reply.forceDestoryAllConnect()
		processor.Stop()
	}
	log.LogDebugf(ActionWriteToCli+" %v", reply.LogMessage(ActionWriteToCli,
		processor.sourceConn.RemoteAddr().String(), reply.StartT, err))

}

/*the processor stop*/
func (processor *PacketProcessor) Stop() {
	processor.exitedMu.Lock()
	defer processor.exitedMu.Unlock()
	if !processor.exited {
		if processor.exitC != nil {
			close(processor.exitC)
		}
		processor.exited = true
	}

}

/*
 allocate followers connects,if it is extentStore and it is Write op,then use last connects
*/
func (processor *PacketProcessor) AllocateFollowersConnects(pkg *Packet, index int) (err error) {
	var conn *net.TCPConn
	if pkg.StoreMode == proto.NormalExtentMode {
		key := fmt.Sprintf("%v_%v_%v", pkg.PartitionID, pkg.ExtentID, pkg.followersAddrs[index])
		processor.followerConnectLock.RLock()
		conn := processor.followerConnects[key]
		processor.followerConnectLock.RUnlock()
		if conn == nil {
			conn, err = gConnPool.GetConnect(pkg.followersAddrs[index])
			if err != nil {
				return
			}
			processor.followerConnectLock.Lock()
			processor.followerConnects[key] = conn
			processor.followerConnectLock.Unlock()
		}
		pkg.followersConns[index] = conn
	} else {
		conn, err = gConnPool.GetConnect(pkg.followersAddrs[index])
		if err != nil {
			return
		}
		pkg.followersConns[index] = conn
	}
	return nil
}

/*get front packet*/
func (processor *PacketProcessor) GetFrontPacket() (e *list.Element) {
	processor.listMux.RLock()
	e = processor.packetList.Front()
	processor.listMux.RUnlock()

	return
}

func (processor *PacketProcessor) PushPacketToList(e *Packet) {
	processor.listMux.Lock()
	processor.packetList.PushBack(e)
	processor.listMux.Unlock()
}

/*if the processor exit,then clean all packet resource*/
func (processor *PacketProcessor) CleanResource() {
	processor.listMux.Lock()
	for e := processor.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		request.forceDestoryAllConnect()
		processor.postFunc(request)

	}
	replys := len(processor.responseCh)
	for i := 0; i < replys; i++ {
		<-processor.responseCh
	}
	processor.packetList = list.New()
	processor.followerConnectLock.RLock()
	for _, conn := range processor.followerConnects {
		conn.Close()
	}
	processor.followerConnectLock.RUnlock()
	processor.followerConnectLock.Lock()
	processor.followerConnects = make(map[string]*net.TCPConn, 0)
	processor.followerConnectLock.Unlock()
	processor.listMux.Unlock()
}

/*delete source packet*/
func (processor *PacketProcessor) DelPacketFromList(reply *Packet) (success bool) {
	processor.listMux.Lock()
	defer processor.listMux.Unlock()
	for e := processor.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
			request.forceDestoryAllConnect()
			request.PackErrorBody(ActionReceiveFromFollower, fmt.Sprintf("unknow expect reply"))
			break
		}
		processor.packetList.Remove(e)
		success = true
		processor.responseCh <- reply
	}

	return
}
