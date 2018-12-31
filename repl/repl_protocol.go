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
 这是关于复制协议的一个结构体。复制协议的大致流程如下：
1. ServerConn goroutine从客户端的socket读取一个包，然后解析包里面的followers地址，然后做prepare函数
   prepare完成后，丢入toBeProcessCh 队列。如果出错，则丢到responseCh队列
2. operatorAndForwardPkg goroutine 从toBeProcessCh队列里面取一个pkg，然后判断是否需要转发给follower
   a.如果需要转发，先发给所有的followers，然后本地执行operator函数，再通知receiveResponse goroutine
     让该goroutine从follower connection上读取follower response，再将该pkg丢到responseCh队列
   b.如果不需要转发，则执行operator函数，再将该pkg丢到responseCh队列
3.receiveResponse goroutine 从responseCh队列拿出一个reply，执行postFunc,然后判断是否需要给客户端回复响应。
  如果需要，则写入客户端的socket

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
  该函数不断地从客户端的socket上读取socket，然后解析follower地址，执行prepare函数，丢入toBeProcessCh队列
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
  该函数不断地从客户端的socket上读取socket，然后解析follower地址，执行prepare函数，丢入toBeProcessCh队列
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
   1.从toBeProcessCh 队列读取一个pkg,判断该pkg是否需要转发，如果不需要转发，则本地执行，放入responseCh队列
   2.如果该pkg需要转发，则先发送给followers，然后执行operator函数。通知receiveResponse goroutine函数读取
     followers的response
   3.从responseCh队列读取一个reply，然后写个客户端的socket
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

// 从所有的follower的connect上读取follower的的response
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
  先把pkg加入到列表里面，遍历所有的followerAddrs，然后发送pkg到所有的followers上
*/
func (rp *ReplProtocol) sendRequestToAllfollowers(request *Packet) (index int, err error) {
	rp.pushPacketToList(request)
	for index = 0; index < len(request.followersAddrs); index++ {
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
 从列表里面取出一个pkg,遍历该pkg的followers的connnect，读取response
 如果读取response失败，则该请求的pkg标记为失败的pkg，然后从列表里面删除该pkg
 并丢弃到responseCh，如果所有的follower都成功，则标记该pkg都执行成功了

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
			request.forceDestoryFollowerConnects()
			return
		}
	}
	request.PackOkReply()
	return
}

/*
  从某个follower上读取该request的response:
  1.判断该follower的socket是否为空，为空则失败
  2.判断该request本地是否执行成功，失败则返回错误
  3.从follower的socket上读取response，失败返回错误
  4.判断reply的包和request的包的一致性，如果判断失败则返回错误
  5.如果follower执行失败，则该request也标记为失败，返回错误
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

/*
  写一个reply to 客户端socket
  1.判断reply是否失败，如果失败销毁所有的follower链接
  2.执行后处理函数
  3.判断是否需要给客户端返回response
  4.写给客户端的socket
*/
func (rp *ReplProtocol) writeResponseToClient(reply *Packet) {
	var err error
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToClient, rp.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		reply.forceDestoryFollowerConnects()
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
 分配pkg的follower链接，以key为单位，该key是partitionid,extentid,以及follower的地址作为key
 extent走这个的原因是，确保每个pkg发到datanode的顺序保证顺序一致。
*/
func (rp *ReplProtocol) AllocateFollowersConnects(pkg *Packet, index int) (err error) {
	var conn *net.TCPConn
	if pkg.ExtentType == proto.NormalExtentType {
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

/*如果replProtocol退出，则遍历所有的packetlist执行后处理函数。并且销毁所有的followers链接
*/
func (rp *ReplProtocol) cleanResource() {
	rp.packetListLock.Lock()
	for e := rp.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		request.forceDestoryFollowerConnects()
		rp.postFunc(request)

	}
	replys := len(rp.responseCh)
	for i := 0; i < replys; i++ {
		<-rp.responseCh
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
