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

package datanode

import (
	"container/list"
	"fmt"
	"net"
	"sync"

	"github.com/tiglabs/containerfs/proto"
)

/*
 this struct is used for packet Processor framwroek
 has three goroutine:
    a. readPacketFromClient goroutine recive pkg from client,and check it avali,then send it to requestCh

    b. InteractWithClient goroutine read from requestCh,send it to all replicates,and do local,then send a sigle to handleCh
       and read pkg from replyCh,and write its response to client

    c. receiveReplicatesResponse goroutine read from handleCh,recive all replicates  pkg response,and send this pkg to replyCh

	if any step error,then change request to error Packet,and send it to replyCh, the InteractWithClient can send it to client

*/
type PacketProcessor struct {
	listMux    sync.RWMutex
	packetList *list.List    //store all recived pkg from client
	handleCh   chan struct{} //if sendto all replicates success,then send a sigle to this chan
	//the receiveReplicatesResponse goroutine can recive response from allreplicates
	requestCh           chan *Packet // the recive pkg goroutine recive a avali pkg,then send to this chan
	replyCh             chan *Packet //this chan used to write client
	sourceConn          *net.TCPConn //in connect
	exitC               chan bool
	exited              bool
	exitedMu            sync.RWMutex
	replicatConnects    map[string]*net.TCPConn //all replicates connects
	replicatConnectLock sync.RWMutex
}

func NewPacketProcessor(inConn *net.TCPConn) *PacketProcessor {
	processor := new(PacketProcessor)
	processor.packetList = list.New()
	processor.handleCh = make(chan struct{}, RequestChanSize)
	processor.requestCh = make(chan *Packet, RequestChanSize)
	processor.replyCh = make(chan *Packet, RequestChanSize)
	processor.exitC = make(chan bool, 1)
	processor.sourceConn = inConn
	processor.replicatConnects = make(map[string]*net.TCPConn, 0)

	return processor
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
 allocate replicates connects,if it is extentStore and it is Write op,then use last connects
*/
func (processor *PacketProcessor) AllocateReplicatConnects(pkg *Packet, index int) (err error) {
	var conn *net.TCPConn
	if pkg.StoreMode == proto.NormalExtentMode {
		key := fmt.Sprintf("%v_%v_%v", pkg.PartitionID, pkg.ExtentID, pkg.replicateAddrs[index])
		processor.replicatConnectLock.RLock()
		conn := processor.replicatConnects[key]
		processor.replicatConnectLock.RUnlock()
		if conn == nil {
			conn, err = gConnPool.GetConnect(pkg.replicateAddrs[index])
			if err != nil {
				return
			}
			processor.replicatConnectLock.Lock()
			processor.replicatConnects[key] = conn
			processor.replicatConnectLock.Unlock()
		}
		pkg.replicateConns[index] = conn
	} else {
		conn, err = gConnPool.GetConnect(pkg.replicateAddrs[index])
		if err != nil {
			return
		}
		pkg.replicateConns[index] = conn
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
func (processor *PacketProcessor) CleanResource(s *DataNode) {
	processor.listMux.Lock()
	for e := processor.packetList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		request.forceDestoryAllConnect()
		s.leaderPutTinyExtentToStore(request)

	}
	replys := len(processor.replyCh)
	for i := 0; i < replys; i++ {
		<-processor.replyCh
	}
	processor.packetList = list.New()
	processor.replicatConnectLock.RLock()
	for _, conn := range processor.replicatConnects {
		conn.Close()
	}
	processor.replicatConnectLock.RUnlock()
	processor.replicatConnectLock.Lock()
	processor.replicatConnects = make(map[string]*net.TCPConn, 0)
	processor.replicatConnectLock.Unlock()
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
			request.PackErrorBody(ActionReceiveFromNext, fmt.Sprintf("unknow expect reply"))
			break
		}
		processor.packetList.Remove(e)
		success = true
		processor.replyCh <- reply
	}

	return
}
