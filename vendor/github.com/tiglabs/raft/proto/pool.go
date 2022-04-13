// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proto

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MsgPoolCnt=64
)

var (
	msgPool [MsgPoolCnt]*MessagePoolCap
	bytePool [MsgPoolCnt]*sync.Pool
	entryPool [MsgPoolCnt]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index:=0;index<MsgPoolCnt;index++{
		entryPool[index]=&sync.Pool{
			New: func() interface{}{
				return &Entry{

				}
			},
		}
		bytePool[index]= &sync.Pool{
			New: func() interface{} {
				return make([]byte, 128)
			},
		}

		msgPool[index]=&MessagePoolCap{
		        c:    make(chan *Message, 512),
		        ecap: 128,
	        }
	}
}


func GetEntryFromPool() (e *Entry) {
	index:=rand.Intn(MsgPoolCnt)
	e = entryPool[index].Get().(*Entry)
	return e
}

func GetEntryFromPoolWithFollower() (e *Entry) {
	e=GetEntryFromPool()
	e.OrgRefCnt=FollowerLogEntryRefCnt
	atomic.StoreInt32(&e.RefCnt,FollowerLogEntryRefCnt)
	atomic.AddUint64(&FollowerGetEntryCnt,1)
	return e
}

func GetEntryFromPoolWithArgWithLeader(t EntryType,term,index uint64,data []byte) (e *Entry) {
	e=GetEntryFromPool()
	e.Type=t
	e.Data=data
	e.Term=term
	e.Index=index
	e.ctx=nil
	e.OrgRefCnt=LeaderLogEntryRefCnt
	atomic.StoreInt32(&e.RefCnt,LeaderLogEntryRefCnt)
	atomic.AddUint64(&LeaderGetEntryCnt,1)

	return
}

func PutEntryToPool(e *Entry) {
	if e.IsLeaderLogEntry() || e.IsFollowerLogEntry() {
		e.DecRefCnt()
		if atomic.LoadInt32(&e.RefCnt) == 0 {
			rindex := rand.Intn(MsgPoolCnt)
			e.Data = nil
			e.Type = 0
			e.Term = 0
			if e.IsLeaderLogEntry(){
				atomic.AddUint64(&LeaderPutEntryCnt, 1)
			}else {
				atomic.AddUint64(&FollowerPutEntryCnt, 1)
			}
			e.Index = 0
			e.OrgRefCnt = 0
			e.RefCnt = 0
			entryPool[rindex].Put(e)
		}
	}

}

func GetMessage() *Message {
	index:=rand.Intn(MsgPoolCnt)
	//msg := msgPool[index].Get().(*Message)
	msg := msgPool[index].Get()
	msg.Reject = false
	msg.RejectIndex = 0
	msg.ID = 0
	msg.From = 0
	msg.To = 0
	msg.Term = 0
	msg.LogTerm = 0
	msg.Index = 0
	msg.Commit = 0
	msg.SnapshotMeta.Index = 0
	msg.SnapshotMeta.Term = 0
	msg.SnapshotMeta.Peers = nil
	msg.SnapshotMeta.Learners = nil
	msg.Snapshot = nil
	msg.Context = nil
	msg.Entries = msg.Entries[0:0]
	msg.ctx = nil
	msg.magic=uint8(index)
	return msg
}

func ReturnMessage(msg *Message) {
	if msg != nil {
		msgPool[msg.magic].Put(msg)
	}
}

func getByteSlice() []byte {
	return bytePool[rand.Intn(MsgPoolCnt)].Get().([]byte)
}

func returnByteSlice(b []byte) {
	bytePool[rand.Intn(MsgPoolCnt)].Put(b)
}


type BytePool struct {
	c    chan []byte
	buffersize    int
}

func NewBytePool(maxSize int, buffersize int) (bp *BytePool) {
	return &BytePool{
		c:    make(chan []byte, maxSize),
		buffersize: buffersize,
	}
}

func (bp *BytePool) Get() (b []byte) {
	select {
	case b = <-bp.c:
	default:
	     b = make([]byte, bp.buffersize)
	}
	return
}

func (bp *BytePool) Put(b []byte) {
	select {
	case bp.c <- b:
	default:
	}
}



type MessagePoolCap struct {
	c    chan *Message
	ecap int
}

func NewMessagePoolCap(maxSize int, entryCap int) (msgpool *MessagePoolCap) {
	return &MessagePoolCap{
		c:    make(chan *Message, maxSize),
		ecap: entryCap,
	}
}

func (msgpool *MessagePoolCap) Get() (msg *Message) {
	select {
	case msg = <-msgpool.c:
	default:
		//msg = &Message{Entries: make([]*Entry, 0, msgpool.ecap)}
		msg = new(Message)
		msg.Entries = make([]*Entry, 0, msgpool.ecap)
	}
	return
}

func (msgpool *MessagePoolCap) Put(msg *Message) {
	select {
	case msgpool.c <- msg:

	default:

	}
}



