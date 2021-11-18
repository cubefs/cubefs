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
	"time"
)

const (
	MsgPoolCnt=64
)

var (
	msgPool [MsgPoolCnt]*sync.Pool
	bytePool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 128)
		},
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index:=0;index<MsgPoolCnt;index++{
		msgPool[index]=&sync.Pool{
			New: func() interface{} {
				return &Message{
					Entries: make([]*Entry, 0, 128),
				}
			},
		}
	}
}

func GetMessage() *Message {
	index:=rand.Uint64()%MsgPoolCnt
	msg := msgPool[index].Get().(*Message)
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
	return bytePool.Get().([]byte)
}

func returnByteSlice(b []byte) {
	bytePool.Put(b)
}
