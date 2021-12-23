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

package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

var (
	errNotExists = errors.New("Key not exists.")
)

const NoCheckLinear = "none"

type subTime struct {
	maxSubTime   int64
	minSubTime   int64
	totalSubTime uint64
	subCount     uint64
}

type checkKV struct {
	Key, Value       string
	LastKey, LastVal string
}

type memoryStatemachine struct {
	sync.RWMutex
	id      uint64
	applied uint64
	raft    *raft.RaftServer
	data    map[string]string
}

func newMemoryStatemachine(id uint64, raft *raft.RaftServer) *memoryStatemachine {
	return &memoryStatemachine{
		id:   id,
		raft: raft,
		data: make(map[string]string),
	}
}

func (ms *memoryStatemachine) Apply(data []byte, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	switch dataType {
	case 0:
		var kv = &checkKV{}
		if err := json.Unmarshal(data, &kv); err != nil {
			return nil, err
		}
		if kv.LastKey != NoCheckLinear && kv.LastVal != NoCheckLinear {
			if val, exist := ms.data[kv.LastKey]; !exist || val != kv.LastVal {
				return nil, fmt.Errorf("apply err: Key[%v], val[%v], err[%v]", kv.LastKey, kv.LastVal, errNotExists.Error())
			}
		}
		ms.data[kv.Key] = kv.Value
	}

	ms.applied = index
	return nil, nil
}

func (ms *memoryStatemachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	return nil, nil
}

func (ms *memoryStatemachine) Snapshot() (proto.Snapshot, error) {
	ms.RLock()
	defer ms.RUnlock()

	if data, err := json.Marshal(ms.data); err != nil {
		return nil, err
	} else {
		data = append(make([]byte, 8), data...)
		binary.BigEndian.PutUint64(data, ms.applied)
		return &memorySnapshot{
			applied: ms.applied,
			data:    data,
		}, nil
	}
}

func (ms *memoryStatemachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	ms.Lock()
	defer ms.Unlock()

	var (
		data  []byte
		block []byte
		err   error
	)
	for err == nil {
		if block, err = iter.Next(); len(block) > 0 {
			data = append(data, block...)
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	ms.applied = binary.BigEndian.Uint64(data)
	if err = json.Unmarshal(data[8:], &ms.data); err != nil {
		return err
	}
	return nil
}

func (ms *memoryStatemachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (ms *memoryStatemachine) Get(key string) (string, error) {
	ms.RLock()
	defer ms.RUnlock()

	if v, ok := ms.data[key]; ok {
		return v, nil
	} else {
		return "", errNotExists
	}
}

func (ms *memoryStatemachine) Put(key, value, lastKey, lastVal string) error {
	kv := &checkKV{Key: key, Value: value, LastKey: lastKey, LastVal: lastVal}
	if data, err := json.Marshal(kv); err != nil {
		return err
	} else {
		//startTime := time.Now().UnixNano() / 1e3
		resp := ms.raft.Submit(nil, ms.id, data)
		_, err = resp.Response()
		//endTime := time.Now().UnixNano() / 1e3
		//computeTime(ms.id, startTime, endTime)
		if err != nil {
			return errors.New(fmt.Sprintf("Put error[%v].\r\n", err))
		}
		return nil
	}
}

func (ms *memoryStatemachine) constructBigData(bitSize int) error {
	bArray := make([]byte, bitSize)
	for i := 0; i < bitSize; i++ {
		bArray[i] = 1
	}
	resp := ms.raft.Submit(nil, ms.id, bArray)
	_, err := resp.Response()
	if err != nil {
		return errors.New(fmt.Sprintf("Put error[%v].\r\n", err))
	}
	return nil
}

func computeTime(id uint64, startTime int64, endTime int64) {
	callTime := endTime - startTime
	if callTime < subTimeMap[id].minSubTime {
		subTimeMap[id].minSubTime = callTime
	}
	if callTime > subTimeMap[id].maxSubTime {
		subTimeMap[id].maxSubTime = callTime
	}
	subTimeMap[id].totalSubTime += uint64(callTime)
	subTimeMap[id].subCount++
}

func (ms *memoryStatemachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(ms.id, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error.")
	}
	return nil
}

func (ms *memoryStatemachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(ms.id, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error.")
	}
	return nil
}

func (ms *memoryStatemachine) setApplied(index uint64) {
	ms.Lock()
	defer ms.Unlock()
	ms.applied = index
}

func (ms *memoryStatemachine) HandleLeaderChange(leader uint64) {}

type memorySnapshot struct {
	offset  int
	applied uint64
	data    []byte
}

func (s *memorySnapshot) Next() ([]byte, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	s.offset = len(s.data)
	return s.data, nil
}

func (s *memorySnapshot) ApplyIndex() uint64 {
	return s.applied
}

func (s *memorySnapshot) Close() {
	return
}
