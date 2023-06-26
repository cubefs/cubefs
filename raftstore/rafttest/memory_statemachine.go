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
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

var (
	errNotExists 	= errors.New("Key not exists.")
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
	IsRollback       bool
}

type memoryStatemachine struct {
	sync.RWMutex
	id           uint64
	applied      uint64
	raft         *raft.RaftServer
	data         map[string]string
	rollbackData map[string]string
}

func newMemoryStatemachine(id uint64, raft *raft.RaftServer) *memoryStatemachine {
	return &memoryStatemachine{
		id:           id,
		raft:         raft,
		data:         make(map[string]string),
		rollbackData: make(map[string]string),
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
		if kv.IsRollback {
			value, exist := ms.data[kv.Key]
			if !exist || kv.Value != value {
				msg := fmt.Sprintf("rollback key(%v) value(%v) exist(%v) is inconsistent with(%v)", kv.Key, kv.Value, exist, value)
				panic(msg)
				return nil, fmt.Errorf(msg)
			}
			if _, exist = ms.rollbackData[kv.Key]; exist {
				msg := fmt.Sprintf("rollback data key(%v) value(%v) is exist", kv.Key, kv.Value)
				panic(msg)
				return nil, fmt.Errorf(msg)
			}
			ms.rollbackData[kv.Key] = kv.Value
			delete(ms.data, kv.Key)
		} else {
			if kv.LastKey != NoCheckLinear && kv.LastVal != NoCheckLinear {
				if val, exist := ms.data[kv.LastKey]; !exist || val != kv.LastVal {
					msg := fmt.Sprintf("apply err: Key[%v], Val[%v], val[%v], exist[%v]", kv.LastKey, kv.LastVal, val, exist)
					panic(msg)
					return nil, fmt.Errorf(msg)
				}
			}
			value, exist := ms.data[kv.Key]
			if exist {
				msg := fmt.Sprintf("apply err: Key[%v] val[%v] is exist", kv.Key, value)
				//panic(msg)
				return nil, fmt.Errorf(msg)
			}
			ms.data[kv.Key] = kv.Value
		}
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

func (ms *memoryStatemachine) AskRollback(original []byte) (rollback []byte, err error) {
	ms.RLock()
	defer ms.RUnlock()

	switch dataType {
	case 0:
		var kv = &checkKV{}
		if err = json.Unmarshal(original, &kv); err != nil {
			return nil, err
		}
		_, exist := ms.data[kv.Key]
		if exist {
			msg := fmt.Sprintf("rollback key(%v) value(%v) is exist", kv.Key, kv.Value)
			//panic(msg)
			return nil, fmt.Errorf(msg)
		}
		kv.IsRollback = true
		if rollback, err = json.Marshal(kv); err != nil {
			return nil, err
		}
	default:
		rollback = original
	}

	return rollback, nil
}

func (ms *memoryStatemachine) Snapshot(recoveryID uint64) (proto.Snapshot, error) {
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

func (ms *memoryStatemachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator, snapV uint32) error {
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

func (ms *memoryStatemachine) GetRollback(key string) (string, error) {
	ms.RLock()
	defer ms.RUnlock()

	if v, ok := ms.rollbackData[key]; ok {
		return v, nil
	} else {
		return "", errNotExists
	}
}

func (ms *memoryStatemachine) GetLen() (dataLen, rollbackLen int) {
	ms.RLock()
	defer ms.RUnlock()

	return len(ms.data), len(ms.rollbackData)
}

func (ms *memoryStatemachine) Put(key, value, lastKey, lastVal string) error {
	kv := &checkKV{Key: key, Value: value, LastKey: lastKey, LastVal: lastVal}
	if data, err := json.Marshal(kv); err != nil {
		return err
	} else {
		resp := ms.raft.Submit(nil, ms.id, data)
		_, err = resp.Response()
		if err != nil {
			return errors.New(fmt.Sprintf("Put error[%v].", err))
		}
		return nil
	}
}

func (ms *memoryStatemachine) PutAsync(key, value, lastKey, lastVal string) (future *raft.Future, err error) {
	kv := &checkKV{Key: key, Value: value, LastKey: lastKey, LastVal: lastVal}
	if data, err := json.Marshal(kv); err != nil {
		return nil, err
	} else {
		resp := ms.raft.Submit(nil, ms.id, data)
		return resp, nil
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
		return errors.New(fmt.Sprintf("Put error[%v].", err))
	}
	return nil
}

func (ms *memoryStatemachine) localConstructBigData(bitSize int, exeMin int, res *resultTest) {
	var sucOp, failOp int64
	bArray := make([]byte, bitSize)
	for i := 0; i < bitSize; i++ {
		bArray[i] = 1
	}
	delay := time.Duration(exeMin) * time.Minute
	//sec := exeMin * 60
	timer := time.NewTimer(delay)
	//start = time.Now()
	defer func() {
		timer.Stop()
		//result = fmt.Sprintf("local put bigsubmit: start-%v, end-%v; size-%d, executeTime-%dmin, success-%d, fail-%d, tps-%d",
		//	start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), bitSize, exeMin, sucOp, failOp, sucOp/sec)
		res.totalCount = sucOp + failOp
		res.sucOp = sucOp
		res.failOp = failOp
		//res.err = err
	}()
	for {
		select {
		case <-timer.C:
			//end = time.Now()
			return
		default:
		}

		resp := ms.raft.Submit(nil, ms.id, bArray)
		_, err := resp.Response()
		if err != nil {
			//return errors.New(fmt.Sprintf("Put error[%v].\r\n", err))
			failOp++
		} else {
			sucOp++
		}
	}

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

func (s *memorySnapshot) Version() uint32 {
	return 0
}
