// Copyright 2022 The CubeFS Authors.
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

package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestQueue(t *testing.T) {
	q := NewQueue(500 * time.Millisecond)
	msgID := "msg_1"
	msgString := "test_msg"

	// test Push
	err := q.Push(msgID, msgString)
	require.NoError(t, err)
	err = q.Push(msgID, msgString)
	require.EqualError(t, err, errExistingMessageID.Error())

	msg, err := q.Get(msgID)
	require.NoError(t, err)
	require.Equal(t, msgString, msg.(string))

	// test Pop
	id, msg, exist := q.Pop()
	require.Equal(t, msgID, id)
	require.Equal(t, msgString, msg)
	require.Equal(t, true, exist)
	_, _, exist = q.Pop()
	require.Equal(t, false, exist)
	time.Sleep(time.Second)
	id, msg, exist = q.Pop()
	require.Equal(t, msgID, id)
	require.Equal(t, msgString, msg)
	require.Equal(t, true, exist)

	// test Requeue
	err = q.Requeue(msgID, 0)
	require.NoError(t, err)
	id, msg, exist = q.Pop()
	require.Equal(t, msgID, id)
	require.Equal(t, msgString, msg)
	require.Equal(t, true, exist)

	err = q.Requeue(msgID, 100*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	id, msg, exist = q.Pop()
	require.Equal(t, msgID, id)
	require.Equal(t, msgString, msg)
	require.Equal(t, true, exist)
	// test q.Stats()
	msgID2 := "msg_2"
	msgString2 := "test_msg2"
	err = q.Push(msgID2, msgString2)
	require.NoError(t, err)
	todo, doing := q.Stats()
	require.Equal(t, 1, todo)
	require.Equal(t, 1, doing)

	// test Remove
	err = q.Remove(msgID)
	require.NoError(t, err)

	// test no such msg id
	noSuchMsgID := "NoSuchId"
	_, err = q.Get(noSuchMsgID)
	require.EqualError(t, err, ErrNoSuchMessageID.Error())

	err = q.Requeue(noSuchMsgID, 0)
	require.EqualError(t, err, ErrNoSuchMessageID.Error())

	err = q.Remove(noSuchMsgID)
	require.EqualError(t, err, ErrNoSuchMessageID.Error())
}

type mockWorkerTask struct {
	src []proto.VunitLocation
	dst proto.VunitLocation
}

func vunits(vuids []proto.Vuid) []proto.VunitLocation {
	ret := []proto.VunitLocation{}
	for _, vuid := range vuids {
		ret = append(ret, proto.VunitLocation{Vuid: vuid, Host: "127.0.0.1:xx"})
	}
	return ret
}

func vunit(vuid proto.Vuid) proto.VunitLocation {
	return proto.VunitLocation{Vuid: vuid, Host: "127.0.0.1:xx"}
}

func (t *mockWorkerTask) GetSrc() []proto.VunitLocation {
	return t.src
}

func (t *mockWorkerTask) GetDest() proto.VunitLocation {
	return t.dst
}

func (t *mockWorkerTask) SetDest(dstVuid proto.VunitLocation) {
	t.dst = dstVuid
}

func TestTaskQueue(t *testing.T) {
	// test Push
	taskID1 := "task_id1"
	task1 := mockWorkerTask{src: vunits([]proto.Vuid{1, 2, 3}), dst: vunit(4)}

	q := NewTaskQueue(100 * time.Millisecond)
	q.PushTask(taskID1, &task1)

	_, ok := q.Query(taskID1)
	require.Equal(t, true, ok)

	// test PopTask
	id, wt, exist := q.PopTask()
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, task1.GetSrc(), wt.GetSrc())
	require.Equal(t, task1.GetDest(), wt.GetDest())
	_, _, exist = q.PopTask()
	require.Equal(t, false, exist)

	// test RetryTask
	q.RetryTask(taskID1)
	time.Sleep(100 * time.Millisecond)
	id, wt, exist = q.PopTask()
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, vunits([]proto.Vuid{1, 2, 3}), wt.GetSrc())
	require.Equal(t, vunit(4), wt.GetDest())

	// test Stats
	taskID2 := "task_id2"
	task2 := mockWorkerTask{src: vunits([]proto.Vuid{3, 4, 5}), dst: vunit(6)}
	q.PushTask(taskID2, &task2)
	todo, doing := q.StatsTasks()
	require.Equal(t, 1, todo)
	require.Equal(t, 1, doing)

	// test Remove
	err := q.RemoveTask(taskID1)
	require.NoError(t, err)
	err = q.RemoveTask(taskID2)
	require.NoError(t, err)
	todo, doing = q.StatsTasks()
	require.Equal(t, 0, todo)
	require.Equal(t, 0, doing)

	// test no such msg id
	noSuchTaskID := "NoSuchId"
	err = q.RemoveTask(noSuchTaskID)
	require.EqualError(t, err, ErrNoSuchMessageID.Error())
}

func newTestWorkerTaskQueue(cancelPunishDuration, renewDuration time.Duration) *WorkerTaskQueue {
	return &WorkerTaskQueue{
		idcQueues:            make(map[string]*Queue),
		cancelPunishDuration: cancelPunishDuration,
		leaseExpiredS:        renewDuration,
	}
}

func TestWorkerTaskQueue(t *testing.T) {
	taskID1 := "task_id1"
	idc := "z0"
	task1 := mockWorkerTask{src: vunits([]proto.Vuid{1, 2, 3}), dst: vunit(4)}

	cancelPunishDuration := 100 * time.Millisecond
	renewDuration := 200 * time.Millisecond

	// test AddPreparedTask
	wq := newTestWorkerTaskQueue(cancelPunishDuration, renewDuration)
	wq.AddPreparedTask(idc, taskID1, &task1)

	// test acquire
	id, wt, exist := wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSrc(), task1.GetSrc())
	require.Equal(t, wt.GetDest(), task1.GetDest())

	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(renewDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSrc(), task1.GetSrc())
	require.Equal(t, wt.GetDest(), task1.GetDest())

	// test Cancel
	err := wq.Cancel(idc, taskID1, task1.GetSrc(), task1.GetDest())
	require.NoError(t, err)
	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(cancelPunishDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSrc(), task1.GetSrc())
	require.Equal(t, wt.GetDest(), task1.GetDest())

	// test Reclaim
	err = wq.Reclaim(idc, taskID1, task1.GetSrc(), task1.GetDest(), vunit(6), 0)
	require.NoError(t, err)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSrc(), vunits([]proto.Vuid{1, 2, 3}))
	require.Equal(t, wt.GetDest(), vunit(6))

	// test Renewal
	err = wq.Renewal(idc, taskID1)
	require.NoError(t, err)
	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(renewDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSrc(), vunits([]proto.Vuid{1, 2, 3}))
	require.Equal(t, wt.GetDest(), vunit(6))
	// test Complete
	_, err = wq.Complete(idc, taskID1, vunits([]proto.Vuid{1, 2, 3}), vunit(6))
	require.NoError(t, err)
	todo, doing := wq.StatsTasks()
	require.Equal(t, 0, todo)
	require.Equal(t, 0, doing)

	// test ErrUnmatchedVuids
	taskID2 := "task_id2"
	task2 := mockWorkerTask{src: vunits([]proto.Vuid{1, 2, 3}), dst: vunit(4)}
	wq = NewWorkerTaskQueue(cancelPunishDuration)
	wq.AddPreparedTask(idc, taskID2, &task2)

	err = wq.Cancel(idc, taskID2, vunits([]proto.Vuid{4, 5, 6}), vunit(4))
	require.EqualError(t, err, ErrUnmatchedVuids.Error())
	err = wq.Reclaim(idc, taskID2, vunits([]proto.Vuid{4, 5, 6}), vunit(4), vunit(5), 0)
	require.EqualError(t, err, ErrUnmatchedVuids.Error())
	_, err = wq.Complete(idc, taskID2, vunits([]proto.Vuid{4, 5, 6}), vunit(4))
	require.EqualError(t, err, ErrUnmatchedVuids.Error())
}
