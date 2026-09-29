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
	q := NewQueue(200 * time.Millisecond)
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
	time.Sleep(time.Millisecond * 250)
	id, msg, exist = q.Pop()
	require.Equal(t, msgID, id)
	require.Equal(t, msgString, msg)
	require.Equal(t, true, exist)

	// test update
	err = q.Update(msgID, msgString)
	require.NoError(t, err)

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

func (t *mockWorkerTask) GetSources() []proto.VunitLocation {
	return t.src
}

func (t *mockWorkerTask) GetDestination() proto.VunitLocation {
	return t.dst
}

func (t *mockWorkerTask) SetDestination(dstVuid proto.VunitLocation) {
	t.dst = dstVuid
}

func (t *mockWorkerTask) ToTask() (*proto.Task, error) {
	return nil, nil
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
	task, ok := wt.(WorkerTask)
	require.True(t, ok)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, task1.GetSources(), task.GetSources())
	require.Equal(t, task1.GetDestination(), task.GetDestination())
	_, _, exist = q.PopTask()
	require.Equal(t, false, exist)

	// test RetryTask
	q.RetryTask(taskID1)
	time.Sleep(100 * time.Millisecond)
	id, wt, exist = q.PopTask()
	task, ok = wt.(WorkerTask)
	require.True(t, ok)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, vunits([]proto.Vuid{1, 2, 3}), task.GetSources())
	require.Equal(t, vunit(4), task.GetDestination())

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

// mockShardTask implements ShardTask for testing.
type mockShardTask struct {
	source      proto.ShardUnitInfoSimple
	leader      proto.ShardUnitInfoSimple
	destination proto.ShardUnitInfoSimple
	badDest     proto.ShardUnitInfoSimple
}

func (t *mockShardTask) GetSource() proto.ShardUnitInfoSimple      { return t.source }
func (t *mockShardTask) GetLeader() proto.ShardUnitInfoSimple      { return t.leader }
func (t *mockShardTask) GetDestination() proto.ShardUnitInfoSimple { return t.destination }
func (t *mockShardTask) SetDestination(d proto.ShardUnitInfoSimple) {
	t.destination = d
}
func (t *mockShardTask) SetLeader(l proto.ShardUnitInfoSimple)        { t.leader = l }
func (t *mockShardTask) GetBadDestination() proto.ShardUnitInfoSimple { return t.badDest }
func (t *mockShardTask) ToTask() (*proto.Task, error)                 { return nil, nil }

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
	require.Equal(t, wt.GetSources(), task1.GetSources())
	require.Equal(t, wt.GetDestination(), task1.GetDestination())

	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(renewDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSources(), task1.GetSources())
	require.Equal(t, wt.GetDestination(), task1.GetDestination())

	// test Cancel
	err := wq.Cancel(idc, taskID1, task1.GetSources(), task1.GetDestination())
	require.NoError(t, err)
	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(cancelPunishDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSources(), task1.GetSources())
	require.Equal(t, wt.GetDestination(), task1.GetDestination())

	// test Reclaim
	err = wq.Reclaim(idc, taskID1, task1.GetSources(), task1.GetDestination(), vunit(6), 0)
	require.NoError(t, err)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSources(), vunits([]proto.Vuid{1, 2, 3}))
	require.Equal(t, wt.GetDestination(), vunit(6))

	// test Renewal
	err = wq.Renewal(idc, taskID1)
	require.NoError(t, err)
	_, _, exist = wq.Acquire(idc)
	require.Equal(t, false, exist)
	time.Sleep(renewDuration)
	id, wt, exist = wq.Acquire(idc)
	require.Equal(t, true, exist)
	require.Equal(t, id, taskID1)
	require.Equal(t, wt.GetSources(), vunits([]proto.Vuid{1, 2, 3}))
	require.Equal(t, wt.GetDestination(), vunit(6))
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

func TestWorkerTaskQueueMissingPaths(t *testing.T) {
	idc := "z0"
	noSuchIdc := "z99"
	cancelPunishDuration := 100 * time.Millisecond
	renewDuration := 200 * time.Millisecond
	taskID := "task_miss_1"
	task := mockWorkerTask{src: vunits([]proto.Vuid{1, 2, 3}), dst: vunit(4)}

	wq := newTestWorkerTaskQueue(cancelPunishDuration, renewDuration)
	wq.AddPreparedTask(idc, taskID, &task)

	// Acquire task to put it into doing state
	_, _, exist := wq.Acquire(idc)
	require.True(t, exist)

	// Update: success
	updatedTask := mockWorkerTask{src: vunits([]proto.Vuid{1, 2, 3}), dst: vunit(99)}
	err := wq.Update(idc, taskID, &updatedTask)
	require.NoError(t, err)

	// Update: no such IDC
	err = wq.Update(noSuchIdc, taskID, &updatedTask)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Query: success
	wt, err := wq.Query(idc, taskID)
	require.NoError(t, err)
	require.Equal(t, vunit(99), wt.GetDestination())

	// Query: no such IDC
	_, err = wq.Query(noSuchIdc, taskID)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// SetLeaseExpiredS
	wq.SetLeaseExpiredS(500 * time.Millisecond)
	require.Equal(t, 500*time.Millisecond, wq.leaseExpiredS)

	// Cancel: no such IDC
	err = wq.Cancel(noSuchIdc, taskID, task.GetSources(), task.GetDestination())
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Reclaim: no such IDC
	err = wq.Reclaim(noSuchIdc, taskID, task.GetSources(), task.GetDestination(), vunit(5), 0)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Renewal: no such IDC
	err = wq.Renewal(noSuchIdc, taskID)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Complete: no such IDC
	_, err = wq.Complete(noSuchIdc, taskID, task.GetSources(), task.GetDestination())
	require.EqualError(t, err, errNoSuchIDCQueue.Error())
}

func TestQueueUpdateNotDoing(t *testing.T) {
	q := NewQueue(200 * time.Millisecond)
	msgID := "msg_todo"
	require.NoError(t, q.Push(msgID, "data"))

	// Update on a task in todo state (not doing) should fail
	err := q.Update(msgID, "new_data")
	require.EqualError(t, err, errTaskStateNotDoing.Error())
}

func TestShardTaskQueue(t *testing.T) {
	idc := "z0"
	noSuchIdc := "z99"
	cancelPunish := 100 * time.Millisecond

	src := proto.ShardUnitInfoSimple{Suid: proto.EncodeSuid(1, 0, 1), DiskID: 1}
	dst := proto.ShardUnitInfoSimple{Suid: proto.EncodeSuid(1, 1, 1), DiskID: 2}
	newDst := proto.ShardUnitInfoSimple{Suid: proto.EncodeSuid(1, 1, 2), DiskID: 3}
	leader := proto.ShardUnitInfoSimple{Suid: proto.EncodeSuid(1, 2, 1), DiskID: 4}
	taskID := "shard_task_1"

	task := &mockShardTask{source: src, destination: dst, leader: leader}

	q := NewShardTaskQueue(cancelPunish)

	// AddPreparedTask: first add creates idc queue
	q.AddPreparedTask(idc, taskID, task)

	// AddPreparedTask: panic on duplicate
	require.Panics(t, func() {
		q.AddPreparedTask(idc, taskID, task)
	})

	// Acquire: no such IDC returns false
	_, _, exist := q.Acquire(noSuchIdc)
	require.False(t, exist)

	// Acquire: success
	id, wt, exist := q.Acquire(idc)
	require.True(t, exist)
	require.Equal(t, taskID, id)
	require.Equal(t, src, wt.GetSource())
	require.Equal(t, dst, wt.GetDestination())

	// Acquire: queue empty (task is in doing)
	_, _, exist = q.Acquire(idc)
	require.False(t, exist)

	// StatsTasks
	todo, doing := q.StatsTasks()
	require.Equal(t, 0, todo)
	require.Equal(t, 1, doing)

	// Cancel: no such IDC
	err := q.Cancel(noSuchIdc, taskID, src, dst)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Cancel: unmatched suid
	err = q.Cancel(idc, taskID, src, proto.ShardUnitInfoSimple{DiskID: 99})
	require.EqualError(t, err, ErrUnmatchedSuid.Error())

	// Cancel: success
	err = q.Cancel(idc, taskID, src, dst)
	require.NoError(t, err)

	// wait cancel punish then re-acquire
	time.Sleep(cancelPunish + 10*time.Millisecond)
	_, _, exist = q.Acquire(idc)
	require.True(t, exist)

	// Reclaim: no such IDC
	err = q.Reclaim(noSuchIdc, taskID, src, dst, newDst, newDst.DiskID)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Reclaim: unmatched suid
	err = q.Reclaim(idc, taskID, src, proto.ShardUnitInfoSimple{DiskID: 99}, newDst, newDst.DiskID)
	require.EqualError(t, err, ErrUnmatchedSuid.Error())

	// Reclaim: success (updates destination)
	err = q.Reclaim(idc, taskID, src, dst, newDst, newDst.DiskID)
	require.NoError(t, err)

	// re-acquire after reclaim (requeued immediately)
	_, wt, exist = q.Acquire(idc)
	require.True(t, exist)
	require.Equal(t, newDst, wt.GetDestination())

	// Update: no such IDC
	_, err = q.Update(noSuchIdc, taskID, leader)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Update: success (updates leader)
	newLeader := proto.ShardUnitInfoSimple{Suid: proto.EncodeSuid(1, 2, 2), DiskID: 5}
	wt2, err := q.Update(idc, taskID, newLeader)
	require.NoError(t, err)
	require.Equal(t, newLeader, wt2.GetLeader())

	// Query: no such IDC
	_, err = q.Query(noSuchIdc, taskID)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Query: success
	wt3, err := q.Query(idc, taskID)
	require.NoError(t, err)
	require.Equal(t, newDst, wt3.GetDestination())

	// Renewal: no such IDC
	err = q.Renewal(noSuchIdc, taskID)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Renewal: success
	err = q.Renewal(idc, taskID)
	require.NoError(t, err)

	// Complete: no such IDC
	_, err = q.Complete(noSuchIdc, taskID, src, newDst)
	require.EqualError(t, err, errNoSuchIDCQueue.Error())

	// Complete: unmatched suid
	_, err = q.Complete(idc, taskID, src, proto.ShardUnitInfoSimple{DiskID: 99})
	require.EqualError(t, err, ErrUnmatchedSuid.Error())

	// Complete: success
	completed, err := q.Complete(idc, taskID, src, newDst)
	require.NoError(t, err)
	require.Equal(t, newDst, completed.GetDestination())

	// StatsTasks after complete: empty
	todo, doing = q.StatsTasks()
	require.Equal(t, 0, todo)
	require.Equal(t, 0, doing)
}
