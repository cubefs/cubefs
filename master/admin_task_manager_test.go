// Copyright 2025 The CubeFS Authors.
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

package master

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

// newTestManager creates an AdminTaskManager without starting the background goroutine.
// This allows tests to call internal methods directly and precisely without timing dependencies.
func newTestManager(addr string) *AdminTaskManager {
	proto.InitBufferPool(int64(32768))
	return &AdminTaskManager{
		targetAddr: addr,
		clusterID:  "unit-test-cluster",
		TaskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}, 1),
		connPool:   util.NewConnectPoolWithTimeout(idleConnTimeout, connectTimeout, false),
	}
}

// newTaskWithSendCount builds a task and sets its SendCount so that
// CheckTaskTimeOut() returns true when SendCount >= proto.MaxSendCount.
func newTaskWithSendCount(opCode uint8, addr string, sendCount uint8) *proto.AdminTask {
	t := proto.NewAdminTask(opCode, addr, nil)
	t.SendCount = sendCount
	return t
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_NewAdminTaskManager validates the public constructor.
// -------------------------------------------------------------------------

func TestAdminTaskManager_NewAdminTaskManager(t *testing.T) {
	// newAdminTaskManager starts a goroutine; stop it immediately via exitCh.
	mgr := newAdminTaskManager("127.0.0.1:19999", "testCluster")
	require.NotNil(t, mgr)
	require.Equal(t, "127.0.0.1:19999", mgr.targetAddr)
	require.Equal(t, "testCluster", mgr.clusterID)
	require.NotNil(t, mgr.TaskMap)

	// Signal the background goroutine to exit and give it a moment to stop.
	mgr.exitCh <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_AddTask_Idempotent
// AddTask must be idempotent: adding the same task a second time is a no-op.
// -------------------------------------------------------------------------

func TestAdminTaskManager_AddTask_Idempotent(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task1 := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr-1", nil)
	task2 := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr-2", nil)

	mgr.AddTask(task1)
	require.Len(t, mgr.TaskMap, 1)

	// Adding task1 a second time should not create a duplicate entry.
	mgr.AddTask(task1)
	require.Len(t, mgr.TaskMap, 1)

	// Adding a distinct task increases the count.
	mgr.AddTask(task2)
	require.Len(t, mgr.TaskMap, 2)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_DelTask_NotFound
// DelTask on a missing key must be a silent no-op.
// -------------------------------------------------------------------------

func TestAdminTaskManager_DelTask_NotFound(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	ghost := proto.NewAdminTask(proto.OpDeleteDataPartition, "ghost", nil)
	// Should not panic even when the task is not in the map.
	require.NotPanics(t, func() {
		mgr.DelTask(ghost)
	})
	require.Empty(t, mgr.TaskMap)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_DelTask_HeartbeatAndNonHeartbeat
// Covers both branches of the opcode check inside DelTask.
// -------------------------------------------------------------------------

func TestAdminTaskManager_DelTask_HeartbeatAndNonHeartbeat(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	// Non-heartbeat task: the debug-log branch (opcode not in heartbeat set) is taken.
	nonHB := proto.NewAdminTask(proto.OpDeleteDataPartition, "n1", nil)
	mgr.AddTask(nonHB)
	require.Len(t, mgr.TaskMap, 1)
	mgr.DelTask(nonHB)
	require.Empty(t, mgr.TaskMap)

	// Heartbeat tasks: the log branch is skipped.
	for _, op := range []uint8{
		proto.OpDataNodeHeartbeat,
		proto.OpMetaNodeHeartbeat,
		proto.OpLcNodeHeartbeat,
		proto.OpFlashNodeHeartbeat,
	} {
		hbTask := proto.NewAdminTask(op, "hb-addr", nil)
		mgr.AddTask(hbTask)
		require.Len(t, mgr.TaskMap, 1)
		mgr.DelTask(hbTask)
		require.Empty(t, mgr.TaskMap)
	}
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToBeDeletedTasks_SendCountTimeout
// A task whose SendCount reaches MaxSendCount is considered timed out.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToBeDeletedTasks_SendCountTimeout(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	// Task that has been sent MaxSendCount times without response.
	timedOut := newTaskWithSendCount(proto.OpDeleteDataPartition, "a1", proto.MaxSendCount)
	// Task that still has remaining retries.
	fresh := newTaskWithSendCount(proto.OpDeleteDataPartition, "a2", 0)

	mgr.AddTask(timedOut)
	mgr.AddTask(fresh)

	del := mgr.getToBeDeletedTasks()
	require.Len(t, del, 1)
	require.Equal(t, timedOut.ID, del[0].ID)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToBeDeletedTasks_SendTimeTimeout
// A task whose SendTime is old enough triggers the timeout path.
// The SendTime warning branch (SendTime > 0) is also covered here.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToBeDeletedTasks_SendTimeTimeout(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	// Simulate a task that was sent more than ResponseTimeOut seconds ago.
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "old", nil)
	task.SendTime = time.Now().Unix() - int64(proto.ResponseTimeOut) - 10 // definitely expired
	task.SendCount = 1                                                    // > 0 so warn is triggered

	mgr.AddTask(task)

	del := mgr.getToBeDeletedTasks()
	require.Len(t, del, 1)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToBeDeletedTasks_NotExpired
// Tasks that are still within the response window must not be collected.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToBeDeletedTasks_NotExpired(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	fresh := proto.NewAdminTask(proto.OpDeleteDataPartition, "fresh", nil)
	// SendCount = 0 and SendTime = 0  →  CheckTaskTimeOut returns false.
	mgr.AddTask(fresh)

	del := mgr.getToBeDeletedTasks()
	require.Empty(t, del)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_doDeleteTasks
// doDeleteTasks must remove all timed-out tasks from the map.
// -------------------------------------------------------------------------

func TestAdminTaskManager_doDeleteTasks(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	expired1 := newTaskWithSendCount(proto.OpDeleteDataPartition, "e1", proto.MaxSendCount)
	expired2 := newTaskWithSendCount(proto.OpDeleteMetaPartition, "e2", proto.MaxSendCount)
	alive := newTaskWithSendCount(proto.OpDeleteDataPartition, "alive", 0)

	mgr.AddTask(expired1)
	mgr.AddTask(expired2)
	mgr.AddTask(alive)

	require.Len(t, mgr.TaskMap, 3)
	mgr.doDeleteTasks()
	// Only the alive task should remain.
	require.Len(t, mgr.TaskMap, 1)
	_, ok := mgr.TaskMap[alive.ID]
	require.True(t, ok)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_HeartbeatFirst
// Heartbeat tasks must appear before urgent and normal tasks.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_HeartbeatFirst(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	hb := proto.NewAdminTask(proto.OpDataNodeHeartbeat, "hb", nil)
	normal := proto.NewAdminTask(proto.OpDeleteDataPartition, "normal", nil)

	mgr.AddTask(hb)
	mgr.AddTask(normal)

	tasks := mgr.getToDoTasks("run-id")
	require.Len(t, tasks, 2)
	// First task must be the heartbeat.
	require.Equal(t, proto.OpDataNodeHeartbeat, tasks[0].OpCode)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_UrgentTasks
// Urgent tasks (create/load operations) are prioritised over normal tasks.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_UrgentTasks(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	urgent := proto.NewAdminTask(proto.OpCreateDataPartition, "urgent", nil)
	normal := proto.NewAdminTask(proto.OpDeleteDataPartition, "normal", nil)

	mgr.AddTask(urgent)
	mgr.AddTask(normal)

	tasks := mgr.getToDoTasks("run-id")
	require.GreaterOrEqual(t, len(tasks), 2)

	// The urgent task must appear before the normal task.
	urgentIdx, normalIdx := -1, -1
	for i, task := range tasks {
		switch task.OpCode {
		case proto.OpCreateDataPartition:
			urgentIdx = i
		case proto.OpDeleteDataPartition:
			normalIdx = i
		}
	}
	require.NotEqual(t, -1, urgentIdx)
	require.NotEqual(t, -1, normalIdx)
	require.Less(t, urgentIdx, normalIdx)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_NormalAndVersionOp
// Normal tasks and OpVersionOperation tasks are collected after heartbeat/urgent.
// OpVersionOperation triggers a special log branch inside getToDoTasks.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_NormalAndVersionOp(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	versionTask := proto.NewAdminTask(proto.OpVersionOperation, "version-addr", "req")
	normalTask := proto.NewAdminTask(proto.OpDeleteDataPartition, "del-addr", nil)

	mgr.AddTask(versionTask)
	mgr.AddTask(normalTask)

	tasks := mgr.getToDoTasks("run-id")
	require.Len(t, tasks, 2)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_MaxTaskNumCutoff
// When the number of tasks exceeds MaxTaskNum the loop should break.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_MaxTaskNumCutoff(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	// Add MaxTaskNum+5 normal tasks so the break is guaranteed to fire.
	// Each task needs a globally unique ID - use a numeric suffix.
	for i := 0; i < MaxTaskNum+5; i++ {
		task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr-"+strconv.Itoa(i), nil)
		// Override the auto-generated ID to guarantee uniqueness.
		task.ID = "maxcut-task-" + strconv.Itoa(i)
		mgr.AddTask(task)
	}

	require.Greater(t, len(mgr.TaskMap), MaxTaskNum)

	tasks := mgr.getToDoTasks("run-id")
	// The loop breaks after MaxTaskNum+1 tasks (tasks collected before the break fires).
	require.LessOrEqual(t, len(tasks), MaxTaskNum+1)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_AlreadySentNotResent
// Tasks that were sent recently and have a recent SendTime must not be returned.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_AlreadySentNotResent(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	sent := proto.NewAdminTask(proto.OpDeleteDataPartition, "sent", nil)
	// Setting SendTime to now means CheckTaskNeedSend returns false.
	sent.SendTime = time.Now().Unix()
	sent.SendCount = 1
	mgr.AddTask(sent)

	tasks := mgr.getToDoTasks("run-id")
	require.Empty(t, tasks)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_updateTaskInfo_BothBranches
// connSuccess=true: SendTime and Status are updated.
// connSuccess=false: only SendCount is incremented.
// -------------------------------------------------------------------------

func TestAdminTaskManager_updateTaskInfo_BothBranches(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	// Branch: connSuccess = false (connection failed, task is just counted).
	task1 := proto.NewAdminTask(proto.OpDeleteDataPartition, "a1", nil)
	require.Equal(t, uint8(0), task1.SendCount)
	before := task1.SendTime

	mgr.updateTaskInfo(task1, false)
	require.Equal(t, uint8(1), task1.SendCount)
	require.Equal(t, before, task1.SendTime) // SendTime unchanged
	require.NotEqual(t, int8(proto.TaskRunning), task1.Status)

	// Branch: connSuccess = true (connection was made, task is now "running").
	task2 := proto.NewAdminTask(proto.OpDeleteDataPartition, "a2", nil)
	mgr.updateTaskInfo(task2, true)
	require.Equal(t, uint8(1), task2.SendCount)
	require.Greater(t, task2.SendTime, int64(0))
	require.Equal(t, int8(proto.TaskRunning), task2.Status)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_buildPacket_Normal
// buildPacket creates a valid packet from an AdminTask.
// -------------------------------------------------------------------------

func TestAdminTaskManager_buildPacket_Normal(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", "my-request")
	task.PartitionID = 42

	pkt, err := mgr.buildPacket(task)
	require.NoError(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, task.OpCode, pkt.Opcode)
	require.Equal(t, task.PartitionID, pkt.PartitionID)
	require.Greater(t, int(pkt.Size), 0)
	require.NotNil(t, pkt.Data)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_buildPacket_MarshalFailure
// Setting Request to a channel makes json.Marshal fail.
// -------------------------------------------------------------------------

func TestAdminTaskManager_buildPacket_MarshalFailure(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", make(chan int))
	_, err := mgr.buildPacket(task)
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getConn_NoPool_InvalidAddr
// When useConnPool=false and the target is unreachable, getConn returns an error.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getConn_NoPool_InvalidAddr(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mgr := newTestManager("127.0.0.1:1") // port 1 should not be listening
	conn, err := mgr.getConn()
	require.Error(t, err)
	require.Nil(t, conn)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getConn_NoPool_ValidAddr
// When useConnPool=false and a real TCP listener is available, getConn succeeds.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getConn_NoPool_ValidAddr(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		c, _ := ln.Accept()
		if c != nil {
			c.Close()
		}
	}()

	mgr := newTestManager(ln.Addr().String())
	conn, err := mgr.getConn()
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getConn_WithPool_InvalidAddr
// When useConnPool=true and the target is unreachable, getConn returns an error.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getConn_WithPool_InvalidAddr(t *testing.T) {
	prev := useConnPool
	useConnPool = true
	defer func() { useConnPool = prev }()

	mgr := newTestManager("127.0.0.1:1")
	conn, err := mgr.getConn()
	require.Error(t, err)
	require.Nil(t, conn)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_putConn_NoPool
// When useConnPool=false, putConn is a no-op (covers the false branch).
// -------------------------------------------------------------------------

func TestAdminTaskManager_putConn_NoPool(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mgr := newTestManager("127.0.0.1:19999")
	// Passing nil is safe when useConnPool=false because the body is not executed.
	require.NotPanics(t, func() {
		mgr.putConn(nil, false)
	})
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_putConn_WithPool_ForceClose
// When useConnPool=true, putConn delegates to the connection pool.
// -------------------------------------------------------------------------

func TestAdminTaskManager_putConn_WithPool_ForceClose(t *testing.T) {
	prev := useConnPool
	useConnPool = true
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		c, _ := ln.Accept()
		if c != nil {
			// Keep it open briefly so putConn can return it to the pool.
			time.Sleep(50 * time.Millisecond)
			c.Close()
		}
	}()

	innerPrev := useConnPool
	useConnPool = false
	defer func() { useConnPool = innerPrev }()

	mgr := newTestManager(ln.Addr().String())
	conn, err := mgr.getConn()
	require.NoError(t, err)

	useConnPool = true
	// Returning to the pool with forceClose=true should not panic.
	require.NotPanics(t, func() {
		mgr.putConn(conn, true)
	})
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendAdminTask_BuildPacketError
// When buildPacket fails (non-serialisable Request), sendAdminTask returns an error.
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendAdminTask_BuildPacketError(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", make(chan int))
	// Use net.Pipe() for a valid-but-unused connection.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	err := mgr.sendAdminTask(task, clientConn)
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendAdminTask_WriteConnError
// Closing the server side of a Pipe before the write causes WriteToConn to fail.
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendAdminTask_WriteConnError(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)
	clientConn, serverConn := net.Pipe()
	// Close server immediately → any write from client fails.
	serverConn.Close()
	defer clientConn.Close()

	err := mgr.sendAdminTask(task, clientConn)
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendAdminTask_ReadConnError
// The server reads the request and then closes, causing ReadFromConn to fail.
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendAdminTask_ReadConnError(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go func() {
		defer serverConn.Close()
		// Read the complete request packet using the same framing the client uses.
		// This allows the client's WriteToConn to complete successfully, after which
		// the server closes without sending a response so ReadFromConn gets an EOF.
		reqPkt := proto.NewPacket()
		reqPkt.ReadFromConnWithVer(serverConn, proto.NoReadDeadlineTime)
	}()

	err := mgr.sendAdminTask(task, clientConn)
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_syncSendAdminTask_GetConnError
// syncSendAdminTask returns an error when getConn fails.
// -------------------------------------------------------------------------

func TestAdminTaskManager_syncSendAdminTask_GetConnError(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mgr := newTestManager("127.0.0.1:1") // unreachable
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)

	pkt, err := mgr.syncSendAdminTask(task)
	require.Error(t, err)
	require.Nil(t, pkt)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_syncSendAdminTask_BuildPacketError
// When buildPacket fails, syncSendAdminTask returns the error before connecting.
// -------------------------------------------------------------------------

func TestAdminTaskManager_syncSendAdminTask_BuildPacketError(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	// Even with a reachable address, the error occurs before getConn.
	// We use an unreachable addr to confirm buildPacket error takes precedence.
	mgr := newTestManager("127.0.0.1:1")
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", make(chan int))

	pkt, err := mgr.syncSendAdminTask(task)
	require.Error(t, err)
	require.Nil(t, pkt)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_syncSendAdminTask_WriteError
// Closing server side before write triggers WriteToConn error path.
// -------------------------------------------------------------------------

func TestAdminTaskManager_syncSendAdminTask_WriteError(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Accept and immediately close → WriteToConn from client fails.
	go func() {
		c, _ := ln.Accept()
		if c != nil {
			c.Close()
		}
	}()

	mgr := newTestManager(ln.Addr().String())
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)

	pkt, err := mgr.syncSendAdminTask(task)
	require.Error(t, err)
	require.Nil(t, pkt)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_syncSendAdminTask_ReadError
// Server reads the request then closes without writing a response.
// -------------------------------------------------------------------------

func TestAdminTaskManager_syncSendAdminTask_ReadError(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		c, _ := ln.Accept()
		if c == nil {
			return
		}
		defer c.Close()
		// Read the complete request packet using proper framing.
		// This lets the client's WriteToConn succeed. Then we close without
		// responding so the client's ReadFromConnWithVer gets an immediate EOF.
		reqPkt := proto.NewPacket()
		reqPkt.ReadFromConnWithVer(c, proto.NoReadDeadlineTime)
	}()

	mgr := newTestManager(ln.Addr().String())
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)

	pkt, err := mgr.syncSendAdminTask(task)
	require.Error(t, err)
	require.Nil(t, pkt)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_syncSendAdminTask_ResultCodeNotOk
// Server reads the full request and responds with a valid packet whose
// ResultCode is not OpOk (0xF0), exercising the error-logging branch in
// syncSendAdminTask (lines 226-230).
// -------------------------------------------------------------------------

func TestAdminTaskManager_syncSendAdminTask_ResultCodeNotOk(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		c, _ := ln.Accept()
		if c == nil {
			return
		}
		defer c.Close()

		// Read the complete request packet so the client's WriteToConn finishes.
		reqPkt := proto.NewPacket()
		if err := reqPkt.ReadFromConnWithVer(c, proto.NoReadDeadlineTime); err != nil {
			return
		}

		// Write back a response whose ResultCode is not OpOk (0xF0).
		resp := proto.NewPacket()
		resp.ResultCode = 0x01 // any value != proto.OpOk
		resp.Data = []byte(`{"error":"injected"}`)
		resp.Size = uint32(len(resp.Data))
		resp.WriteToConn(c) //nolint:errcheck – best effort in test goroutine
	}()

	mgr := newTestManager(ln.Addr().String())
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "addr", nil)

	pkt, err := mgr.syncSendAdminTask(task)
	require.Error(t, err)
	require.NotNil(t, pkt) // packet is returned even on non-OpOk result
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendTasks_ConnFailure
// When getConn fails for all tasks, sendTasks breaks on the first task
// and calls updateTaskInfo(task, false), leaving SendTime unchanged.
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendTasks_ConnFailure(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	// Target address that nothing is listening on.
	mgr := newTestManager("127.0.0.1:1")
	task := proto.NewAdminTask(proto.OpDeleteDataPartition, "a1", nil)

	sendTimeBefore := task.SendTime
	mgr.sendTasks([]*proto.AdminTask{task})

	// On connection failure, SendCount is incremented but SendTime remains unchanged.
	require.Equal(t, uint8(1), task.SendCount)
	require.Equal(t, sendTimeBefore, task.SendTime)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendTasks_SendAdminTaskFailure
// getConn succeeds but the server closes immediately → sendAdminTask fails.
// The task is then retried (continue), not abandoned (break).
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendTasks_SendAdminTaskFailure(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Server accepts and immediately closes the connection.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	mgr := newTestManager(ln.Addr().String())

	task1 := proto.NewAdminTask(proto.OpDeleteDataPartition, "a1", nil)
	task2 := proto.NewAdminTask(proto.OpDeleteDataPartition, "a2", nil)
	// Assign distinct IDs.
	task2.ID = "task2-distinct"

	mgr.sendTasks([]*proto.AdminTask{task1, task2})

	// Both tasks should have their SendCount incremented (continue path).
	require.Equal(t, uint8(1), task1.SendCount)
	require.Equal(t, uint8(1), task2.SendCount)
	// SendTime is set by updateTaskInfo(task, true) when connection succeeded.
	require.Greater(t, task1.SendTime, int64(0))
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_sendTasks_VersionOpTask
// OpVersionOperation tasks log a special message; exercise that code path.
// -------------------------------------------------------------------------

func TestAdminTaskManager_sendTasks_VersionOpTask(t *testing.T) {
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	// Use an unreachable address to trigger the connection-failure break.
	mgr := newTestManager("127.0.0.1:1")

	versionTask := proto.NewAdminTask(proto.OpVersionOperation, "version-addr", nil)
	mgr.sendTasks([]*proto.AdminTask{versionTask})

	// The task's SendCount must have been incremented.
	require.Equal(t, uint8(1), versionTask.SendCount)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_process_ExitViaChannel
// Sending to exitCh causes the process goroutine to stop cleanly.
// -------------------------------------------------------------------------

func TestAdminTaskManager_process_ExitViaChannel(t *testing.T) {
	// newAdminTaskManager launches process(); sending to exitCh stops it.
	mgr := newAdminTaskManager("127.0.0.1:19999", "exit-test-cluster")

	done := make(chan struct{})
	go func() {
		// The background goroutine runs until it receives on exitCh.
		mgr.exitCh <- struct{}{}
		close(done)
	}()

	select {
	case <-done:
		// Process goroutine received the exit signal within the deadline.
	case <-time.After(5 * time.Second):
		t.Fatal("process goroutine did not exit within 5 seconds")
	}
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_getToDoTasks_AllTypes
// Comprehensive integration-style test: heartbeat, urgent, normal, and
// version-op tasks all placed into one manager.  Verifies that all three
// collection passes in getToDoTasks fire correctly.
// -------------------------------------------------------------------------

func TestAdminTaskManager_getToDoTasks_AllTypes(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")

	hb := proto.NewAdminTask(proto.OpMetaNodeHeartbeat, "hb", nil)
	urgent := proto.NewAdminTask(proto.OpCreateMetaPartition, "urgent", nil)
	verOp := proto.NewAdminTask(proto.OpVersionOperation, "ver-op", "any")
	del := proto.NewAdminTask(proto.OpDeleteDataPartition, "del", nil)

	// Give each a unique ID to prevent accidental dedup.
	hb.ID = "hb-1"
	urgent.ID = "urgent-1"
	verOp.ID = "verop-1"
	del.ID = "del-1"

	mgr.AddTask(hb)
	mgr.AddTask(urgent)
	mgr.AddTask(verOp)
	mgr.AddTask(del)

	tasks := mgr.getToDoTasks("all-types")
	require.Len(t, tasks, 4)
}

// -------------------------------------------------------------------------
// TestAdminTaskManager_doSendTasks_NoTasks
// When the TaskMap is empty, doSendTasks returns early after getToDoTasks.
// -------------------------------------------------------------------------

func TestAdminTaskManager_doSendTasks_NoTasks(t *testing.T) {
	mgr := newTestManager("127.0.0.1:19999")
	// Empty map → getToDoTasks returns empty slice → early return.
	require.NotPanics(t, func() {
		mgr.doSendTasks()
	})
}
