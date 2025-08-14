// Copyright 2018 The CubeFS Authors.
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
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

// const
const (
	// the maximum number of tasks that can be handled each time
	MaxTaskNum = 30

	TaskWorkerInterval = time.Second * time.Duration(2)
	idleConnTimeout    = 90                     // seconds
	connectTimeout     = 10                     // seconds
	MaxRetryNum        = 3                      // maximum number of retries for sending tasks
	RetryInterval      = 100 * time.Millisecond // retry interval for sending tasks
)

// AdminTaskManager sends administration commands to the metaNode or dataNode.
type AdminTaskManager struct {
	clusterID  string
	targetAddr string
	TaskMap    map[string]*proto.AdminTask
	sync.RWMutex
	exitCh   chan struct{}
	connPool *util.ConnectPool
}

func newAdminTaskManager(targetAddr, clusterID string) (sender *AdminTaskManager) {
	proto.InitBufferPool(int64(32768))

	sender = &AdminTaskManager{
		targetAddr: targetAddr,
		clusterID:  clusterID,
		TaskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}, 1),
		connPool:   util.NewConnectPoolWithTimeout(idleConnTimeout, connectTimeout),
	}
	go sender.process()

	return
}

func (sender *AdminTaskManager) process() {
	ticker := time.NewTicker(TaskWorkerInterval)
	defer func() {
		ticker.Stop()
		Warn(sender.clusterID, fmt.Sprintf("clusterID[%v] %v sender stop", sender.clusterID, sender.targetAddr))
	}()
	for {
		select {
		case <-sender.exitCh:
			return
		case <-ticker.C:
			sender.doDeleteTasks()
			sender.doSendTasks()
		}
	}
}

func (sender *AdminTaskManager) doDeleteTasks() {
	delTasks := sender.getToBeDeletedTasks()
	for _, t := range delTasks {
		sender.DelTask(t)
	}
}

func (sender *AdminTaskManager) getToBeDeletedTasks() (delTasks []*proto.AdminTask) {
	sender.RLock()
	defer sender.RUnlock()
	delTasks = make([]*proto.AdminTask, 0)

	for _, task := range sender.TaskMap {
		if task.CheckTaskTimeOut() {
			log.LogWarnf(fmt.Sprintf("clusterID[%v] %v has no response until time out",
				sender.clusterID, task.ID))
			if task.SendTime > 0 {
				Warn(sender.clusterID, fmt.Sprintf("clusterID[%v] %v has no response until time out",
					sender.clusterID, task.ID))
			}

			// timed-out tasks will be deleted
			delTasks = append(delTasks, task)
		}
	}
	return
}

func (sender *AdminTaskManager) doSendTasks() {
	id := uuid.New()
	log.LogDebugf("doSendTasks %v", id.String())
	tasks := sender.getToDoTasks(id.String())
	if len(tasks) == 0 {
		return
	}
	sender.sendTasks(tasks)
}

func (sender *AdminTaskManager) getConn() (conn *net.TCPConn, err error) {
	if useConnPool {
		return sender.connPool.GetConnect(sender.targetAddr)
	}
	var connect net.Conn
	connect, err = net.Dial("tcp", sender.targetAddr)
	if err == nil {
		conn = connect.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
	}
	return
}

func (sender *AdminTaskManager) putConn(conn *net.TCPConn, forceClose bool) {
	if useConnPool {
		sender.connPool.PutConnect(conn, forceClose)
	}
}

func (sender *AdminTaskManager) sendTasks(tasks []*proto.AdminTask) {
	for _, task := range tasks {
		if task.OpCode == proto.OpVersionOperation {
			log.LogInfof("action[sendTasks] get task to addr [%v]", task.OperatorAddr)
		}
		conn, err := sender.getConn()
		if err != nil {
			msg := fmt.Sprintf("clusterID[%v] get connection to %v,err,%v", sender.clusterID, sender.targetAddr, errors.Stack(err))
			WarnBySpecialKey(fmt.Sprintf("%v_%v_sendTask", sender.clusterID, ModuleName), msg)
			sender.putConn(conn, true)
			sender.updateTaskInfo(task, false)
			break
		}
		if err = sender.sendAdminTask(task, conn); err != nil {
			log.LogError(fmt.Sprintf("send task %v to %v err %v,errStack,%v", task.ID, sender.targetAddr, err, errors.Stack(err)))
			sender.putConn(conn, true)
			sender.updateTaskInfo(task, true)
			continue
		}
		sender.putConn(conn, false)
	}
}

func (sender *AdminTaskManager) updateTaskInfo(task *proto.AdminTask, connSuccess bool) {
	task.SendCount++
	if connSuccess {
		task.SendTime = time.Now().Unix()
		task.Status = proto.TaskRunning
	}
}

func (sender *AdminTaskManager) buildPacket(task *proto.AdminTask) (packet *proto.Packet, err error) {
	packet = proto.NewPacket()
	packet.Opcode = task.OpCode
	packet.ReqID = proto.GenerateRequestID()
	packet.PartitionID = task.PartitionID
	body, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	packet.Size = uint32(len(body))
	packet.Data = body
	return packet, nil
}

func (sender *AdminTaskManager) sendAdminTask(task *proto.AdminTask, conn net.Conn) (err error) {
	packet, err := sender.buildPacket(task)
	if err != nil {
		return errors.Trace(err, "action[sendAdminTask build packet failed,task:%v]", task.ID)
	}
	if err = packet.WriteToConn(conn); err != nil {
		return errors.Trace(err, "action[sendAdminTask],WriteToConn failed,task:%v", task.ID)
	}
	if err = packet.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
		return errors.Trace(err, "action[sendAdminTask],ReadFromConn failed task:%v", task.ID)
	}
	log.LogDebugf(fmt.Sprintf("action[sendAdminTask] sender task:%v success", task.ToString()))
	sender.updateTaskInfo(task, true)

	return nil
}

func (sender *AdminTaskManager) syncSendAdminTask(task *proto.AdminTask) (packet *proto.Packet, err error) {
	packet, err = sender.buildPacket(task)
	if err != nil {
		return nil, errors.Trace(err, "action[syncSendAdminTask build packet failed,task:%v]", task.ID)
	}
	log.LogInfof("action[syncSendAdminTask],task[%s], op %s, reqId %d", task.ToString(), packet.GetOpMsg(), packet.GetReqID())
	conn, err := sender.getConn()
	if err != nil {
		return nil, errors.Trace(err, "action[syncSendAdminTask get conn failed,task:%v]", task.ID)
	}
	defer func() {
		if err == nil {
			sender.putConn(conn, false)
		} else {
			sender.putConn(conn, true)
		}
	}()

	for i := 0; i < MaxRetryNum; i++ {
		if err = packet.WriteToConn(conn); err != nil {
			log.LogErrorf(fmt.Sprintf("action[sendAdminTask],WriteToConn failed,task:%v", task.ID))
			time.Sleep(RetryInterval)
			continue
		}
		break
	}
	if err != nil {
		return nil, errors.Trace(err, "action[syncSendAdminTask],WriteToConn failed,task:%v,reqID[%v]", task.ID, packet.ReqID)
	}

	for i := 0; i < MaxRetryNum; i++ {
		if err = packet.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime); err != nil {
			log.LogErrorf(fmt.Sprintf("action[sendAdminTask],ReadFromConn failed task:%v,err:%v", task.ID, err))
			time.Sleep(RetryInterval)
			continue
		}
		break
	}
	if err != nil {
		return nil, errors.Trace(err, "action[syncSendAdminTask],ReadFromConn failed task:%v,reqID[%v]", task.ID, packet.ReqID)
	}

	if packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("result code[%v],msg[%v]", packet.ResultCode, string(packet.Data))
		log.LogErrorf("action[syncSendAdminTask],task:%v,reqID[%v],err[%v],", task.ID, packet.ReqID, err)
		return
	}
	return packet, nil
}

// DelTask deletes the to-be-deleted tasks.
func (sender *AdminTaskManager) DelTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		return
	}
	if t.OpCode != proto.OpMetaNodeHeartbeat && t.OpCode != proto.OpDataNodeHeartbeat && t.OpCode != proto.OpLcNodeHeartbeat && t.OpCode != proto.OpFlashNodeHeartbeat {
		log.LogDebugf("action[DelTask] delete task[%v]", t.ToString())
	}
	delete(sender.TaskMap, t.ID)
}

// AddTask adds a new task to the task map.
func (sender *AdminTaskManager) AddTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		sender.TaskMap[t.ID] = t
	}
}

func (sender *AdminTaskManager) getToDoTasks(id string) (tasks []*proto.AdminTask) {
	sender.RLock()
	defer sender.RUnlock()
	tasks = make([]*proto.AdminTask, 0)

	// send heartbeat task first
	for _, t := range sender.TaskMap {
		if t.IsHeartbeatTask() && t.CheckTaskNeedSend() {
			tasks = append(tasks, t)
			t.SendTime = time.Now().Unix()
			log.LogDebugf("getToDoTasks get heartbeatTask %v %v", t.RequestID, id)
		}
	}
	// send urgent task immediately
	for _, t := range sender.TaskMap {
		if t.IsUrgentTask() && t.CheckTaskNeedSend() {
			tasks = append(tasks, t)
			t.SendTime = time.Now().Unix()
		}
	}
	for _, task := range sender.TaskMap {
		if !task.IsHeartbeatTask() && !task.IsUrgentTask() && task.CheckTaskNeedSend() {
			tasks = append(tasks, task)
			task.SendTime = time.Now().Unix()
			if task.OpCode == proto.OpVersionOperation {
				log.LogInfof("action[getToDoTasks] get task to addr [%v]", task.OperatorAddr)
				continue
			}
		}

		if len(tasks) > MaxTaskNum {
			break
		}
	}
	return
}
