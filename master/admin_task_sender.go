// Copyright 2018 The ChuBao Authors.
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
	"sync"
	"time"

	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/chubaoio/cbfs/util/pool"
	"github.com/juju/errors"
	"net"
)

const (
	MinTaskLen         = 30
	TaskWorkerInterval = time.Microsecond * time.Duration(200)
	ForceCloseConnect  = true
	NoCloseConnect     = false
)

/*
master send admin command to metaNode or dataNode by the sender,
because this command is cost very long time,so the sender just send command
and do nothing..then the metaNode or  dataNode send a new http request to reply command response
to master

*/

type AdminTaskSender struct {
	clusterID  string
	targetAddr string
	TaskMap    map[string]*proto.AdminTask
	sync.Mutex
	exitCh   chan struct{}
	connPool *pool.ConnPool
}

func NewAdminTaskSender(targetAddr, clusterID string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		clusterID:  clusterID,
		TaskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
		connPool:   pool.NewConnPool(),
	}
	go sender.process()

	return
}

func (sender *AdminTaskSender) process() {
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

func (sender *AdminTaskSender) doDeleteTasks() {
	delTasks := sender.getNeedDeleteTasks()
	for _, t := range delTasks {
		sender.DelTask(t)
	}
	return
}

// the task which is time out will be delete
func (sender *AdminTaskSender) getNeedDeleteTasks() (delTasks []*proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	delTasks = make([]*proto.AdminTask, 0)
	for _, task := range sender.TaskMap {
		if task.CheckTaskTimeOut() {
			log.LogWarnf(fmt.Sprintf("clusterID[%v] %v has no response util time out",
				sender.clusterID, task.ID))
			if task.SendTime > 0 {
				Warn(sender.clusterID, fmt.Sprintf("clusterID[%v] %v has no response util time out",
					sender.clusterID, task.ID))
			}
			delTasks = append(delTasks, task)
		}
	}
	return
}

func (sender *AdminTaskSender) doSendTasks() {
	tasks := sender.getNeedDealTask()
	if len(tasks) == 0 {
		time.Sleep(time.Second)
		return
	}
	sender.sendTasks(tasks)
}

func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) {
	for _, task := range tasks {
		conn, err := sender.connPool.Get(sender.targetAddr)
		if err != nil {
			msg := fmt.Sprintf("clusterID[%v] get connection to %v,err,%v", sender.clusterID, sender.targetAddr, errors.ErrorStack(err))
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_sendTask", sender.clusterID, UmpModuleName), msg)
			sender.connPool.Put(conn, ForceCloseConnect)
			sender.updateTaskInfo(task, false)
			break
		}
		if err = sender.sendAdminTask(task, conn); err != nil {
			log.LogError(fmt.Sprintf("send task %v to %v,err,%v", task.ToString(), sender.targetAddr, errors.ErrorStack(err)))
			sender.connPool.Put(conn, ForceCloseConnect)
			sender.updateTaskInfo(task, true)
			continue
		}
		sender.connPool.Put(conn, NoCloseConnect)
	}
}

func (sender *AdminTaskSender) updateTaskInfo(task *proto.AdminTask, connSuccess bool) {
	task.SendCount++
	if connSuccess {
		task.SendTime = time.Now().Unix()
		task.Status = proto.TaskRunning
	}

}

func (sender *AdminTaskSender) buildPacket(task *proto.AdminTask) (packet *proto.Packet, err error) {
	packet = proto.NewPacket()
	packet.Opcode = task.OpCode
	packet.ReqID = proto.GetReqID()
	body, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	packet.Size = uint32(len(body))
	packet.Data = body
	return packet, nil
}

func (sender *AdminTaskSender) sendAdminTask(task *proto.AdminTask, conn net.Conn) (err error) {
	packet, err := sender.buildPacket(task)
	if err != nil {
		return errors.Annotatef(err, "action[sendAdminTask build packet failed,task:%v]", task.ID)
	}
	if err = packet.WriteToConn(conn); err != nil {
		return errors.Annotatef(err, "action[sendAdminTask],WriteToConn failed,task:%v", task.ID)
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return errors.Annotatef(err, "action[sendAdminTask],ReadFromConn failed task:%v", task.ID)
	}
	log.LogDebugf(fmt.Sprintf("action[sendAdminTask] sender task:%v success", task.ToString()))
	sender.updateTaskInfo(task, true)

	return nil
}

func (sender *AdminTaskSender) DelTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		return
	}
	if t.OpCode != proto.OpMetaNodeHeartbeat && t.OpCode != proto.OpDataNodeHeartbeat {
		log.LogDebugf("action[DelTask] delete task[%v]", t.ToString())
	}
	delete(sender.TaskMap, t.ID)
}

func (sender *AdminTaskSender) PutTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		sender.TaskMap[t.ID] = t
	}
}

func (sender *AdminTaskSender) IsExist(t *proto.AdminTask) bool {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	return ok
}

func (sender *AdminTaskSender) getNeedDealTask() (tasks []*proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	tasks = make([]*proto.AdminTask, 0)

	//send heartbeat task first
	for _, t := range sender.TaskMap {
		if t.IsHeartbeatTask() && t.CheckTaskNeedSend() == true {
			tasks = append(tasks, t)
			t.SendTime = time.Now().Unix()
			t.SendCount++
		}
	}
	//send urgent task immediately
	for _, t := range sender.TaskMap {
		if t.IsUrgentTask() && t.CheckTaskNeedSend() == true {
			tasks = append(tasks, t)
			t.SendTime = time.Now().Unix()
			t.SendCount++
		}
	}
	for _, task := range sender.TaskMap {
		if !task.IsHeartbeatTask() && !task.IsUrgentTask() && task.CheckTaskNeedSend() {
			tasks = append(tasks, task)
		}
		if len(tasks) > MinTaskLen {
			break
		}
	}
	return
}
