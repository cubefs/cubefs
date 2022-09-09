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

package proto

import (
	"fmt"
	"time"
)

const (
	TaskFailed       = 2
	TaskStart        = 0
	TaskSucceeds     = 1
	TaskRunning      = 3
	ResponseInterval = 5
	ResponseTimeOut  = 100
	MaxSendCount     = 5
)

// AdminTask defines the administration task.
type AdminTask struct {
	ID           string
	PartitionID  uint64
	OpCode       uint8
	OperatorAddr string
	Status       int8
	SendTime     int64
	CreateTime   int64
	SendCount    uint8
	Request      interface{}
	Response     interface{}
}

// ToString returns the string format of the task.
func (t *AdminTask) ToString() (msg string) {
	msg = fmt.Sprintf("ID[%v] Status[%d] LastSendTime[%v]  SendCount[%v] Request[%v] Response[%v]",
		t.ID, t.Status, t.SendTime, t.SendCount, t.Request, t.Response)

	return
}

func (t *AdminTask) IdString() string {
	return fmt.Sprintf("id:%s_sendTime_%d_createTime_%d", t.ID, t.SendTime, t.CreateTime)
}

// CheckTaskNeedSend checks if the task needs to be sent out.
func (t *AdminTask) CheckTaskNeedSend() (needRetry bool) {
	if (int)(t.SendCount) < MaxSendCount && time.Now().Unix()-t.SendTime > (int64)(ResponseInterval) {
		needRetry = true
	}
	return
}

// CheckTaskTimeOut checks if the task is timed out.
func (t *AdminTask) CheckTaskTimeOut() (notResponse bool) {
	if (int)(t.SendCount) >= MaxSendCount || (t.SendTime > 0 && (time.Now().Unix()-t.SendTime > int64(ResponseTimeOut))) {
		notResponse = true
	}
	return
}

// SetStatus sets the status of the task.
func (t *AdminTask) SetStatus(status int8) {
	t.Status = status
}

// IsTaskSuccessful returns if the task has been executed successful.
func (t *AdminTask) IsTaskSuccessful() (isSuccess bool) {
	if t.Status == TaskSucceeds {
		isSuccess = true
	}

	return
}

// IsTaskFailed returns if the task failed.
func (t *AdminTask) IsTaskFailed() (isFail bool) {
	if t.Status == TaskFailed {
		isFail = true
	}

	return
}

// IsUrgentTask returns if the task is urgent.
func (t *AdminTask) IsUrgentTask() bool {
	return t.isCreateTask() || t.isLoadTask() || t.isUpdateMetaPartitionTask()
}

// isUpdateMetaPartitionTask checks if the task is to update the meta partition.
func (t *AdminTask) isUpdateMetaPartitionTask() bool {
	return t.OpCode == OpUpdateMetaPartition
}

func (t *AdminTask) isLoadTask() bool {
	return t.OpCode == OpLoadDataPartition
}

func (t *AdminTask) isCreateTask() bool {
	return t.OpCode == OpCreateDataPartition || t.OpCode == OpCreateMetaPartition
}

// IsHeartbeatTask returns if the task is a heartbeat task.
func (t *AdminTask) IsHeartbeatTask() bool {
	return t.OpCode == OpDataNodeHeartbeat || t.OpCode == OpMetaNodeHeartbeat
}

// NewAdminTask returns a new adminTask.
func NewAdminTask(opCode uint8, opAddr string, request interface{}) (t *AdminTask) {
	t = new(AdminTask)
	t.OpCode = opCode
	t.Request = request
	t.OperatorAddr = opAddr
	t.ID = fmt.Sprintf("addr[%v]_op[%v]", t.OperatorAddr, t.OpCode)
	t.CreateTime = time.Now().Unix()
	return
}
