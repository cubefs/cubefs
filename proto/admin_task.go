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

package proto

import (
	"fmt"
	"time"
)

const (
	TaskFail         = 2
	TaskStart        = 0
	TaskSuccess      = 1
	TaskRunning      = 3
	ResponseInterval = 5
	ResponseTimeOut  = 100
	MaxSendCount     = 5
)

/*task struct to node*/
type AdminTask struct {
	ID           string
	OpCode       uint8
	OperatorAddr string
	Status       int8
	SendTime     int64
	CreateTime   int64
	SendCount    uint8
	Request      interface{}
	Response     interface{}
}

func (t *AdminTask) ToString() (msg string) {
	msg = fmt.Sprintf("Id[%v] Status[%d] LastSendTime[%v]  SendCount[%v] Request[%v] Response[%v]",
		t.ID, t.Status, t.SendTime, t.SendCount, t.Request, t.Response)

	return
}

//1.has never send, t.SendTime=0,
//2.has send but response time out
func (t *AdminTask) CheckTaskNeedSend() (needRetry bool) {
	if (int)(t.SendCount) < MaxSendCount && time.Now().Unix()-t.SendTime > (int64)(ResponseInterval) {
		needRetry = true
	}
	return
}

//the task which sendCount >=  MaxSendCount, the last send has no response after ResponseTimeOut passed,
// to be consider time out
func (t *AdminTask) CheckTaskTimeOut() (notResponse bool) {
	var (
		timeOut int64
	)
	timeOut = ResponseTimeOut
	if (int)(t.SendCount) >= MaxSendCount || (t.SendTime > 0 && (time.Now().Unix()-t.SendTime > timeOut)) {
		notResponse = true
	}

	return
}

func (t *AdminTask) SetStatus(status int8) {
	t.Status = status
}

func (t *AdminTask) CheckTaskIsSuccess() (isSuccess bool) {
	if t.Status == TaskSuccess {
		isSuccess = true
	}

	return
}

func (t *AdminTask) CheckTaskIsFail() (isFail bool) {
	if t.Status == TaskFail {
		isFail = true
	}

	return
}

func (t *AdminTask) IsUrgentTask() bool {
	return t.isCreateTask() || t.isLoadTask() || t.isUpdateEndTask()
}

func (t *AdminTask) isUpdateEndTask() bool {
	return t.OpCode == OpUpdateMetaPartition
}

func (t *AdminTask) isLoadTask() bool {
	return t.OpCode == OpLoadDataPartition
}

func (t *AdminTask) isCreateTask() bool {
	return t.OpCode == OpCreateDataPartition || t.OpCode == OpCreateMetaPartition
}

func (t *AdminTask) IsHeartbeatTask() bool {
	return t.OpCode == OpDataNodeHeartbeat || t.OpCode == OpMetaNodeHeartbeat
}

func NewAdminTask(opCode uint8, opAddr string, request interface{}) (t *AdminTask) {
	t = new(AdminTask)
	t.OpCode = opCode
	t.Request = request
	t.OperatorAddr = opAddr
	t.ID = fmt.Sprintf("addr[%v]_op[%v]", t.OperatorAddr, t.OpCode)
	t.CreateTime = time.Now().Unix()
	return
}
