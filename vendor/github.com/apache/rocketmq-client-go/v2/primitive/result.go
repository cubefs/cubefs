/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package primitive

import (
	"fmt"
)

// SendStatus of message
type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
	SendUnknownError

	FlagCompressed  = 0x1
	FlagBornHostV6  = 0x1 << 4
	FlagStoreHostV6 = 0x1 << 5
	MsgIdLength     = 8 + 8

	propertySeparator  = '\002'
	nameValueSeparator = '\001'
)

// SendResult RocketMQ send result
type SendResult struct {
	Status        SendStatus
	MsgID         string
	MessageQueue  *MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	RegionID      string
	TraceOn       bool
}

func NewSendResult() *SendResult {
	return &SendResult{Status: SendUnknownError}
}

// SendResult send message result to string(detail result)
func (result *SendResult) String() string {
	return fmt.Sprintf("SendResult [sendStatus=%d, msgIds=%s, offsetMsgId=%s, queueOffset=%d, messageQueue=%s]",
		result.Status, result.MsgID, result.OffsetMsgID, result.QueueOffset, result.MessageQueue.String())
}

// SendResult RocketMQ send result
type TransactionSendResult struct {
	*SendResult
	State LocalTransactionState
}

// PullStatus pull Status
type PullStatus int

// predefined pull Status
const (
	PullFound PullStatus = iota
	PullNoNewMsg
	PullNoMsgMatched
	PullOffsetIllegal
	PullBrokerTimeout
)

// PullResult the pull result
type PullResult struct {
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
	Status               PullStatus
	SuggestWhichBrokerId int64

	// messageExts message info
	messageExts []*MessageExt
	//
	body []byte
}

func (result *PullResult) GetMessageExts() []*MessageExt {
	return result.messageExts
}

func (result *PullResult) SetMessageExts(msgExts []*MessageExt) {
	result.messageExts = msgExts
}

func (result *PullResult) GetMessages() []*Message {
	if len(result.messageExts) == 0 {
		return make([]*Message, 0)
	}
	return toMessages(result.messageExts)
}

func (result *PullResult) SetBody(data []byte) {
	result.body = data
}

func (result *PullResult) GetBody() []byte {
	return result.body
}

func (result *PullResult) String() string {
	return ""
}

func toMessages(messageExts []*MessageExt) []*Message {
	msgs := make([]*Message, 0)

	return msgs
}
