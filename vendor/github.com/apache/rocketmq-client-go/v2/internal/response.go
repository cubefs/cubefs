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

package internal

const (
	ResSuccess              = int16(0)
	ResError                = int16(1)
	ResFlushDiskTimeout     = int16(10)
	ResSlaveNotAvailable    = int16(11)
	ResFlushSlaveTimeout    = int16(12)
	ResTopicNotExist        = int16(17)
	ResPullNotFound         = int16(19)
	ResPullRetryImmediately = int16(20)
	ResPullOffsetMoved      = int16(21)
)

type SendMessageResponse struct {
	MsgId         string
	QueueId       int32
	QueueOffset   int64
	TransactionId string
	MsgRegion     string
}

func (response *SendMessageResponse) Decode(properties map[string]string) {

}

type PullMessageResponse struct {
	SuggestWhichBrokerId int64
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
}
