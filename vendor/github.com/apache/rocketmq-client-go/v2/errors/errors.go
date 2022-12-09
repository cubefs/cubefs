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

package errors

import "errors"

var (
	ErrRequestTimeout    = errors.New("request timeout")
	ErrMQEmpty           = errors.New("MessageQueue is nil")
	ErrOffset            = errors.New("offset < 0")
	ErrNumbers           = errors.New("numbers < 0")
	ErrEmptyTopic        = errors.New("empty topic")
	ErrEmptyNameSrv      = errors.New("empty namesrv")
	ErrEmptyGroupID      = errors.New("empty group id")
	ErrTestMin           = errors.New("test minutes must be positive integer")
	ErrOperationInterval = errors.New("operation interval must be positive integer")
	ErrMessageBody       = errors.New("message body size must be positive integer")
	ErrEmptyExpression   = errors.New("empty expression")
	ErrCreated           = errors.New("consumer group has been created")
	ErrBrokerNotFound    = errors.New("broker can not found")
	ErrStartTopic        = errors.New("cannot subscribe topic since client either failed to start or has been shutdown.")
	ErrResponse          = errors.New("response error")
	ErrCompressLevel     = errors.New("unsupported compress level")
	ErrUnknownIP         = errors.New("unknown IP address")
	ErrService           = errors.New("service close is not running, please check")
	ErrTopicNotExist     = errors.New("topic not exist")
	ErrNotExisted        = errors.New("not existed")
	ErrNoNameserver      = errors.New("nameServerAddrs can't be empty.")
	ErrMultiIP           = errors.New("multiple IP addr does not support")
	ErrIllegalIP         = errors.New("IP addr error")
	ErrTopicEmpty        = errors.New("topic is nil")
	ErrMessageEmpty      = errors.New("message is nil")
	ErrNotRunning        = errors.New("producer not started")
	ErrPullConsumer      = errors.New("pull consumer has not supported")
	ErrProducerCreated   = errors.New("producer group has been created")
	ErrMultipleTopics    = errors.New("the topic of the messages in one batch should be the same")
)
