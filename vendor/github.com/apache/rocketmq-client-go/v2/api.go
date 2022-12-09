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

package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type Producer interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error),
		msg ...*primitive.Message) error
	SendOneWay(ctx context.Context, mq ...*primitive.Message) error
	Request(ctx context.Context, ttl time.Duration, msg *primitive.Message) (*primitive.Message, error)
	RequestAsync(ctx context.Context, ttl time.Duration, callback internal.RequestCallback, msg *primitive.Message) error
}

func NewProducer(opts ...producer.Option) (Producer, error) {
	return producer.NewDefaultProducer(opts...)
}

type TransactionProducer interface {
	Start() error
	Shutdown() error
	SendMessageInTransaction(ctx context.Context, mq *primitive.Message) (*primitive.TransactionSendResult, error)
}

func NewTransactionProducer(listener primitive.TransactionListener, opts ...producer.Option) (TransactionProducer, error) {
	return producer.NewTransactionProducer(listener, opts...)
}

type PushConsumer interface {
	// Start the PushConsumer for consuming message
	Start() error

	// Shutdown the PushConsumer, all offset of MessageQueue will be sync to broker before process exit
	Shutdown() error
	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error

	// Unsubscribe a topic
	Unsubscribe(topic string) error

	// Suspend the consumption
	Suspend()

	// Resume the consumption
	Resume()
}

func NewPushConsumer(opts ...consumer.Option) (PushConsumer, error) {
	return consumer.NewPushConsumer(opts...)
}

type PullConsumer interface {
	// Start the PullConsumer for consuming message
	Start() error

	// Shutdown the PullConsumer, all offset of MessageQueue will be commit to broker before process exit
	Shutdown() error

	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector) error

	// Unsubscribe a topic
	Unsubscribe(topic string) error

	// MessageQueues get MessageQueue list about for a given topic. This method will issue a remote call to the server
	// if it does not already have any MessageQueue about the given topic.
	MessageQueues(topic string) []primitive.MessageQueue

	// Pull message for the topic specified. It is an error to not have subscribed to any topics before pull for message
	//
	// Specified numbers of messages is returned if message greater that numbers, and the offset will auto forward.
	// It means that if you meeting messages consuming failed, you should process failed messages by yourself.
	Pull(ctx context.Context, topic string, numbers int) (*primitive.PullResult, error)

	// Pull message for the topic specified from a specified MessageQueue and offset. It is an error to not have
	// subscribed to any topics before pull for message. the method will not affect the offset recorded
	//
	// Specified numbers of messages is returned.
	PullFrom(ctx context.Context, mq primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error)

	// Lookup offset for the given message queue by timestamp. The returned offset for the message queue is the
	// earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding message
	// queue.
	//
	// Timestamp must be millisecond level, if you want to lookup the earliest offset of the mq, you could set the
	// timestamp 0, and if you want to the latest offset the mq, you could set the timestamp math.MaxInt64.
	Lookup(ctx context.Context, mq primitive.MessageQueue, timestamp int64) (int64, error)

	// Commit the offset of specified mqs to broker, if auto-commit is disable, you must commit the offset manually.
	Commit(ctx context.Context, mqs ...primitive.MessageQueue) (int64, error)

	// CommittedOffset return the offset of specified Message
	CommittedOffset(mq primitive.MessageQueue) (int64, error)

	// Seek set offset of the mq, if you wanna re-consuming your message form one position, the method may help you.
	// if you want re-consuming from one time, you cloud Lookup() then seek it.
	Seek(mq primitive.MessageQueue, offset int64) error

	// Pause consuming for specified MessageQueues, after pause, client will not fetch any message from the specified
	// message queues
	//
	// Note that this method does not affect message queue subscription. In particular, it does not cause a group
	// rebalance.
	//
	// if a MessageQueue belong a topic that has not been subscribed, an error will be returned
	//Pause(mqs ...primitive.MessageQueue) error

	// Resume specified message queues which have been paused with Pause, if a MessageQueue that not paused,
	// it will be ignored. if not subscribed, an error will be returned
	//Resume(mqs ...primitive.MessageQueue) error
}

// The PullConsumer has not implemented completely, if you want have an experience of PullConsumer, you could use
// consumer.NewPullConsumer(...), but it may changed in the future.
//
// The PullConsumer will be supported in next release
func NewPullConsumer(opts ...consumer.Option) (PullConsumer, error) {
	return nil, errors.ErrPullConsumer
}
