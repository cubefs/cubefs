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

package producer

import (
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func defaultProducerOptions() producerOptions {
	opts := producerOptions{
		ClientOptions:              internal.DefaultClientOptions(),
		Selector:                   NewRoundRobinQueueSelector(),
		SendMsgTimeout:             3 * time.Second,
		DefaultTopicQueueNums:      4,
		CreateTopicKey:             "TBW102",
		Resolver:                   primitive.NewHttpResolver("DEFAULT"),
		CompressMsgBodyOverHowmuch: 4096,
		CompressLevel:              5,
	}
	opts.ClientOptions.GroupName = "DEFAULT_PRODUCER"
	return opts
}

type producerOptions struct {
	internal.ClientOptions
	Selector              QueueSelector
	SendMsgTimeout        time.Duration
	DefaultTopicQueueNums int
	CreateTopicKey        string // "TBW102" Will be created at broker when isAutoCreateTopicEnable. when topic is not created,
	// and broker open isAutoCreateTopicEnable, topic will use "TBW102" config to create topic
	Resolver                   primitive.NsResolver
	CompressMsgBodyOverHowmuch int
	CompressLevel              int
}

type Option func(*producerOptions)

// WithGroupName set group name address
func WithGroupName(group string) Option {
	return func(opts *producerOptions) {
		if group == "" {
			return
		}
		opts.GroupName = group
	}
}

func WithInstanceName(name string) Option {
	return func(opts *producerOptions) {
		opts.InstanceName = name
	}
}

// WithNamespace set the namespace of producer
func WithNamespace(namespace string) Option {
	return func(opts *producerOptions) {
		opts.Namespace = namespace
	}
}

func WithSendMsgTimeout(duration time.Duration) Option {
	return func(opts *producerOptions) {
		opts.SendMsgTimeout = duration
	}
}

func WithVIPChannel(enable bool) Option {
	return func(opts *producerOptions) {
		opts.VIPChannelEnabled = enable
	}
}

// WithRetry return a Option that specifies the retry times when send failed.
// TODO: use retry middleware instead
func WithRetry(retries int) Option {
	return func(opts *producerOptions) {
		opts.RetryTimes = retries
	}
}

func WithInterceptor(f ...primitive.Interceptor) Option {
	return func(opts *producerOptions) {
		opts.Interceptors = append(opts.Interceptors, f...)
	}
}

func WithQueueSelector(s QueueSelector) Option {
	return func(options *producerOptions) {
		options.Selector = s
	}
}

func WithCredentials(c primitive.Credentials) Option {
	return func(options *producerOptions) {
		options.ClientOptions.Credentials = c
	}
}

func WithDefaultTopicQueueNums(queueNum int) Option {
	return func(options *producerOptions) {
		options.DefaultTopicQueueNums = queueNum
	}
}

func WithCreateTopicKey(topic string) Option {
	return func(options *producerOptions) {
		options.CreateTopicKey = topic
	}
}

// WithNsResolver set nameserver resolver to fetch nameserver addr
func WithNsResolver(resolver primitive.NsResolver) Option {
	return func(options *producerOptions) {
		options.Resolver = resolver
	}
}

// WithNameServer set NameServer address, only support one NameServer cluster in alpha2
func WithNameServer(nameServers primitive.NamesrvAddr) Option {
	return func(options *producerOptions) {
		options.Resolver = primitive.NewPassthroughResolver(nameServers)
	}
}

// WithNameServerDomain set NameServer domain
func WithNameServerDomain(nameServerUrl string) Option {
	return func(opts *producerOptions) {
		opts.Resolver = primitive.NewHttpResolver("DEFAULT", nameServerUrl)
	}
}

// WithCompressMsgBodyOverHowmuch set compression threshold
func WithCompressMsgBodyOverHowmuch(threshold int) Option {
	return func(opts *producerOptions) {
		opts.CompressMsgBodyOverHowmuch = threshold
	}
}

// WithCompressLevel set compress level (0~9)
// 0 stands for best speed
// 9 stands for best compression ratio
func WithCompressLevel(level int) Option {
	return func(opts *producerOptions) {
		opts.CompressLevel = level
	}
}
