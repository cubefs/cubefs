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

/**
 * builtin interceptor
 */
package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// WithTrace support rocketmq trace: https://github.com/apache/rocketmq/wiki/RIP-6-Message-Trace.
func WithTrace(traceCfg *primitive.TraceConfig) Option {
	return func(options *producerOptions) {

		ori := options.Interceptors
		options.Interceptors = make([]primitive.Interceptor, 0)
		options.Interceptors = append(options.Interceptors, newTraceInterceptor(traceCfg))
		options.Interceptors = append(options.Interceptors, ori...)
	}
}

func newTraceInterceptor(traceCfg *primitive.TraceConfig) primitive.Interceptor {
	dispatcher := internal.NewTraceDispatcher(traceCfg)
	if dispatcher != nil {
		dispatcher.Start()
	}

	return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
		if dispatcher == nil {
			return fmt.Errorf("GetOrNewRocketMQClient faild")
		}
		beginT := time.Now()
		err := next(ctx, req, reply)

		producerCtx := primitive.GetProducerCtx(ctx)
		if producerCtx.Message.Topic == dispatcher.GetTraceTopicName() {
			return err
		}

		// SendOneway && SendAsync has no reply.
		if reply == nil {
			return err
		}

		result := reply.(*primitive.SendResult)
		if result.RegionID == "" || !result.TraceOn {
			return err
		}

		sendSuccess := result.Status == primitive.SendOK
		costT := time.Since(beginT).Nanoseconds() / int64(time.Millisecond)
		storeT := beginT.UnixNano()/int64(time.Millisecond) + costT/2

		traceBean := internal.TraceBean{
			Topic:       producerCtx.Message.Topic,
			Tags:        producerCtx.Message.GetTags(),
			Keys:        producerCtx.Message.GetKeys(),
			StoreHost:   producerCtx.BrokerAddr,
			ClientHost:  utils.LocalIP,
			BodyLength:  len(producerCtx.Message.Body),
			MsgType:     producerCtx.MsgType,
			MsgId:       result.MsgID,
			OffsetMsgId: result.OffsetMsgID,
			StoreTime:   storeT,
		}

		traceCtx := internal.TraceContext{
			RequestId: primitive.CreateUniqID(), // set id
			TimeStamp: time.Now().UnixNano() / int64(time.Millisecond),

			TraceType:  internal.Pub,
			GroupName:  producerCtx.ProducerGroup,
			RegionId:   result.RegionID,
			TraceBeans: []internal.TraceBean{traceBean},
			CostTime:   costT,
			IsSuccess:  sendSuccess,
		}
		dispatcher.Append(traceCtx)
		return err
	}
}
