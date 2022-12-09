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

package consumer

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
	return func(options *consumerOptions) {

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
		consumerCtx, exist := primitive.GetConsumerCtx(ctx)
		if !exist || len(consumerCtx.Msgs) == 0 {
			return next(ctx, req, reply)
		}

		beginT := time.Now()
		// before traceCtx
		traceCx := internal.TraceContext{
			RequestId: primitive.CreateUniqID(),
			TimeStamp: time.Now().UnixNano() / int64(time.Millisecond),
			TraceType: internal.SubBefore,
			GroupName: consumerCtx.ConsumerGroup,
			IsSuccess: true,
		}
		beans := make([]internal.TraceBean, 0)
		for _, msg := range consumerCtx.Msgs {
			if msg == nil {
				continue
			}
			regionID := msg.GetRegionID()
			traceOn := msg.IsTraceOn()
			if traceOn == "false" {
				continue
			}
			bean := internal.TraceBean{
				Topic:      msg.Topic,
				MsgId:      msg.MsgId,
				Tags:       msg.GetTags(),
				Keys:       msg.GetKeys(),
				StoreTime:  msg.StoreTimestamp,
				BodyLength: int(msg.StoreSize),
				RetryTimes: int(msg.ReconsumeTimes),
				ClientHost: utils.LocalIP,
				StoreHost:  utils.LocalIP,
			}
			beans = append(beans, bean)
			traceCx.RegionId = regionID
		}
		if len(beans) > 0 {
			traceCx.TraceBeans = beans
			traceCx.TimeStamp = time.Now().UnixNano() / int64(time.Millisecond)
			dispatcher.Append(traceCx)
		}

		err := next(ctx, req, reply)

		// after traceCtx
		costTime := time.Since(beginT).Nanoseconds() / int64(time.Millisecond)
		ctxType := consumerCtx.Properties[primitive.PropCtxType]
		afterCtx := internal.TraceContext{
			TimeStamp: time.Now().UnixNano() / int64(time.Millisecond),

			TraceType:   internal.SubAfter,
			RegionId:    traceCx.RegionId,
			GroupName:   traceCx.GroupName,
			RequestId:   traceCx.RequestId,
			IsSuccess:   consumerCtx.Success,
			CostTime:    costTime,
			TraceBeans:  traceCx.TraceBeans,
			ContextCode: primitive.ConsumeReturnType(ctxType).Ordinal(),
		}
		dispatcher.Append(afterCtx)
		return err
	}
}
