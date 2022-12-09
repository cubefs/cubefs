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
	"context"
)

// Invoker finish a message invoke on producer/consumer.
type Invoker func(ctx context.Context, req, reply interface{}) error

// Interceptor intercepts the invoke of a producer/consumer on messages.
// In PushConsumer call, the req is []*MessageExt type and the reply is ConsumeResultHolder,
// use type assert to get real type.
type Interceptor func(ctx context.Context, req, reply interface{}, next Invoker) error

func ChainInterceptors(interceptors ...Interceptor) Interceptor {
	if len(interceptors) == 0 {
		return nil
	}
	if len(interceptors) == 1 {
		return interceptors[0]
	}
	return func(ctx context.Context, req, reply interface{}, invoker Invoker) error {
		return interceptors[0](ctx, req, reply, getChainedInterceptor(interceptors, 0, invoker))
	}
}

func getChainedInterceptor(interceptors []Interceptor, cur int, finalInvoker Invoker) Invoker {
	if cur == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, req, reply interface{}) error {
		return interceptors[cur+1](ctx, req, reply, getChainedInterceptor(interceptors, cur+1, finalInvoker))
	}
}
