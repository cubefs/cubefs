// Copyright 2022 The CubeFS Authors.
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

package access

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type ctxKey uint8

const (
	_operationName = "access_client"
)

const (
	_ ctxKey = iota
	reqidKey
)

// WithRequestID trace request id in full life of the request
// The second parameter rid could be the one of type below:
//     a string,
//     an interface { String() string },
//     an interface { TraceID() string },
//     an interface { RequestID() string },
func WithRequestID(ctx context.Context, rid interface{}) context.Context {
	return context.WithValue(ctx, reqidKey, rid)
}

func reqidFromContext(ctx context.Context) (string, bool) {
	val := ctx.Value(reqidKey)
	if val == nil {
		return "", false
	}
	if rid, ok := val.(string); ok {
		return rid, true
	}
	if rid, ok := val.(interface{ String() string }); ok {
		return rid.String(), true
	}
	if rid, ok := val.(interface{ TraceID() string }); ok {
		return rid.TraceID(), true
	}
	if rid, ok := val.(interface{ RequestID() string }); ok {
		return rid.RequestID(), true
	}
	return "", false
}

func withReqidContext(ctx context.Context) context.Context {
	if rid, ok := reqidFromContext(ctx); ok {
		_, ctx := trace.StartSpanFromContextWithTraceID(ctx, _operationName, rid)
		return ctx
	}
	if span := trace.SpanFromContext(ctx); span != nil {
		return ctx
	}
	_, ctx = trace.StartSpanFromContext(ctx, _operationName)
	return ctx
}
