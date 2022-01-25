package access

import (
	"context"

	"github.com/cubefs/blobstore/common/trace"
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
//     an interface { TraceID() string },
//     an interface { ReqID() string },
//     an interface { ReqId() string },
//     an interface { String() string },
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
	if rid, ok := val.(interface{ TraceID() string }); ok {
		return rid.TraceID(), true
	}
	if rid, ok := val.(interface{ ReqID() string }); ok {
		return rid.ReqID(), true
	}
	if rid, ok := val.(interface{ ReqId() string }); ok {
		return rid.ReqId(), true
	}
	if rid, ok := val.(interface{ String() string }); ok {
		return rid.String(), true
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
