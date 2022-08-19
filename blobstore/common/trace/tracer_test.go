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

package trace

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

func TestExplicitStartTime(t *testing.T) {
	tracer := NewTracer("blobstore")
	defer tracer.Close()

	start := time.Now()

	span := tracer.StartSpan("testStartTime", StartTime(start))
	defer span.Finish()

	require.Equal(t, start, span.(*spanImpl).startTime)
}

func TestExplicitTags(t *testing.T) {
	tracer := NewTracer("blobstore")
	defer tracer.Close()

	tags := Tags{
		"tag1K": "tag1V",
		"tag2K": "tag2V",
	}

	span1 := tracer.StartSpan("testTags", tags)
	defer span1.Finish()

	require.Equal(t, tags, span1.(*spanImpl).tags)

	tag1 := Tag{
		Key:   "tag1K",
		Value: "tag1V",
	}
	span2 := tracer.StartSpan("testTag", tag1).(Span)
	defer span2.Finish()

	expect := Tags{
		"tag1K": "tag1V",
	}
	require.Equal(t, expect, span2.(*spanImpl).tags)

	tag2 := Tag{
		Key:   "tag2K",
		Value: "tag2V",
	}
	tag2.Set(span2)
	require.Equal(t, tags, span2.(*spanImpl).tags)
}

func TestExplicitReferences(t *testing.T) {
	tracer := NewTracer("blobstore")
	defer tracer.Close()

	parentSpan := tracer.StartSpan("parent").(*spanImpl)
	defer parentSpan.Finish()

	span1 := tracer.StartSpan("child", ChildOf(parentSpan.Context())).(*spanImpl)
	defer span1.Finish()

	require.Equal(t, 1, len(span1.references))
	require.Equal(t, parentSpan.context.traceID, span1.context.traceID)
	require.Equal(t, parentSpan.context.spanID, span1.context.parentID)

	ctx := ContextWithSpan(context.Background(), span1)
	span2 := SpanFromContext(ctx).(*spanImpl)

	span3, _ := StartSpanFromContext(ctx, "child of span")
	cs := span3.(*spanImpl)

	require.Equal(t, span1, span2)
	require.Equal(t, 1, len(cs.references))
	require.Equal(t, span1.context.traceID, cs.context.traceID)
	require.Equal(t, span1.context.spanID, cs.context.parentID)

	newParentSpan := tracer.StartSpan("newParentSpan")
	span4 := tracer.StartSpan("nChild", FollowsFrom(span3.Context()),
		FollowsFrom(newParentSpan.Context())).(*spanImpl)

	require.Equal(t, 2, len(span4.references))

	span5 := tracer.StartSpan("empty span context", ChildOf(&SpanContext{})).(*spanImpl)
	require.Equal(t, 0, len(span5.references))
}

func TestStartSpanFromContext(t *testing.T) {
	span, ctx := StartSpanFromContext(context.Background(), "span1")
	defer span.Finish()

	require.NotNil(t, SpanFromContext(ctx))
	s := span.(*spanImpl)

	childSpan, _ := StartSpanFromContext(ctx, "child span")
	cs := childSpan.(*spanImpl)
	require.Equal(t, 1, len(cs.references))
	require.Equal(t, s.context.traceID, cs.context.traceID)
	require.Equal(t, s.context.spanID, cs.context.parentID)

	// root span
	traceID := "traceID"
	traceID2 := "traceID2"
	rootSpan1, _ := StartSpanFromContextWithTraceID(context.Background(), "root span1", traceID)
	rootSpan2, _ := StartSpanFromContextWithTraceID(ctx, "root span2", traceID2)
	rs1 := rootSpan1.(*spanImpl)
	rs2 := rootSpan2.(*spanImpl)

	require.Equal(t, traceID, rs1.context.traceID)
	require.NotEqual(t, traceID, rs2.context.traceID)
}

func TestSpanFromContext(t *testing.T) {
	ctx := context.Background()
	require.Nil(t, SpanFromContext(ctx))

	span, ctx := StartSpanFromContext(ctx, "span1")
	defer span.Finish()

	require.NotNil(t, SpanFromContext(ctx))

	spanSafe := SpanFromContextSafe(context.Background())
	defer spanSafe.Finish()

	s := spanSafe.(*spanImpl)
	require.Equal(t, defaultRootSpanName, s.operationName)

	spanCopy := SpanFromContextSafe(ctx)

	sc := spanCopy.(*spanImpl)
	require.Equal(t, span.OperationName(), sc.OperationName())
}

func TestStartSpanFromHTTPHeaderSafe(t *testing.T) {
	r := &http.Request{Header: http.Header{}}
	traceID := "test"
	span, _ := StartSpanFromHTTPHeaderSafe(r, "http")
	require.NotEqual(t, traceID, span.Context().(*SpanContext).traceID)

	r.Header.Set(reqidKey, traceID)
	span, _ = StartSpanFromHTTPHeaderSafe(r, "http")
	require.Equal(t, traceID, span.Context().(*SpanContext).traceID)
}

func TestNewTracer(t *testing.T) {
	tracer := NewTracer("blobstore")
	require.Equal(t, defaultMaxLogsPerSpan, tracer.options.maxLogsPerSpan)
	tracer.Close()

	tracer = NewTracer("blobstore", TracerOptions.MaxLogsPerSpan(10))
	require.Equal(t, 10, tracer.options.maxLogsPerSpan)
	tracer.Close()
}

func TestCloseGlobalTracer(t *testing.T) {
	noopTracer := opentracing.NoopTracer{}
	opentracing.SetGlobalTracer(noopTracer)
	CloseGlobalTracer()

	tracer := NewTracer("blobstore")
	SetGlobalTracer(tracer)
	CloseGlobalTracer()
}

func TestInject(t *testing.T) {
	r := &http.Request{Header: http.Header{}}
	err := InjectWithHTTPHeader(context.Background(), r)
	require.NoError(t, err)
	span1, _ := StartSpanFromHTTPHeaderSafe(r, "span1")
	firstUpper := func(s string) string {
		if s == "" {
			return ""
		}
		return strings.ToUpper(s[:1]) + s[1:]
	}
	require.Equal(t, r.Header.Get(firstUpper(fieldKeyTraceID)), span1.Context().(*SpanContext).traceID)

	span2, ctx := StartSpanFromContext(context.Background(), "span2")
	r = &http.Request{Header: http.Header{}}
	err = InjectWithHTTPHeader(ctx, r)
	require.NoError(t, err)

	span3, _ := StartSpanFromHTTPHeaderSafe(r, "span3")
	require.Equal(t, r.Header.Get(firstUpper(fieldKeyTraceID)), span3.Context().(*SpanContext).traceID)
	require.Equal(t, span2.Context().(*SpanContext).traceID, span3.Context().(*SpanContext).traceID)
}
