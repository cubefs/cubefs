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
	"errors"
	"hash/maphash"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	ptlog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestSpan_Tags(t *testing.T) {
	span, _ := StartSpanFromContext(context.Background(), "test tags")
	defer span.Finish()

	expectedTags := Tags{
		"module": "worker",
		"ip":     "127.0.0.1",
	}

	span.SetTag("module", "worker")
	span.SetTag("ip", "127.0.0.1")
	require.Equal(t, span.Tags(), expectedTags)
}

func TestSpan_Logs(t *testing.T) {
	span, _ := StartSpanFromContext(context.Background(), "test logs")
	defer span.Finish()

	expectedLogs := []struct {
		logs []ptlog.Field
	}{
		{logs: []ptlog.Field{ptlog.String("event", "success"), ptlog.Int("waited.millis", 20)}},
		{logs: []ptlog.Field{ptlog.String("event", "failed"), ptlog.Int("waited.millis", 1500)}},
	}

	for k, v := range expectedLogs {
		span.LogFields(v.logs...)
		require.Equal(t, expectedLogs[k].logs, span.Logs()[k].Fields)
	}
	require.Equal(t, 2, len(span.Logs()))

	fields := []ptlog.Field{ptlog.String("code", "200"), ptlog.Float32("count", 100)}
	for k, v := range fields {
		span.LogKV(v.Key(), v.Value())
		require.Equal(t, fields[k].Key(), span.Logs()[k+2].Fields[0].Key())
		require.Equal(t, fields[k].Value(), span.Logs()[k+2].Fields[0].Value())
	}
	require.Equal(t, 4, len(span.Logs()))

	span.LogKV("only key")
	require.Equal(t, 5, len(span.Logs()))
}

func TestSpan_OperationName(t *testing.T) {
	span, _ := StartSpanFromContext(context.Background(), "span")
	defer span.Finish()

	id := span.TraceID()
	{
		span := span.WithOperation("")
		require.Equal(t, id, span.TraceID())
		require.Equal(t, "span", span.OperationName())
	}
	require.Equal(t, "span", span.OperationName())
	span.SetOperationName("span2")
	require.Equal(t, id, span.TraceID())
	require.Equal(t, "span2", span.OperationName())
	{
		span := span.WithOperation("span3")
		require.Equal(t, id, span.TraceID())
		require.Equal(t, "span2:span3", span.OperationName())
	}
}

func TestSpan_WithOperation(t *testing.T) {
	rootSpan, ctx := StartSpanFromContext(context.Background(), "Operation")
	defer rootSpan.Finish()

	span := SpanFromContext(ctx)

	spanL1 := span.WithOperation("L1")
	require.Equal(t, "Operation:L1", spanL1.OperationName())
	spanL2 := spanL1.WithOperation("L2")
	spanL2.Info("span l2:", spanL2.String())
	spanL3 := spanL2.WithOperation("L3")
	spanL3.Warn("span l3:", spanL3.String())
	require.Equal(t, "Operation:L1:L2:L3", spanL3.OperationName())
	spanL2x := spanL1.WithOperation("L2x")
	spanL2x.Error("span l2x:", spanL2x.String())

	spanL1.SetOperationName("")
	require.Equal(t, "", spanL1.OperationName())
	require.Equal(t, rootSpan.String(), spanL1.String())

	{
		root := SpanFromContextSafe(context.Background())
		require.Equal(t, "", root.OperationName())

		span := root.WithOperation("")
		require.Equal(t, "", span.OperationName())
		span = span.WithOperation("span")
		require.Equal(t, "span", span.OperationName())
	}
}

func TestSpan_ContextOperation(t *testing.T) {
	root, ctx := StartSpanFromContext(context.Background(), "Context")
	defer root.Finish()

	id := root.TraceID()
	require.Equal(t, id, root.TraceID())

	newCtx := ContextWithSpan(ctx, root.WithOperation("L1").WithOperation("L2"))
	span, _ := StartSpanFromContextWithTraceID(newCtx, "opt", "traceid")

	require.Equal(t, id, root.TraceID())
	require.Equal(t, "traceid", span.TraceID())
	require.Equal(t, "opt", span.OperationName())

	span = SpanFromContext(newCtx)
	require.Equal(t, id, root.TraceID())
	require.Equal(t, id, span.TraceID())
	require.Equal(t, "Context:L1:L2", span.OperationName())
}

func TestSpan_Baggage(t *testing.T) {
	span, ctx := StartSpanFromContext(context.Background(), "test baggage")
	defer span.Finish()

	baggages := []struct {
		k string
		v string
	}{
		{k: "k1", v: "v1"},
		{k: "k2", v: "v2"},
		{k: "k3", v: "v3"},
	}
	for _, v := range baggages {
		span.SetBaggageItem(v.k, v.v)
		require.Equal(t, v.v, span.BaggageItem(v.k))
	}

	spanChild, _ := StartSpanFromContext(ctx, "child of span")
	for _, v := range baggages {
		require.Equal(t, v.v, spanChild.BaggageItem(v.k))
	}

	spanChild.SetBaggageItem("k4", "v4")
	require.Equal(t, "v4", spanChild.BaggageItem("k4"))
	require.Equal(t, "v4", span.BaggageItem("k4"))
}

func TestSpan_TrackLog(t *testing.T) {
	span, ctx := StartSpanFromContext(context.Background(), "test trackLog")
	defer span.Finish()

	span.AppendTrackLog("sleep", time.Now(), nil)
	require.Equal(t, []string{"sleep"}, span.TrackLog())

	spanChild, _ := StartSpanFromContext(ctx, "child of span")
	require.Equal(t, []string{"sleep"}, spanChild.TrackLog())

	spanChild.AppendTrackLog("sleep2", time.Now(), errors.New("sleep2 err"))
	require.Equal(t, []string{"sleep", "sleep2/sleep2 err"}, spanChild.TrackLog())
	require.Equal(t, []string{"sleep", "sleep2/sleep2 err"}, span.TrackLog())

	spanChild.AppendRPCTrackLog([]string{"blobnode:4", "scheduler:5"})
	require.Equal(t, []string{"sleep", "sleep2/sleep2 err", "blobnode:4", "scheduler:5"}, spanChild.TrackLog())
	require.Equal(t, []string{"sleep", "sleep2/sleep2 err", "blobnode:4", "scheduler:5"}, span.TrackLog())

	spanChild.AppendTrackLog("sleep3", time.Now(), nil)
	require.Equal(t, []string{"sleep", "sleep2/sleep2 err", "blobnode:4", "scheduler:5", "sleep3"}, span.TrackLog())

	msg := make([]byte, maxErrorLen+10)
	for idx := range msg {
		msg[idx] = 97
	}
	spanChild.AppendTrackLog("longError", time.Now(), errors.New(string(msg)))
	except := "longError/" + string(msg[:maxErrorLen])
	require.Equal(t, except, span.TrackLog()[len(span.TrackLog())-1])
}

func TestSpan_TrackLogWithDuration(t *testing.T) {
	span, ctx := StartSpanFromContext(context.Background(), "test trackLog")
	defer span.Finish()

	span.AppendTrackLogWithDuration("sleep", time.Millisecond, nil)
	require.Equal(t, []string{"sleep:1"}, span.TrackLog())

	spanChild, _ := StartSpanFromContext(ctx, "child of span")
	require.Equal(t, []string{"sleep:1"}, spanChild.TrackLog())

	spanChild.AppendTrackLogWithDuration("sleep2", 2*time.Millisecond, errors.New("sleep2 err"))
	require.Equal(t, []string{"sleep:1", "sleep2:2/sleep2 err"}, spanChild.TrackLog())
	require.Equal(t, []string{"sleep:1", "sleep2:2/sleep2 err"}, span.TrackLog())

	spanChild.AppendRPCTrackLog([]string{"blobnode:4", "scheduler:5"})
	require.Equal(t, []string{"sleep:1", "sleep2:2/sleep2 err", "blobnode:4", "scheduler:5"}, spanChild.TrackLog())
	require.Equal(t, []string{"sleep:1", "sleep2:2/sleep2 err", "blobnode:4", "scheduler:5"}, span.TrackLog())

	spanChild.AppendTrackLogWithDuration("sleep3", 3*time.Millisecond, nil)
	require.Equal(t, []string{"sleep:1", "sleep2:2/sleep2 err", "blobnode:4", "scheduler:5", "sleep3:3"}, span.TrackLog())
}

func TestSpan_TrackLogWithFunc(t *testing.T) {
	span, ctx := StartSpanFromContext(context.Background(), "test trackLog")
	defer span.Finish()

	span.AppendTrackLogWithFunc("sleep", func() error { time.Sleep(time.Millisecond); return nil })
	require.Equal(t, 1, len(span.TrackLog()))

	spanChild, _ := StartSpanFromContext(ctx, "child of span")
	require.Equal(t, 1, len(spanChild.TrackLog()))

	spanChild.AppendTrackLogWithFunc("sleep2", func() error { return errors.New("sleep2 err") })
	require.Equal(t, 2, len(spanChild.TrackLog()))
	require.Equal(t, 2, len(span.TrackLog()))

	spanChild.AppendRPCTrackLog([]string{"blobnode:4", "scheduler:5"})
	require.Equal(t, 4, len(spanChild.TrackLog()))
	require.Equal(t, 4, len(span.TrackLog()))

	spanChild.AppendTrackLogWithFunc("sleep3", func() error { return nil })
	require.Equal(t, 5, len(span.TrackLog()))
}

func TestSpan_TrackLogWithOption(t *testing.T) {
	span, _ := StartSpanFromContext(context.Background(), "test trackLog options")
	defer span.Finish()

	duration := time.Hour + 2*time.Minute + 3*time.Second + 4*time.Millisecond + 5*time.Microsecond + 6
	err := errors.New("err length")
	for _, f := range []func() SpanOption{
		OptSpanDurationAny,
		OptSpanDurationNs,
		OptSpanDurationUs,
		OptSpanDurationMs,
		OptSpanDurationSecond,
		OptSpanDurationMinute,
		OptSpanDurationHour,
	} {
		span.AppendTrackLogWithDuration("m", duration, nil, f(), OptSpanDurationUnit())
		t.Log(span.TrackLog())
	}
	span.AppendTrackLogWithDuration("e", duration, err, OptSpanDurationNone(), OptSpanErrorLength(3))
	t.Log(span.TrackLog())
	span.AppendTrackLogWithDuration("em", duration, err, OptSpanDurationAny(), OptSpanErrorLength(3))
	t.Log(span.TrackLog())
}

func TestSpan_BaseLogger(t *testing.T) {
	originLevel := log.GetOutputLevel()
	defer log.SetOutputLevel(originLevel)
	rootSpan, ctx := StartSpanFromContext(context.Background(), "test baseLogger")
	defer rootSpan.Finish()

	logLevel := []log.Level{
		log.Ldebug,
		log.Linfo,
		log.Lwarn,
		log.Lerror,
		log.Lpanic,
		log.Lfatal,
	}

	for _, level := range logLevel {
		rootSpan.Infof("set log level, level: %d", level)
		log.SetOutputLevel(level)

		span, _ := StartSpanFromContext(ctx, "test baseLogger")

		span.Println("println")
		span.Printf("%s", "printf")

		span.Debug("span info:", span.String())
		span.Debugf("spanContent info: %+v ,traceID: %s", span.Context(), span.TraceID())

		span.Info("service name", span.Tracer().(*Tracer).serviceName)
		span.Infof("start span success, name: %s", span.OperationName())

		span.Warn("get spanID")
		span.Warnf("spanID: %d", span.Context().(*SpanContext).spanID)

		ctx := context.Background()
		if spanNil := SpanFromContext(ctx); spanNil == nil {
			span.Error("SpanFromContext failed")
			span.Errorf("ctx: %+v, span: %+v", ctx, spanNil)
		}

		require.Panics(t, func() {
			if level%2 == 0 {
				span.Panic("panic on span", span)
			} else {
				span.Panicf("panic on span: %p", span.Context().(*SpanContext))
			}
		})
		span.Finish()

		{
			span := rootSpan.WithOperation("with-operation")

			span.Println("println")
			span.Printf("%s", "printf")
			span.Debug("operation span info:", span.String())
			span.Debugf("spanContent info: %+v ,traceID: %s", span.Context(), span.TraceID())
			span.Info("operation service name", span.Tracer().(*Tracer).serviceName)
			span.Infof("operation span success, name: %s", span.OperationName())
			span.Warn("get spanID")
			span.Warnf("spanID: %d", span.Context().(*SpanContext).spanID)
			span.Error("SpanFromContext failed")
			span.Errorf("operationName: %s", span.OperationName())

			require.Panics(t, func() {
				if level%2 == 0 {
					span.Panic("panic on span", span)
				} else {
					span.Panicf("panic on span: %p", span.Context().(*SpanContext))
				}
			})
			span.Finish()
		}
	}
}

func TestSpan_FinishWithOptions(t *testing.T) {
	span, _ := StartSpanFromContext(context.Background(), "test baseLogger")

	span.FinishWithOptions(opentracing.FinishOptions{
		LogRecords: []opentracing.LogRecord{
			{Timestamp: time.Now()},
			{Timestamp: time.Now()},
		},
		BulkLogData: []opentracing.LogData{
			{Timestamp: time.Now()},
		},
	})
}

func Benchmark_RandomID_RandLock(b *testing.B) {
	var l sync.Mutex
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for ii := 0; ii < b.N; ii++ {
		l.Lock()
		r.Uint64()
		l.Unlock()
	}
}

func Benchmark_RandomID_Maphash(b *testing.B) {
	for ii := 0; ii < b.N; ii++ {
		new(maphash.Hash).Sum64()
	}
}

func Benchmark_Span_TrackLog(b *testing.B) {
	span, _ := StartSpanFromContext(context.Background(), "")
	module := "m"
	duration := time.Minute + time.Second + time.Millisecond*3
	err := errors.New("loooooooooooooooong length")
	b.ResetTimer()
	b.Run("duration-any", func(b *testing.B) {
		for ii := 0; ii < b.N; ii++ {
			span.AppendTrackLogWithDuration(module, duration, nil, OptSpanDurationAny())
		}
	})
	b.Run("duration-second", func(b *testing.B) {
		for ii := 0; ii < b.N; ii++ {
			span.AppendTrackLogWithDuration(module, duration, nil, OptSpanDurationSecond())
		}
	})
	b.Run("duration-error", func(b *testing.B) {
		for ii := 0; ii < b.N; ii++ {
			span.AppendTrackLogWithDuration(module, duration, err, OptSpanErrorLength(13))
		}
	})
}
