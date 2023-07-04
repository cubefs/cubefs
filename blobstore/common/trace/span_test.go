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
		{
			logs: []ptlog.Field{ptlog.String("event", "success"), ptlog.Int("waited.millis", 20)},
		},
		{
			logs: []ptlog.Field{ptlog.String("event", "failed"), ptlog.Int("waited.millis", 1500)},
		},
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

	require.Equal(t, "span", span.OperationName())
	span.SetOperationName("span2")
	require.Equal(t, "span2", span.OperationName())
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
