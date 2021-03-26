// Copyright (c) 2020 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/log/zap/mock_opentracing"
)

func TestTracerDelegation(t *testing.T) {
	logObserver, logs := observer.New(zap.DebugLevel)
	logger := zap.New(logObserver)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := mock_opentracing.NewMockTracer(ctrl)
	mockSpan := mock_opentracing.NewMockSpan(ctrl)
	mockSpanContext := mock_opentracing.NewMockSpanContext(ctrl)

	newLoggingTracer := NewLoggingTracer(logger, mockTracer)

	mockTracer.EXPECT().StartSpan("someString").Return(mockSpan)
	newLoggingTracer.StartSpan("someString")
	assert.Len(t, logs.FilterMessage("StartSpan").TakeAll(), 1)

	mockTracer.EXPECT().Inject(mockSpanContext, "format", "carrier").Return(nil)
	err := newLoggingTracer.Inject(mockSpanContext, "format", "carrier")
	assert.NoError(t, err)
	assert.Len(t, logs.FilterMessage("Inject").TakeAll(), 1)

	mockTracer.EXPECT().Extract("format", "carrier").Return(mockSpanContext, nil)
	sc, err := newLoggingTracer.Extract("format", "carrier")
	assert.NoError(t, err)
	assert.Equal(t, sc, mockSpanContext)
	assert.Len(t, logs.FilterMessage("Extract").TakeAll(), 1)
}

func TestSpanDelegation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := mock_opentracing.NewMockTracer(ctrl)
	mockSpan := mock_opentracing.NewMockSpan(ctrl)
	mockSpanContext := mock_opentracing.NewMockSpanContext(ctrl)

	logObserver, logs := observer.New(zap.DebugLevel)
	logger := zap.New(logObserver)
	loggingSpan := newLoggingSpan(logger, mockSpan)

	mockSpan.EXPECT().Finish()
	loggingSpan.Finish()
	assert.Len(t, logs.FilterMessage("Finish").TakeAll(), 1)

	finishOpts := opentracing.FinishOptions{
		FinishTime: time.Now(),
	}
	mockSpan.EXPECT().FinishWithOptions(finishOpts)
	loggingSpan.FinishWithOptions(finishOpts)
	assert.Len(t, logs.FilterMessage("FinishWithOptions").TakeAll(), 1)

	mockSpan.EXPECT().Context().Return(mockSpanContext)
	loggingSpan.Context()
	assert.Len(t, logs.FilterMessage("Context").TakeAll(), 1)

	mockSpan.EXPECT().SetOperationName("operation_name")
	loggingSpan.SetOperationName("operation_name")
	assert.Len(t, logs.FilterMessage("SetOperationName").TakeAll(), 1)

	mockSpan.EXPECT().SetTag("k", "v")
	loggingSpan.SetTag("k", "v")
	assert.Len(t, logs.FilterMessage("SetTag").TakeAll(), 1)

	logField := log.String("k", "v")
	mockSpan.EXPECT().LogFields(logField)
	loggingSpan.LogFields(logField)
	assert.Len(t, logs.FilterMessage("LogFields").TakeAll(), 1)

	mockSpan.EXPECT().LogKV("k", "v")
	loggingSpan.LogKV("k", "v")
	assert.Len(t, logs.FilterMessage("LogKV").TakeAll(), 1)

	mockSpan.EXPECT().SetBaggageItem("k", "v")
	loggingSpan.SetBaggageItem("k", "v")
	assert.Len(t, logs.FilterMessage("SetBaggageItem").TakeAll(), 1)

	mockSpan.EXPECT().BaggageItem("k").Return("v")
	value := loggingSpan.BaggageItem("k")
	assert.Equal(t, "v", value)
	assert.Len(t, logs.FilterMessage("BaggageItem").TakeAll(), 1)

	mockSpan.EXPECT().Tracer().Return(mockTracer)
	tracer := loggingSpan.Tracer()
	assert.Equal(t, mockTracer, tracer)
	assert.Len(t, logs.FilterMessage("Tracer").TakeAll(), 1)

	mockSpan.EXPECT().LogEvent("event")
	loggingSpan.LogEvent("event")
	assert.Len(t, logs.FilterMessage("Deprecated: LogEvent").TakeAll(), 1)

	mockSpan.EXPECT().LogEventWithPayload("event", "payload")
	loggingSpan.LogEventWithPayload("event", "payload")
	assert.Len(t, logs.FilterMessage("Deprecated: LogEventWithPayload").TakeAll(), 1)

	logData := opentracing.LogData{
		Event:   "event",
		Payload: "payload",
	}
	mockSpan.EXPECT().Log(logData)
	loggingSpan.Log(logData)
	assert.Len(t, logs.FilterMessage("Deprecated: Log").TakeAll(), 1)
}

func TestSpanStart_Finish(t *testing.T) {
	// Test using a jaeger tracer and verify that the `span` and `context` keys
	// are populated
	tracer, closer := jaeger.NewTracer("test",
		jaeger.NewConstSampler(true),
		jaeger.NewNullReporter())
	defer closer.Close()

	logObserver, logs := observer.New(zap.DebugLevel)
	logger := zap.New(logObserver)

	loggingTracer := NewLoggingTracer(logger, tracer)
	span := loggingTracer.StartSpan("operation")

	startSpanLogs := logs.FilterMessage("StartSpan").TakeAll()
	assert.Len(t, startSpanLogs, 1)

	fields := startSpanLogs[0].ContextMap()
	assert.Contains(t, fields, "context")

	span.Finish()
	finishSpanLogs := logs.FilterMessage("Finish").TakeAll()
	assert.Len(t, finishSpanLogs, 1)
	fields = finishSpanLogs[0].ContextMap()
	assert.Contains(t, fields, "span")
}
