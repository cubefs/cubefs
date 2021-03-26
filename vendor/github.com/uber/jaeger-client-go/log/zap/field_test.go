// Copyright (c) 2017 Uber Technologies, Inc.
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
	"context"
	"testing"
	"time"

	jaeger "github.com/uber/jaeger-client-go"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestTraceField(t *testing.T) {
	assert.Equal(t, zap.Skip(), Trace(nil), "Expected Trace of a nil context to be a no-op.")

	withTracedContext(func(ctx context.Context) {
		enc := zapcore.NewMapObjectEncoder()
		Trace(ctx).AddTo(enc)

		logged, ok := enc.Fields["trace"].(map[string]interface{})
		require.True(t, ok, "Expected trace to be a map.")

		// We could extract the span from the context and assert specific IDs,
		// but that just copies the production code. Instead, just assert that
		// the keys we expect are present.
		keys := make(map[string]struct{}, len(logged))
		for k := range logged {
			keys[k] = struct{}{}
		}
		assert.Equal(
			t,
			map[string]struct{}{"span": {}, "trace": {}, "sampled": {}},
			keys,
			"Expected to log span and trace IDs.",
		)
	})
}

func withTracedContext(f func(ctx context.Context)) {
	tracer, closer := jaeger.NewTracer(
		"serviceName", jaeger.NewConstSampler(true), jaeger.NewNullReporter(),
	)
	defer closer.Close()

	ctx := opentracing.ContextWithSpan(context.Background(), tracer.StartSpan("test"))
	f(ctx)
}

func TestSpanField(t *testing.T) {
	tracer, closer := jaeger.NewTracer("serviceName",
		jaeger.NewConstSampler(true),
		jaeger.NewNullReporter(),
	)
	defer closer.Close()

	parentSpan := tracer.StartSpan("parent")
	parentSpanContext := parentSpan.Context().(jaeger.SpanContext)
	baggageKey := "Peregrin"
	baggageValue := "Took"
	parentSpan.SetBaggageItem(baggageKey, baggageValue)

	now := time.Now()
	// create a child span so that span refs are populated
	span := tracer.StartSpan("child",
		opentracing.ChildOf(parentSpan.Context()),
		opentracing.StartTime(now))
	spanContext := span.Context().(jaeger.SpanContext)

	// Enable Debug
	ext.SamplingPriority.Set(span, 1)

	// Enable Firehose
	jaeger.EnableFirehose(span.(*jaeger.Span))

	// Add a log
	span.Log(opentracing.LogData{
		Timestamp: now,
		Event:     "test",
	})

	enc := zapcore.NewMapObjectEncoder()
	Span(span).AddTo(enc)

	assert.Contains(t, enc.Fields, "span")
	logged, ok := enc.Fields["span"].(map[string]interface{})
	require.True(t, ok, "Expected map.")

	expected := map[string]interface{}{
		"context": map[string]interface{}{
			"trace":    spanContext.TraceID().String(),
			"span":     spanContext.SpanID().String(),
			"parent":   parentSpanContext.SpanID().String(),
			"debug":    true,
			"sampled":  true,
			"firehose": true,
			"baggage": []interface{}{
				map[string]interface{}{
					"key":   baggageKey,
					"value": baggageValue,
				},
			},
		},

		"operation_name": "child",
		"duration":       time.Duration(0),
		"start_time":     now,
		"tags": map[string]interface{}{
			"key":   "sampling.priority",
			"value": uint16(1),
		},

		"logs": []interface{}{
			map[string]interface{}{
				"ts": now,
				"fields": []interface{}{
					map[string]interface{}{
						"key":   "event",
						"value": "test",
					},
				},
			},
		},

		"span_refs": []interface{}{
			map[string]interface{}{
				"type":  "child_of",
				"span":  parentSpanContext.SpanID().String(),
				"trace": parentSpanContext.TraceID().String(),
			},
		},
	}
	assert.Equal(t, expected, logged)

}
