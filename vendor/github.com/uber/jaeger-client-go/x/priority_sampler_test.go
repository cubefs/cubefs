// Copyright (c) 2019 The Jaeger Authors.
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

package x

import (
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-client-go"
)

func makePrioritySampler(t *testing.T) *PrioritySampler {
	tagSampler := NewTagMatchingSampler("theWho", []TagMatcher{
		{TagValue: "Bender", Firehose: false},
		{TagValue: "Leela", Firehose: true},
	})

	return NewPrioritySampler(tagSampler, jaeger.NewConstSampler(false))
}

func TestPrioritySamplerShouldNotSampleOrFinalize(t *testing.T) {
	tracer, closer := jaeger.NewTracer("svc", makePrioritySampler(t), jaeger.NewNullReporter())
	defer closer.Close()

	span := tracer.StartSpan("op1")
	assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.False(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())

	span.SetOperationName("op2")
	assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.False(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())

	span.Finish()
	assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.False(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())
}

func TestPrioritySampler(t *testing.T) {
	tracer, closer := jaeger.NewTracer("svc", makePrioritySampler(t), jaeger.NewNullReporter())
	defer closer.Close()

	// tags at span creation
	span := tracer.StartSpan("op1", opentracing.Tags{"theWho": "Bender"})
	assert.True(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.True(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())

	// tags after span creation
	span = tracer.StartSpan("op1")
	assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.False(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())
	span.SetTag("theWho", "Bender")
	assert.True(t, span.Context().(jaeger.SpanContext).IsSampled())
	assert.True(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())
}

func TestPrioritySamplerFirehose(t *testing.T) {
	tracer, closer := jaeger.NewTracer("svc", makePrioritySampler(t), jaeger.NewNullReporter())
	defer closer.Close()

	tests := []struct {
		tagValue       string
		expectFirehose bool
	}{
		{tagValue: "Bender", expectFirehose: false},
		{tagValue: "Leela", expectFirehose: true},
	}

	for _, test := range tests {
		// tags at span creation
		span := tracer.StartSpan("op1", opentracing.Tags{"theWho": test.tagValue})
		assert.True(t, span.Context().(jaeger.SpanContext).IsSampled())
		assert.True(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())
		assert.Equal(t, test.expectFirehose, span.Context().(jaeger.SpanContext).IsFirehose())
	}
}
