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

package jaeger

import (
	"fmt"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ Sampler   = new(SamplerV2Base)
	_ SamplerV2 = new(ConstSampler)
	_ SamplerV2 = new(ProbabilisticSampler)
	_ SamplerV2 = new(RateLimitingSampler)
	_ SamplerV2 = new(PerOperationSampler)
	// Note: GuaranteedThroughputProbabilisticSampler is currently not V2

	_ Sampler   = new(retryableTestSampler)
	_ SamplerV2 = new(retryableTestSampler)
)

// retryableTestSampler leaves the sampling unfinalized (retryable) after OnCreateSpan,
// and makes it final after OnSetOperationName.
type retryableTestSampler struct {
	SamplerV2Base
	decision bool
	tags     []Tag
}

func newRetryableSampler(decision bool) *retryableTestSampler {
	return &retryableTestSampler{
		decision: decision,
		tags:     makeSamplerTags("retryable", decision),
	}
}

func (s *retryableTestSampler) OnCreateSpan(span *Span) SamplingDecision {
	return SamplingDecision{Sample: s.decision, Retryable: true, Tags: s.tags}
}

func (s *retryableTestSampler) OnSetOperationName(span *Span, operationName string) SamplingDecision {
	return SamplingDecision{Sample: s.decision, Retryable: false, Tags: s.tags}
}

func (s *retryableTestSampler) OnSetTag(span *Span, key string, value interface{}) SamplingDecision {
	return SamplingDecision{Sample: s.decision, Retryable: true, Tags: s.tags}
}

func (s *retryableTestSampler) OnFinishSpan(span *Span) SamplingDecision {
	return SamplingDecision{Sample: s.decision, Retryable: true, Tags: s.tags}
}

func TestSpanRemainsWriteable(t *testing.T) {
	tracer, closer := NewTracer("test-service", newRetryableSampler(false), NewNullReporter())
	defer closer.Close()

	span := tracer.StartSpan("op1").(*Span)
	assert.True(t, span.context.isWriteable(), "span is writeable when created")
	assert.False(t, span.context.isSamplingFinalized(), "span is not finalized when created")

	span.SetTag("k1", "v1")
	assert.True(t, span.context.isWriteable(), "span is writeable after setting tags")
	assert.Equal(t, opentracing.Tags{"k1": "v1"}, span.Tags())

	span.SetOperationName("op2")
	assert.False(t, span.context.isWriteable(), "span is not writeable after sampler returns retryable=true")
	assert.True(t, span.context.isSamplingFinalized(), "span is finalized after sampler returns retryable=true")
}

func TestSpanSharesSamplingStateWithChildren(t *testing.T) {
	tracer, closer := NewTracer("test-service", newRetryableSampler(false), NewNullReporter())
	defer closer.Close()

	sp1 := tracer.StartSpan("op1").(*Span)
	assert.False(t, sp1.context.isSamplingFinalized(), "span is not finalized when created")

	sp2 := tracer.StartSpan("op2", opentracing.ChildOf(sp1.Context())).(*Span)
	assert.False(t, sp2.context.isSamplingFinalized(), "child span is not finalized when created")

	sp2.SetOperationName("op3")
	assert.True(t, sp2.context.isSamplingFinalized(), "child span is finalized after changing operation name")
	assert.True(t, sp1.context.isSamplingFinalized(), "parent span is also finalized after child is finalized")
}

func TestSamplingIsFinalizedOnSamplingPriorityTag(t *testing.T) {
	tracer, closer := NewTracer("test-service", newRetryableSampler(false), NewNullReporter())
	defer closer.Close()

	tests := []struct {
		name            string
		priority        uint16
		expectedSampled bool
		expectedTags    opentracing.Tags
	}{
		{
			name:            "sampling.priority=1",
			priority:        1,
			expectedSampled: true,
			expectedTags:    opentracing.Tags{"sampling.priority": uint16(1)},
		},
		{
			name:            "sampling.priority=0",
			expectedSampled: false,
			priority:        0,
			expectedTags:    opentracing.Tags{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := tracer.StartSpan("op1").(*Span)
			assert.False(t, span.context.isSamplingFinalized(), "span is not finalized when created")

			ext.SamplingPriority.Set(span, tt.priority)
			assert.True(t, span.context.isSamplingFinalized(), "span is finalized after sampling.priority tag")
			assert.Equal(t, tt.expectedSampled, span.context.IsSampled(), "span is sampled after sampling.priority tag")
			assert.Equal(t, tt.expectedTags, span.Tags(), "sampling.priority tag in the span")
		})
	}
}

func TestSamplingIsFinalizedWithV1Samplers(t *testing.T) {
	probabilistic, err := NewProbabilisticSampler(0.5)
	require.NoError(t, err)
	samplers := []Sampler{
		NewConstSampler(false),
		probabilistic,
		NewRateLimitingSampler(1),
	}
	for _, sampler := range samplers {
		t.Run(fmt.Sprintf("%s", sampler), func(t *testing.T) {
			tracer, closer := NewTracer("test-service", sampler, NewNullReporter())
			defer closer.Close()

			span := tracer.StartSpan("op1").(*Span)
			assert.True(t, span.context.isSamplingFinalized(), "span is finalized when created with V1 sampler")
		})
	}
}

func TestSamplingIsNotFinalizedWhenContextIsInjected(t *testing.T) {
	tracer, closer := NewTracer("test-service", newRetryableSampler(false), NewNullReporter())
	defer closer.Close()

	span := tracer.StartSpan("op1").(*Span)
	assert.False(t, span.context.isSamplingFinalized(), "span is not finalized when created")

	err := tracer.Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier{})
	require.NoError(t, err)

	assert.False(t, span.context.isSamplingFinalized(), "span is not finalized after context is serialized")
}

func TestSamplingIsFinalizedInChildSpanOfRemoteParent(t *testing.T) {
	for _, sampled := range []bool{false, true} {
		t.Run(fmt.Sprintf("sampled=%t", sampled), func(t *testing.T) {
			tracer, closer := NewTracer("test-service", newRetryableSampler(sampled), NewNullReporter())
			defer closer.Close()

			span := tracer.StartSpan("parent").(*Span)
			assert.False(t, span.context.isSamplingFinalized(), "span is not finalized when created")
			assert.Equal(t, sampled, span.context.IsSampled())

			carrier := opentracing.TextMapCarrier{}
			err := tracer.Inject(span.Context(), opentracing.TextMap, carrier)
			require.NoError(t, err)

			parentContext, err := tracer.Extract(opentracing.TextMap, carrier)
			require.NoError(t, err)
			childSpan := tracer.StartSpan("child", opentracing.ChildOf(parentContext)).(*Span)
			assert.True(t, childSpan.context.isSamplingFinalized(), "child span is finalized")
			assert.Equal(t, sampled, childSpan.context.IsSampled())
		})
	}
}

func TestSpanIsWriteableIfSampledOrNotFinalized(t *testing.T) {
	tracer, closer := NewTracer("test-service", newRetryableSampler(false), NewNullReporter())
	defer closer.Close()

	span := tracer.StartSpan("span").(*Span)
	assert.False(t, span.context.isSamplingFinalized(), "span is not finalized when created")
	assert.False(t, span.context.IsSampled(), "span is not sampled")
	assert.True(t, span.context.isWriteable(), "span is writeable")

	tracer.(*Tracer).sampler = NewConstSampler(true)
	span = tracer.StartSpan("span").(*Span)
	assert.True(t, span.context.isSamplingFinalized(), "span is finalized when created")
	assert.True(t, span.context.IsSampled(), "span is sampled")
	assert.True(t, span.context.isWriteable(), "span is writeable")
}

func TestSetOperationOverridesOperationOnSampledSpan(t *testing.T) {
	tracer, closer := NewTracer("test-service", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	span := tracer.StartSpan("op1").(*Span)
	assert.True(t, span.context.isSamplingFinalized(), "span is finalized when created")
	assert.True(t, span.context.IsSampled(), "span is sampled")
	assert.Equal(t, "op1", span.OperationName())
	assert.Equal(t, makeSamplerTags("const", true), span.tags)

	span.tags = []Tag{} // easier to check that tags are not re-added
	span.SetOperationName("op2")
	assert.True(t, span.context.isSamplingFinalized(), "span is finalized when created")
	assert.True(t, span.context.IsSampled(), "span is sampled")
	assert.Equal(t, "op2", span.OperationName())
	assert.Equal(t, []Tag{}, span.tags, "sampling tags are not re-added")
}
