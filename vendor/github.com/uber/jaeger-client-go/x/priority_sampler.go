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
	"sync/atomic"

	"github.com/uber/jaeger-client-go"
)

// prioritySamplerState keeps the state of all underlying samplers, specifically
// whether each of them has previously returned retryable=false, in which case
// those samplers are no longer invoked on future sampling calls.
type prioritySamplerState struct {
	// the calls to this sampler are on the critical path of many Span 'write'
	// operations, and this state is shared across all spans of the trace,
	// so we want to minimize the lock contention. This field is used as a bitmap
	// with atomic LOAD & CAS operations on the individual 64-bit words.
	samplerFinishedBitmap []uint64
}

func newPrioritySamplerState(count int) *prioritySamplerState {
	return &prioritySamplerState{
		samplerFinishedBitmap: make([]uint64, 1+count/64),
	}
}

func (s *prioritySamplerState) fieldAndMask(index int) (field *uint64, mask uint64) {
	field = &s.samplerFinishedBitmap[index/64]
	mask = uint64(1) << uint(index%64)
	return
}

func (s *prioritySamplerState) isSamplerRetryable(index int) bool {
	field, mask := s.fieldAndMask(index)
	value := atomic.LoadUint64(field)
	return (value & mask) == mask
}

func (s *prioritySamplerState) markSamplerFinished(index int) {
	field, mask := s.fieldAndMask(index)
	swapped := false
	for !swapped {
		value := atomic.LoadUint64(field)
		swapped = atomic.CompareAndSwapUint64(field, value, value|mask)
	}
}

// PrioritySampler contains a list of samplers that it interrogates in order.
// Sampling methods return as soon as one of the samplers returns sample=true.
// The retryable state for each underlying sampler is stored in the extended context
// and once retryable=false is returned by one of the delegates it will never be
// called again.
type PrioritySampler struct {
	jaeger.SamplerV2Base

	delegates []jaeger.SamplerV2
}

type extendedStateKey string

// TODO this may need to be unique per instance of the sampler.
const prioritySamplerExtendedStateKey = extendedStateKey("priority-sampler")

// NewPrioritySampler creates a new PrioritySampler with given delegates.
func NewPrioritySampler(delegates ...jaeger.SamplerV2) *PrioritySampler {
	return &PrioritySampler{delegates: delegates}
}

// OnCreateSpan implements SamplerV2 API.
func (s *PrioritySampler) OnCreateSpan(span *jaeger.Span) jaeger.SamplingDecision {
	return s.trySampling(
		span,
		func(sampler jaeger.SamplerV2) jaeger.SamplingDecision {
			return sampler.OnCreateSpan(span)
		},
	)
}

// OnSetOperationName implements SamplerV2 API.
func (s *PrioritySampler) OnSetOperationName(span *jaeger.Span, operationName string) jaeger.SamplingDecision {
	return s.trySampling(
		span,
		func(sampler jaeger.SamplerV2) jaeger.SamplingDecision {
			return sampler.OnSetOperationName(span, operationName)
		},
	)
}

// OnSetTag implements SamplerV2 API.
func (s *PrioritySampler) OnSetTag(span *jaeger.Span, key string, value interface{}) jaeger.SamplingDecision {
	return s.trySampling(
		span,
		func(sampler jaeger.SamplerV2) jaeger.SamplingDecision {
			return sampler.OnSetTag(span, key, value)
		},
	)
}

// OnFinishSpan implements SamplerV2 API.
func (s *PrioritySampler) OnFinishSpan(span *jaeger.Span) jaeger.SamplingDecision {
	return s.trySampling(
		span,
		func(sampler jaeger.SamplerV2) jaeger.SamplingDecision {
			return sampler.OnFinishSpan(span)
		},
	)
}

// Close calls Close on all delegate samplers.
func (s *PrioritySampler) Close() {
	for _, s := range s.delegates {
		s.Close()
	}
}

func (s *PrioritySampler) getState(span *jaeger.Span) *prioritySamplerState {
	ctx := span.Context().(jaeger.SpanContext)
	return ctx.ExtendedSamplingState(
		prioritySamplerExtendedStateKey,
		func() interface{} {
			return newPrioritySamplerState(len(s.delegates))
		},
	).(*prioritySamplerState)
}

func (s *PrioritySampler) trySampling(
	span *jaeger.Span,
	sampleFn func(v2 jaeger.SamplerV2) jaeger.SamplingDecision,
) jaeger.SamplingDecision {
	state := s.getState(span)
	retryable := false
	for i := 0; i < len(s.delegates); i++ {
		if state.isSamplerRetryable(i) {
			continue
		}
		decision := sampleFn(s.delegates[i])
		if !decision.Retryable {
			state.markSamplerFinished(i)
		}
		if decision.Sample {
			return decision
		}
		retryable = retryable || decision.Retryable
	}
	return jaeger.SamplingDecision{Sample: false, Retryable: retryable}
}
