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

package jaeger

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
	"github.com/uber/jaeger-client-go/utils"
)

const (
	testOperationName          = "op"
	testFirstTimeOperationName = "firstTimeOp"

	testDefaultSamplingProbability = 0.5
	testMaxID                      = uint64(1) << 62
	testDefaultMaxOperations       = 10
)

var (
	testProbabilisticExpectedTags = []Tag{
		{"sampler.type", "probabilistic"},
		{"sampler.param", 0.5},
	}
	testLowerBoundExpectedTags = []Tag{
		{"sampler.type", "lowerbound"},
		{"sampler.param", 0.5},
	}
)

func TestSamplerTags(t *testing.T) {
	prob, err := NewProbabilisticSampler(0.1)
	require.NoError(t, err)
	rate := NewRateLimitingSampler(0.1)
	remote := &RemotelyControlledSampler{}
	remote.setSampler(NewConstSampler(true))
	tests := []struct {
		sampler  SamplerV2
		typeTag  string
		paramTag interface{}
	}{
		{NewConstSampler(true), "const", true},
		{NewConstSampler(false), "const", false},
		{prob, "probabilistic", 0.1},
		{rate, "ratelimiting", 0.1},
		{remote, "const", true},
	}
	for _, test := range tests {
		decision := test.sampler.OnCreateSpan(makeSpan(0, testOperationName))
		assert.Equal(t, makeSamplerTags(test.typeTag, test.paramTag), decision.Tags)
	}
}

func TestProbabilisticSamplerErrors(t *testing.T) {
	_, err := NewProbabilisticSampler(-0.1)
	assert.Error(t, err)
	_, err = NewProbabilisticSampler(1.1)
	assert.Error(t, err)
}

func TestProbabilisticSampler(t *testing.T) {
	sampler, _ := NewProbabilisticSampler(0.5)
	sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.False(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID - 20}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)

	t.Run("test_64bit_id", func(t *testing.T) {
		sampled, tags := sampler.IsSampled(TraceID{Low: (testMaxID + 10) | 1<<63}, testOperationName)
		assert.False(t, sampled)
		assert.Equal(t, testProbabilisticExpectedTags, tags)
		sampled, tags = sampler.IsSampled(TraceID{Low: (testMaxID - 20) | 1<<63}, testOperationName)
		assert.True(t, sampled)
		assert.Equal(t, testProbabilisticExpectedTags, tags)
	})

	sampler2, _ := NewProbabilisticSampler(0.5)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(NewConstSampler(true)))
}

func TestProbabilisticSamplerPerformance(t *testing.T) {
	t.Skip("Skipped performance test")
	sampler, _ := NewProbabilisticSampler(0.01)
	rand := utils.NewRand(8736823764)
	var count uint64
	for i := 0; i < 100000000; i++ {
		id := TraceID{Low: uint64(rand.Int63())}
		if sampled, _ := sampler.IsSampled(id, testOperationName); sampled {
			count++
		}
	}
	// println("Sampled:", count, "rate=", float64(count)/float64(100000000))
	// Sampled: 999829 rate= 0.009998290
}

func TestRateLimitingSampler(t *testing.T) {
	sampler := NewRateLimitingSampler(2)
	sampler2 := NewRateLimitingSampler(2)
	sampler3 := NewRateLimitingSampler(3)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(sampler3))
	assert.False(t, sampler.Equal(NewConstSampler(false)))

	sampler = NewRateLimitingSampler(2)
	sampled, _ := sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)

	sampler = NewRateLimitingSampler(0.1)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)
}

func TestGuaranteedThroughputProbabilisticSamplerUpdate(t *testing.T) {
	samplingRate := 0.5
	lowerBound := 2.0
	sampler, err := NewGuaranteedThroughputProbabilisticSampler(lowerBound, samplingRate)
	assert.NoError(t, err)

	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, samplingRate, sampler.samplingRate)

	newSamplingRate := 0.6
	newLowerBound := 1.0
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newSamplingRate, sampler.samplingRate)

	newSamplingRate = 1.1
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, 1.0, sampler.samplingRate)
}

func TestAdaptiveSampler(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: testDefaultSamplingProbability},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 1.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler, err := NewAdaptiveSampler(strategies, 42)
	require.NoError(t, err, "deprecated constructor successful")
	assert.Equal(t, 42, sampler.maxOperations)
	sampler.Close()

	sampler = NewPerOperationSampler(PerOperationSamplerParams{Strategies: strategies})
	assert.Equal(t, sampler.maxOperations, 2000, "default MaxOperations applied")
	sampler.Close()

	sampler = NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	defer sampler.Close()

	decision := sampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testLowerBoundExpectedTags, decision.Tags)

	decision = sampler.OnCreateSpan(makeSpan(testMaxID-20, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
	assert.False(t, decision.Sample)

	// This operation is seen for the first time by the sampler
	decision = sampler.OnCreateSpan(makeSpan(testMaxID, testFirstTimeOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnSetOperationName(makeSpan(testMaxID, testFirstTimeOperationName), testFirstTimeOperationName)
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnSetTag(makeSpan(testMaxID, testFirstTimeOperationName), "key", "value")
	assert.False(t, decision.Sample)

	decision = sampler.OnFinishSpan(makeSpan(testMaxID, testFirstTimeOperationName))
	assert.False(t, decision.Sample)
}

func TestPerOperationSampler_String(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             testOperationName,
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1},
			},
		},
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})

	assert.Equal(t,
		"PerOperationSampler(defaultSampler=ProbabilisticSampler(samplingRate=0.5), lowerBound=2.000000, "+
			"maxOperations=10, operationNameLateBinding=false, numOperations=1,\nsamplers=[\n"+
			"(operationName=op, "+
			"sampler=GuaranteedThroughputProbabilisticSampler(lowerBound=2.000000, samplingRate=1.000000))])",
		sampler.String())
}

func TestAdaptiveSamplerErrors(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             testOperationName,
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: -0.1},
			},
		},
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	assert.Equal(t, 0.0, sampler.samplers[testOperationName].samplingRate)

	strategies.PerOperationStrategies[0].ProbabilisticSampling.SamplingRate = 1.1
	sampler = NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	assert.Equal(t, 1.0, sampler.samplers[testOperationName].samplingRate)
}

func TestAdaptiveSamplerUpdate(t *testing.T) {
	samplingRate := 0.1
	lowerBound := 2.0
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: samplingRate},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: lowerBound,
		PerOperationStrategies:           samplingRates,
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})

	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, testDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 1)

	// Update the sampler with new sampling rates
	newSamplingRate := 0.2
	newLowerBound := 3.0
	newDefaultSamplingProbability := 0.1
	newSamplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
		{
			Operation:             testFirstTimeOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
	}
	strategies = &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       newDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: newLowerBound,
		PerOperationStrategies:           newSamplingRates,
	}

	sampler.update(strategies)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 2)
}

func TestMaxOperations(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 0.1},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: 1,
		Strategies:    strategies,
	})

	decision := sampler.OnCreateSpan(makeSpan(testMaxID-10, testFirstTimeOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)
}

func TestAdaptiveSamplerDoesNotApplyToChildrenSpans(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       0,
		DefaultLowerBoundTracesPerSecond: 0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             "op1",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 0.0},
			},
			{
				Operation:             "op2",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1.0},
			},
		},
	}
	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations:            1,
		OperationNameLateBinding: true, // these tests rely on late binding
		Strategies:               strategies,
	})
	tracer, closer := NewTracer("service", sampler, NewNullReporter())
	defer closer.Close()

	_ = tracer.StartSpan("op1") // exhaust lower bound sampler once
	span1 := tracer.StartSpan("op1")
	assert.False(t, span1.Context().(SpanContext).IsSampled(), "op1 should not be sampled on root span")
	span1.SetOperationName("op2")
	assert.True(t, span1.Context().(SpanContext).IsSampled(), "op2 should be sampled on root span")

	span2 := tracer.StartSpan("op2")
	assert.True(t, span2.Context().(SpanContext).IsSampled(), "op2 should be sampled on root span")

	parent := tracer.StartSpan("op1")
	assert.False(t, parent.Context().(SpanContext).IsSampled(), "parent span should not be sampled")
	assert.False(t, parent.Context().(SpanContext).IsSamplingFinalized(), "parent span should not be finalized")

	child := tracer.StartSpan("op2", opentracing.ChildOf(parent.Context()))
	assert.False(t, child.Context().(SpanContext).IsSampled(), "child span should not be sampled even with op2")
	assert.False(t, child.Context().(SpanContext).IsSamplingFinalized(), "child span should not be finalized")
	child.SetOperationName("op2")
	assert.False(t, child.Context().(SpanContext).IsSampled(), "op2 should not be sampled on the child span")
	assert.True(t, child.Context().(SpanContext).IsSamplingFinalized(), "child span should be finalized after setOperationName")
}

func TestAdaptiveSampler_lockRaceCondition(t *testing.T) {
	agent, remoteSampler, _ := initAgent(t)
	defer agent.Close()
	remoteSampler.Close() // stop timer-based updates, we want to call them manually

	numOperations := 1000
	adaptiveSampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: 2000,
		Strategies: &sampling.PerOperationSamplingStrategies{
			DefaultSamplingProbability: 1,
		},
	})
	// Overwrite the sampler with an adaptive sampler
	remoteSampler.setSampler(adaptiveSampler)

	tracer, closer := NewTracer("service", remoteSampler, NewNullReporter())
	defer closer.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(2)

	isSampled := func(t *testing.T, remoteSampler *RemotelyControlledSampler, numOperations int, operationNamePrefix string) {
		for i := 0; i < numOperations; i++ {
			runtime.Gosched()
			span := tracer.StartSpan(fmt.Sprintf("%s%d", operationNamePrefix, i))
			assert.True(t, span.Context().(SpanContext).IsSampled())
		}
	}

	// Start 2 go routines that will simulate simultaneous calls to IsSampled
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "a")
	}()
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "b")
	}()
}
