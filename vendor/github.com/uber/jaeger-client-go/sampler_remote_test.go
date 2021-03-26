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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	mTestutils "github.com/uber/jaeger-lib/metrics/metricstest"

	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/testutils"
	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
)

func TestRemotelyControlledSampler_updateRace(t *testing.T) {
	m := &Metrics{
		SamplerRetrieved: metrics.NullCounter,
		SamplerUpdated:   metrics.NullCounter,
	}
	initSampler, _ := NewProbabilisticSampler(0.123)
	logger := log.NullLogger
	fetcher := &testSamplingStrategyFetcher{response: []byte("probabilistic")}
	parser := new(testSamplingStrategyParser)
	updaters := []SamplerUpdater{new(ProbabilisticSamplerUpdater)}
	sampler := NewRemotelyControlledSampler(
		"test",
		SamplerOptions.Metrics(m),
		SamplerOptions.MaxOperations(42),
		SamplerOptions.OperationNameLateBinding(true),
		SamplerOptions.InitialSampler(initSampler),
		SamplerOptions.Logger(logger),
		SamplerOptions.SamplingServerURL("my url"),
		SamplerOptions.SamplingRefreshInterval(time.Millisecond),
		SamplerOptions.SamplingStrategyFetcher(fetcher),
		SamplerOptions.SamplingStrategyParser(parser),
		SamplerOptions.Updaters(updaters...),
	)

	s := makeSpan(1, "test")
	end := make(chan struct{})

	accessor := func(f func()) {
		for {
			select {
			case <-end:
				return
			default:
				f()
			}
		}
	}

	go accessor(func() {
		sampler.UpdateSampler()
	})

	go accessor(func() {
		sampler.IsSampled(TraceID{Low: 1}, "test")
	})

	go accessor(func() {
		sampler.OnCreateSpan(s)
	})

	go accessor(func() {
		sampler.OnSetTag(s, "test", 1)
	})

	go accessor(func() {
		sampler.OnFinishSpan(s)
	})

	go accessor(func() {
		sampler.OnSetOperationName(s, "test")
	})

	time.Sleep(100 * time.Millisecond)
	close(end)
	sampler.Close()
}

type testSamplingStrategyFetcher struct {
	response []byte
}

func (c *testSamplingStrategyFetcher) Fetch(serviceName string) ([]byte, error) {
	return []byte(c.response), nil
}

type testSamplingStrategyParser struct {
}

func (p *testSamplingStrategyParser) Parse(response []byte) (interface{}, error) {
	strategy := new(sampling.SamplingStrategyResponse)

	switch string(response) {
	case "probabilistic":
		strategy.StrategyType = sampling.SamplingStrategyType_PROBABILISTIC
		strategy.ProbabilisticSampling = &sampling.ProbabilisticSamplingStrategy{
			SamplingRate: 0.85,
		}
		return strategy, nil
	}

	return nil, errors.New("unknown strategy test request")
}

func TestRemoteSamplerOptions(t *testing.T) {
	m := new(Metrics)
	initSampler, _ := NewProbabilisticSampler(0.123)
	logger := log.NullLogger
	fetcher := new(fakeSamplingFetcher)
	parser := new(samplingStrategyParser)
	updaters := []SamplerUpdater{new(ProbabilisticSamplerUpdater)}
	sampler := NewRemotelyControlledSampler(
		"test",
		SamplerOptions.Metrics(m),
		SamplerOptions.MaxOperations(42),
		SamplerOptions.OperationNameLateBinding(true),
		SamplerOptions.InitialSampler(initSampler),
		SamplerOptions.Logger(logger),
		SamplerOptions.SamplingServerURL("my url"),
		SamplerOptions.SamplingRefreshInterval(42*time.Second),
		SamplerOptions.SamplingStrategyFetcher(fetcher),
		SamplerOptions.SamplingStrategyParser(parser),
		SamplerOptions.Updaters(updaters...),
	)
	assert.Same(t, m, sampler.metrics)
	assert.Equal(t, 42, sampler.posParams.MaxOperations)
	assert.True(t, sampler.posParams.OperationNameLateBinding)
	assert.Same(t, initSampler, sampler.Sampler())
	assert.Same(t, logger, sampler.logger)
	assert.Equal(t, "my url", sampler.samplingServerURL)
	assert.Equal(t, 42*time.Second, sampler.samplingRefreshInterval)
	assert.Same(t, fetcher, sampler.samplingFetcher)
	assert.Same(t, parser, sampler.samplingParser)
	assert.Same(t, updaters[0], sampler.updaters[0])
}

func TestRemoteSamplerOptionsDefaults(t *testing.T) {
	options := new(samplerOptions).applyOptionsAndDefaults()
	sampler, ok := options.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.Equal(t, 0.001, sampler.samplingRate)

	assert.NotNil(t, options.logger)
	assert.NotEmpty(t, options.samplingServerURL)
	assert.NotNil(t, options.metrics)
	assert.NotZero(t, options.samplingRefreshInterval)
}

func initAgent(t *testing.T) (*testutils.MockAgent, *RemotelyControlledSampler, *mTestutils.Factory) {
	agent, err := testutils.StartMockAgent()
	require.NoError(t, err)

	metricsFactory := mTestutils.NewFactory(0)
	metrics := NewMetrics(metricsFactory, nil)

	initialSampler, _ := NewProbabilisticSampler(0.001)
	sampler := NewRemotelyControlledSampler(
		"client app",
		SamplerOptions.Metrics(metrics),
		SamplerOptions.SamplingServerURL("http://"+agent.SamplingServerAddr()),
		SamplerOptions.MaxOperations(testDefaultMaxOperations),
		SamplerOptions.InitialSampler(initialSampler),
		SamplerOptions.Logger(log.NullLogger),
		SamplerOptions.SamplingRefreshInterval(time.Minute),
	)
	sampler.Close() // stop timer-based updates, we want to call them manually

	return agent, sampler, metricsFactory
}

func makeSpan(id uint64, operationName string) *Span {
	return &Span{
		context: SpanContext{
			traceID:       TraceID{Low: id},
			samplingState: new(samplingState),
		},
		operationName: operationName,
	}
}

func TestRemotelyControlledSampler(t *testing.T) {
	agent, remoteSampler, metricsFactory := initAgent(t)
	defer agent.Close()

	defaultSampler := newProbabilisticSampler(0.001)
	remoteSampler.setSampler(defaultSampler)

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, testDefaultSamplingProbability))
	remoteSampler.UpdateSampler()
	metricsFactory.AssertCounterMetrics(t, []mTestutils.ExpectedMetric{
		{Name: "jaeger.tracer.sampler_queries", Tags: map[string]string{"result": "ok"}, Value: 1},
		{Name: "jaeger.tracer.sampler_updates", Tags: map[string]string{"result": "ok"}, Value: 1},
	}...)
	s1, ok := remoteSampler.Sampler().(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.EqualValues(t, testDefaultSamplingProbability, s1.samplingRate, "Sampler should have been updated")

	decision := remoteSampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
	assert.False(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)
	decision = remoteSampler.OnCreateSpan(makeSpan(testMaxID-10, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	remoteSampler.setSampler(defaultSampler)

	c := make(chan time.Time)
	ticker := &time.Ticker{C: c}
	go remoteSampler.pollControllerWithTicker(ticker)

	c <- time.Now() // force update based on timer
	time.Sleep(10 * time.Millisecond)
	remoteSampler.Close()

	s2, ok := remoteSampler.Sampler().(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.EqualValues(t, testDefaultSamplingProbability, s2.samplingRate, "Sampler should have been updated from timer")

	assert.False(t, remoteSampler.Equal(remoteSampler)) // for code coverage only
}

func makeSamplerTags(key string, value interface{}) []Tag {
	return []Tag{
		{"sampler.type", key},
		{"sampler.param", value},
	}
}

func TestRemotelyControlledSampler_updateSampler(t *testing.T) {
	tests := []struct {
		probabilities              map[string]float64
		defaultProbability         float64
		expectedDefaultProbability float64
		expectedTags               []Tag
	}{
		{
			probabilities:              map[string]float64{testOperationName: 1.1},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               makeSamplerTags("probabilistic", 1.0),
		},
		{
			probabilities:              map[string]float64{testOperationName: testDefaultSamplingProbability},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities: map[string]float64{
				testOperationName:          testDefaultSamplingProbability,
				testFirstTimeOperationName: testDefaultSamplingProbability,
			},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities:              map[string]float64{"new op": 1.1},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities:              map[string]float64{"new op": 1.1},
			defaultProbability:         1.1,
			expectedDefaultProbability: 1.0,
			expectedTags:               makeSamplerTags("probabilistic", 1.0),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			agent, sampler, metricsFactory := initAgent(t)
			defer agent.Close()

			initSampler, ok := sampler.Sampler().(*ProbabilisticSampler)
			assert.True(t, ok)

			res := &sampling.SamplingStrategyResponse{
				StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
				OperationSampling: &sampling.PerOperationSamplingStrategies{
					DefaultSamplingProbability:       test.defaultProbability,
					DefaultLowerBoundTracesPerSecond: 0.001,
				},
			}
			for opName, prob := range test.probabilities {
				res.OperationSampling.PerOperationStrategies = append(res.OperationSampling.PerOperationStrategies,
					&sampling.OperationSamplingStrategy{
						Operation: opName,
						ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
							SamplingRate: prob,
						},
					},
				)
			}

			agent.AddSamplingStrategy("client app", res)
			sampler.UpdateSampler()

			metricsFactory.AssertCounterMetrics(t,
				mTestutils.ExpectedMetric{
					Name: "jaeger.tracer.sampler_updates", Tags: map[string]string{"result": "ok"}, Value: 1,
				},
			)

			s, ok := sampler.Sampler().(*PerOperationSampler)
			assert.True(t, ok)
			assert.NotEqual(t, initSampler, sampler.Sampler(), "Sampler should have been updated")
			assert.Equal(t, test.expectedDefaultProbability, s.defaultSampler.SamplingRate())

			// First call is always sampled
			decision := sampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
			assert.True(t, decision.Sample)

			decision = sampler.OnCreateSpan(makeSpan(testMaxID-10, testOperationName))
			assert.True(t, decision.Sample)
			assert.Equal(t, test.expectedTags, decision.Tags)
		})
	}
}

func TestRemotelyControlledSampler_updateDefaultRate(t *testing.T) {
	agent, sampler, _ := initAgent(t)
	defer agent.Close()

	res := &sampling.SamplingStrategyResponse{
		StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
		OperationSampling: &sampling.PerOperationSamplingStrategies{
			DefaultSamplingProbability: 0.5,
		},
	}
	agent.AddSamplingStrategy("client app", res)
	sampler.UpdateSampler()

	// Check what rate we get for a specific operation
	decision := sampler.OnCreateSpan(makeSpan(0, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, makeSamplerTags("probabilistic", 0.5), decision.Tags)

	// Change the default and update
	res.OperationSampling.DefaultSamplingProbability = 0.1
	sampler.UpdateSampler()

	// Check sampling rate has changed
	decision = sampler.OnCreateSpan(makeSpan(0, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, makeSamplerTags("probabilistic", 0.1), decision.Tags)

	// Add an operation-specific rate
	res.OperationSampling.PerOperationStrategies = []*sampling.OperationSamplingStrategy{{
		Operation: testOperationName,
		ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
			SamplingRate: 0.2,
		},
	}}
	sampler.UpdateSampler()

	// Check we get the requested rate
	decision = sampler.OnCreateSpan(makeSpan(0, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, makeSamplerTags("probabilistic", 0.2), decision.Tags)

	// Now remove the operation-specific rate
	res.OperationSampling.PerOperationStrategies = nil
	sampler.UpdateSampler()

	// Check we get the default rate
	assert.True(t, decision.Sample)
	decision = sampler.OnCreateSpan(makeSpan(0, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, makeSamplerTags("probabilistic", 0.1), decision.Tags)
}

func TestSamplerQueryError(t *testing.T) {
	agent, sampler, metricsFactory := initAgent(t)
	defer agent.Close()

	// override the actual handler
	sampler.samplingFetcher = &fakeSamplingFetcher{}

	initSampler, ok := sampler.Sampler().(*ProbabilisticSampler)
	assert.True(t, ok)

	sampler.Close() // stop timer-based updates, we want to call them manually

	sampler.UpdateSampler()
	assert.Equal(t, initSampler, sampler.Sampler(), "Sampler should not have been updated due to query error")

	metricsFactory.AssertCounterMetrics(t,
		mTestutils.ExpectedMetric{Name: "jaeger.tracer.sampler_queries", Tags: map[string]string{"result": "err"}, Value: 1},
	)
}

type fakeSamplingFetcher struct{}

func (c *fakeSamplingFetcher) Fetch(serviceName string) ([]byte, error) {
	return nil, errors.New("query error")
}

func TestRemotelyControlledSampler_updateSamplerFromAdaptiveSampler(t *testing.T) {
	agent, remoteSampler, metricsFactory := initAgent(t)
	defer agent.Close()
	remoteSampler.Close() // close the second time (initAgent already called Close)

	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 1.0,
	}
	adaptiveSampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.setSampler(adaptiveSampler)

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.5))
	remoteSampler.UpdateSampler()

	// Sampler should have been updated to probabilistic
	_, ok := remoteSampler.Sampler().(*ProbabilisticSampler)
	require.True(t, ok)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.setSampler(adaptiveSampler)

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 1))
	remoteSampler.UpdateSampler()

	// Sampler should have been updated to ratelimiting
	_, ok = remoteSampler.Sampler().(*RateLimitingSampler)
	require.True(t, ok)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.setSampler(adaptiveSampler)

	// Update existing adaptive sampler
	agent.AddSamplingStrategy("client app", &sampling.SamplingStrategyResponse{OperationSampling: strategies})
	remoteSampler.UpdateSampler()

	metricsFactory.AssertCounterMetrics(t,
		mTestutils.ExpectedMetric{Name: "jaeger.tracer.sampler_queries", Tags: map[string]string{"result": "ok"}, Value: 3},
		mTestutils.ExpectedMetric{Name: "jaeger.tracer.sampler_updates", Tags: map[string]string{"result": "ok"}, Value: 3},
	)
}

func TestRemotelyControlledSampler_updateRateLimitingOrProbabilisticSampler(t *testing.T) {
	probabilisticSampler, err := NewProbabilisticSampler(0.002)
	require.NoError(t, err)
	otherProbabilisticSampler, err := NewProbabilisticSampler(0.003)
	require.NoError(t, err)
	maxProbabilisticSampler, err := NewProbabilisticSampler(1.0)
	require.NoError(t, err)

	rateLimitingSampler := NewRateLimitingSampler(2)
	otherRateLimitingSampler := NewRateLimitingSampler(3)

	testCases := []struct {
		res                  *sampling.SamplingStrategyResponse
		initSampler          SamplerV2
		expectedSampler      Sampler
		shouldErr            bool
		referenceEquivalence bool
		caption              string
	}{
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 1.5),
			initSampler:          probabilisticSampler,
			expectedSampler:      maxProbabilisticSampler,
			shouldErr:            true,
			referenceEquivalence: false,
			caption:              "invalid probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.002),
			initSampler:          probabilisticSampler,
			expectedSampler:      probabilisticSampler,
			shouldErr:            false,
			referenceEquivalence: true,
			caption:              "unchanged probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.003),
			initSampler:          probabilisticSampler,
			expectedSampler:      otherProbabilisticSampler,
			shouldErr:            false,
			referenceEquivalence: false,
			caption:              "valid probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 2),
			initSampler:          rateLimitingSampler,
			expectedSampler:      rateLimitingSampler,
			shouldErr:            false,
			referenceEquivalence: true,
			caption:              "unchanged rate limiting strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 3),
			initSampler:          rateLimitingSampler,
			expectedSampler:      otherRateLimitingSampler,
			shouldErr:            false,
			referenceEquivalence: false,
			caption:              "valid rate limiting strategy",
		},
		{
			res:                  &sampling.SamplingStrategyResponse{},
			initSampler:          rateLimitingSampler,
			expectedSampler:      rateLimitingSampler,
			shouldErr:            true,
			referenceEquivalence: true,
			caption:              "invalid strategy",
		},
	}

	for _, tc := range testCases {
		testCase := tc // capture loop var
		t.Run(testCase.caption, func(t *testing.T) {
			remoteSampler := NewRemotelyControlledSampler(
				"test",
				SamplerOptions.InitialSampler(testCase.initSampler.(Sampler)),
				SamplerOptions.Updaters(
					new(ProbabilisticSamplerUpdater),
					new(RateLimitingSamplerUpdater),
				),
			)
			err := remoteSampler.updateSamplerViaUpdaters(testCase.res)
			if testCase.shouldErr {
				require.Error(t, err)
				return
			}
			if testCase.referenceEquivalence {
				assert.Equal(t, testCase.expectedSampler, remoteSampler.Sampler())
			} else {
				type comparable interface {
					Equal(other Sampler) bool
				}
				es, esOk := testCase.expectedSampler.(comparable)
				require.True(t, esOk, "expected sampler %+v must implement Equal()", testCase.expectedSampler)
				assert.True(t, es.Equal(remoteSampler.Sampler().(Sampler)),
					"sampler.Equal: want=%+v, have=%+v", testCase.expectedSampler, remoteSampler.Sampler())
			}
		})
	}
}

func getSamplingStrategyResponse(strategyType sampling.SamplingStrategyType, value float64) *sampling.SamplingStrategyResponse {
	if strategyType == sampling.SamplingStrategyType_PROBABILISTIC {
		return &sampling.SamplingStrategyResponse{
			StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
				SamplingRate: value,
			},
		}
	}
	if strategyType == sampling.SamplingStrategyType_RATE_LIMITING {
		return &sampling.SamplingStrategyResponse{
			StrategyType: sampling.SamplingStrategyType_RATE_LIMITING,
			RateLimitingSampling: &sampling.RateLimitingSamplingStrategy{
				MaxTracesPerSecond: int16(value),
			},
		}
	}
	return nil
}

func TestRemotelyControlledSampler_printErrorForBrokenUpstream(t *testing.T) {
	logger := &log.BytesBufferLogger{}
	sampler := NewRemotelyControlledSampler(
		"client app",
		SamplerOptions.Logger(logger),
		SamplerOptions.SamplingServerURL("invalid address"),
	)
	sampler.Close() // stop timer-based updates, we want to call them manually
	sampler.UpdateSampler()
	assert.Contains(t, logger.String(), "failed to fetch sampling strategy:")
}
