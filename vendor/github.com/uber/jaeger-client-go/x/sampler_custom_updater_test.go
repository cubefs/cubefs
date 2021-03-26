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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/testutils"
	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
)

type customSamplingStrategyResponse struct {
	sampling.SamplingStrategyResponse

	TagMatching *TagMatchingSamplingStrategy `json:"tagEqualitySampling"`
}

type customSamplingStrategyParser struct{}

func (p *customSamplingStrategyParser) Parse(response []byte) (interface{}, error) {
	strategy := new(customSamplingStrategyResponse)
	err := json.Unmarshal(response, strategy)
	if err != nil {
		return nil, err
	}
	return strategy, err
}

type customSamplerUpdater struct {
	samplerLock     sync.Mutex
	mainSampler     jaeger.SamplerV2
	defaultUpdaters []jaeger.SamplerUpdater
}

func newCustomSamplerUpdater() *customSamplerUpdater {
	return &customSamplerUpdater{
		defaultUpdaters: []jaeger.SamplerUpdater{
			new(jaeger.ProbabilisticSamplerUpdater),
			new(jaeger.RateLimitingSamplerUpdater),
		},
	}
}

func (u *customSamplerUpdater) Update(sampler jaeger.SamplerV2, strategy interface{}) (jaeger.SamplerV2, error) {
	u.samplerLock.Lock()
	defer u.samplerLock.Unlock()

	// when we're called the first time, the sampler will be the original sampler
	if u.mainSampler == nil {
		u.mainSampler = sampler
	}

	if resp, ok := strategy.(*customSamplingStrategyResponse); ok {
		sampler, err := u.applyDefaultUpdaters(u.mainSampler, &resp.SamplingStrategyResponse)
		if err != nil {
			return nil, err
		}
		u.mainSampler = sampler
		if resp.TagMatching == nil {
			return sampler, nil
		}
		tagSampler := NewTagMatchingSamplerFromStrategy(*resp.TagMatching)
		return NewPrioritySampler(tagSampler, u.mainSampler), nil
	}
	return nil, fmt.Errorf("unsupported sampling strategy response %+v", strategy)
}

func (u *customSamplerUpdater) applyDefaultUpdaters(sampler jaeger.SamplerV2, res *sampling.SamplingStrategyResponse) (jaeger.SamplerV2, error) {
	for _, updater := range u.defaultUpdaters {
		sampler, err := updater.Update(sampler, res)
		if err != nil {
			return nil, err
		}
		if sampler != nil {
			return sampler, nil
		}
	}
	return nil, fmt.Errorf("unsupported sampling strategy type %v", res.GetStrategyType())
}

type customTestObjects struct {
	agent   *testutils.MockAgent
	sampler *jaeger.RemotelyControlledSampler
	tracer  *jaeger.Tracer
	updater *customSamplerUpdater
}

func withAgent(t *testing.T, fn func(objects customTestObjects)) {
	agent, err := testutils.StartMockAgent()
	require.NoError(t, err)
	defer agent.Close()

	updater := newCustomSamplerUpdater()
	sampler := jaeger.NewRemotelyControlledSampler(
		"service",
		jaeger.SamplerOptions.SamplingServerURL("http://"+agent.SamplingServerAddr()),
		jaeger.SamplerOptions.SamplingRefreshInterval(time.Minute),
		jaeger.SamplerOptions.SamplingStrategyParser(new(customSamplingStrategyParser)),
		jaeger.SamplerOptions.Updaters(updater),
	)
	sampler.Close() // stop timer-based updates, we want to call them manually

	tracer, closer := jaeger.NewTracer("service", sampler, jaeger.NewNullReporter())
	defer closer.Close()

	fn(customTestObjects{agent: agent, sampler: sampler, tracer: tracer.(*jaeger.Tracer), updater: updater})
}

func TestCustomRemoteSampler(t *testing.T) {
	mainStrategy := sampling.SamplingStrategyResponse{
		StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
		ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
			SamplingRate: 0.5,
		},
	}
	mainSampler, _ := jaeger.NewProbabilisticSampler(0.5)
	neverSampleStrategy := sampling.SamplingStrategyResponse{
		StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
		ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
			SamplingRate: 0.0,
		},
	}
	neverSampler, _ := jaeger.NewProbabilisticSampler(0.0)
	tagStrategy := &TagMatchingSamplingStrategy{
		Key: "theWho",
		Values: map[string]TagMatcher{
			"Bender": {
				Firehose: true,
			},
		},
	}

	assertSampler := func(t *testing.T, expected jaeger.Sampler, actual jaeger.SamplerV2) {
		assert.IsType(t, expected, actual)
		assert.True(t, expected.Equal(actual.(jaeger.Sampler)),
			"sampler.Equal: want=%+v, have=%+v", expected, actual)
	}

	withAgent(t, func(obj customTestObjects) {
		// step 1 - probabilistic sampler
		obj.agent.AddSamplingStrategy("service", &mainStrategy)
		obj.sampler.UpdateSampler()
		assertSampler(t, mainSampler, obj.sampler.Sampler())

		// step 2 - combine with tag matching sampler
		obj.agent.AddSamplingStrategy("service", &customSamplingStrategyResponse{
			SamplingStrategyResponse: neverSampleStrategy,
			TagMatching:              tagStrategy,
		})
		obj.sampler.UpdateSampler()
		assert.IsType(t, new(PrioritySampler), obj.sampler.Sampler())
		assertSampler(t, neverSampler, obj.updater.mainSampler)

		span := obj.tracer.StartSpan("span")
		assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
		span.SetTag("theWho", "Bender")
		assert.True(t, span.Context().(jaeger.SpanContext).IsSampled())

		// step 3 - back to probabilistic sampler only
		obj.agent.AddSamplingStrategy("service", &mainStrategy)
		obj.sampler.UpdateSampler()
		assertSampler(t, mainSampler, obj.sampler.Sampler())
	})
}
