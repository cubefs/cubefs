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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
)

const tagMatchingStrategy = `
    {
      "key": "theWho",
      "values": {
        "Bender": {
          "firehose": false
        }
      },
      "matchers": [
        {
          "value": 42,
          "firehose": true
        }
      ]
    }
`

func tagMatchingSampler(t *testing.T) *TagMatchingSampler {
	sampler, err := NewTagMatchingSamplerFromStrategyJSON([]byte(tagMatchingStrategy))
	require.NoError(t, err)
	assert.Equal(t, "theWho", sampler.tagKey)
	assert.Len(t, sampler.matchersByValue, 2)
	return sampler
}

func TestTagMatchingSamplerShouldNotSampleOrFinalize(t *testing.T) {
	tracer, closer := jaeger.NewTracer("svc", tagMatchingSampler(t), jaeger.NewNullReporter())
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

func TestTagMatchingSampler(t *testing.T) {
	tests := []struct {
		name           string
		tagKey         string
		tagValue       interface{}
		expectSampled  bool
		expectFinal    bool
		expectFirehose bool
	}{
		{
			name:           "matching key and string value",
			tagKey:         "theWho",
			tagValue:       "Bender",
			expectSampled:  true,
			expectFinal:    true,
			expectFirehose: false,
		},
		{
			name:           "matching key and numeric value",
			tagKey:         "theWho",
			tagValue:       float64(42), // because JSON parses "42" as float
			expectSampled:  true,
			expectFinal:    true,
			expectFirehose: true,
		},
		{
			name:           "matching key and mismatching value",
			tagKey:         "theWho",
			tagValue:       "Leela",
			expectSampled:  false,
			expectFinal:    false,
			expectFirehose: false,
		},
		{
			name:           "mismatching key",
			tagKey:         "theWhoAgain",
			expectSampled:  false,
			expectFinal:    false,
			expectFirehose: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracer, closer := jaeger.NewTracer("svc", tagMatchingSampler(t), jaeger.NewNullReporter())
			defer closer.Close()

			span := tracer.StartSpan("op1")
			assert.False(t, span.Context().(jaeger.SpanContext).IsSampled())
			assert.False(t, span.Context().(jaeger.SpanContext).IsSamplingFinalized())

			span.SetTag(test.tagKey, test.tagValue)
			assert.Equal(t, test.expectSampled, span.Context().(jaeger.SpanContext).IsSampled())
			assert.Equal(t, test.expectFinal, span.Context().(jaeger.SpanContext).IsSamplingFinalized())
			assert.Equal(t, test.expectFirehose, span.Context().(jaeger.SpanContext).IsFirehose())
		})
	}
}

func TestNewTagMatchingSamplerFromStrategyJSONError(t *testing.T) {
	_, err := NewTagMatchingSamplerFromStrategyJSON([]byte("bad json"))
	require.Error(t, err)
}
