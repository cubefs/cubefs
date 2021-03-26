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

	"github.com/uber/jaeger-client-go"
)

// TagMatcher describes which values TagMatchingSampler will match.
type TagMatcher struct {
	TagValue interface{} `json:"value"`
	Firehose bool        `json:"firehose"`
}

// TagMatchingSampler samples traces that have spans with a particular tag value(s).
type TagMatchingSampler struct {
	jaeger.SamplerV2Base

	tagKey             string
	matchersByValue    map[interface{}]TagMatcher
	samplerTagsByValue map[interface{}][]jaeger.Tag
	undecided          jaeger.SamplingDecision
}

// NewTagMatchingSampler creates TagMatchingSampler with given tag key and matchers.
func NewTagMatchingSampler(tagKey string, matchers []TagMatcher) *TagMatchingSampler {
	// pre-generate sampler tags for all known tag values
	samplerTagsByValue := make(map[interface{}][]jaeger.Tag)
	matchersByValue := make(map[interface{}]TagMatcher)
	for _, m := range matchers {
		matchersByValue[m.TagValue] = m
		samplerTagsByValue[m.TagValue] = []jaeger.Tag{
			jaeger.NewTag("sampler.type", "TagMatchingSampler"),
			jaeger.NewTag("sampler.param", fmt.Sprintf("%s=%v", tagKey, m.TagValue)),
		}
	}
	return &TagMatchingSampler{
		tagKey:             tagKey,
		matchersByValue:    matchersByValue,
		samplerTagsByValue: samplerTagsByValue,
		undecided:          jaeger.SamplingDecision{Sample: false, Retryable: true, Tags: nil},
	}
}

// TagMatchingSamplingStrategy defines JSON format for TagMatchingSampler strategy.
type TagMatchingSamplingStrategy struct {
	Key      string       `json:"key"`
	Matchers []TagMatcher `json:"matchers"`
	// legacy format as map
	Values map[string]TagMatcher `json:"values"`
}

// NewTagMatchingSamplerFromStrategyJSON creates the sampler from a JSON configuration of the following form:
//
//     {
//       "key": "tagKey",
//       "matchers": [
//         {
//           "value": "tagValue1",
//           "firehose": true
//         },
//         {
//           "value": 42,
//           "firehose": false
//         }
//       ],
//       "values": {
//         "tagValue1": {
//           "firehose": true
//         },
//         "tagValue2": {
//           "firehose": false
//         }
//       }
//     }
//
// Note that matchers can be specified either via "matchers" array (preferred),
// or via "values" dictionary (legacy, only supports string values).
//
// When a given tag value appears multiple time, then last one in "matchers" wins,
// and the one in "values" wins overall.
func NewTagMatchingSamplerFromStrategyJSON(jsonString []byte) (*TagMatchingSampler, error) {
	var strategy TagMatchingSamplingStrategy
	err := json.Unmarshal(jsonString, &strategy)
	if err != nil {
		return nil, err
	}
	return NewTagMatchingSamplerFromStrategy(strategy), nil
}

// NewTagMatchingSamplerFromStrategy instantiates TagMatchingSampler from a strategy.
func NewTagMatchingSamplerFromStrategy(strategy TagMatchingSamplingStrategy) *TagMatchingSampler {
	for k, v := range strategy.Values {
		v.TagValue = k
		strategy.Matchers = append(strategy.Matchers, v)
	}
	return NewTagMatchingSampler(strategy.Key, strategy.Matchers)
}

func (s *TagMatchingSampler) decide(span *jaeger.Span, value interface{}) jaeger.SamplingDecision {
	matcher, ok := s.matchersByValue[value]
	if !ok {
		return s.undecided
	}
	if matcher.Firehose {
		if ctx, ok := span.Context().(jaeger.SpanContext); ok {
			ctx.SetFirehose()
		}
	}
	return jaeger.SamplingDecision{Sample: true, Retryable: false, Tags: s.samplerTagsByValue[value]}
}

// OnCreateSpan never samples.
func (s *TagMatchingSampler) OnCreateSpan(span *jaeger.Span) jaeger.SamplingDecision {
	return s.undecided
}

// OnSetOperationName never samples.
func (s *TagMatchingSampler) OnSetOperationName(span *jaeger.Span, operationName string) jaeger.SamplingDecision {
	return s.undecided
}

// OnSetTag samples if the tag matches key and one of the allowed value(s).
func (s *TagMatchingSampler) OnSetTag(span *jaeger.Span, key string, value interface{}) jaeger.SamplingDecision {
	if key == s.tagKey {
		return s.decide(span, value)
	}
	return s.undecided
}

// OnFinishSpan never samples.
func (s *TagMatchingSampler) OnFinishSpan(span *jaeger.Span) jaeger.SamplingDecision {
	return s.undecided
}
