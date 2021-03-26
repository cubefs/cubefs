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
	"time"

	"github.com/uber/jaeger-client-go/thrift-gen/sampling"

	"github.com/opentracing/opentracing-go"
)

type benchSampler struct {
	name    string
	sampler Sampler
}

func (b benchSampler) String() string {
	return b.name
}

func benchAdaptiveSampler(lateBinding bool, prob float64) Sampler {
	ops := makeOps(5)
	samplingRates := make([]*sampling.OperationSamplingStrategy, 5)
	for i, op := range ops {
		samplingRates[i] = &sampling.OperationSamplingStrategy{
			Operation:             op,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: prob},
		}
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       prob,
		DefaultLowerBoundTracesPerSecond: 0,
		PerOperationStrategies:           samplingRates,
	}
	return NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations:            7,
		OperationNameLateBinding: lateBinding,
		Strategies:               strategies,
	})
	// Change to below when running on <=2.19
	//return newAdaptiveSampler(strategies, 7)
}

func BenchmarkTracer(b *testing.B) {
	axes := []axis{
		{
			name: "sampler",
			values: []interface{}{
				benchSampler{name: "NeverSample", sampler: NewConstSampler(false)},
				benchSampler{name: "AlwaysSample", sampler: NewConstSampler(true)},
				benchSampler{name: "AdaptiveNeverSampleNoLateBinding", sampler: benchAdaptiveSampler(false, 0)},
				benchSampler{name: "AdaptiveAlwaysSampleNoLateBinding", sampler: benchAdaptiveSampler(false, 1)},
				benchSampler{name: "AdaptiveNeverSampleWithLateBinding", sampler: benchAdaptiveSampler(true, 0)},
				benchSampler{name: "AdaptiveAlwaysSampleWithLateBinding", sampler: benchAdaptiveSampler(true, 1)},
			},
		},
		// adding remote sampler on top of others did not show significant impact, keeping it off for now.
		{name: "remoteSampler", values: []interface{}{false}},
		{name: "children", values: []interface{}{false, true}},
		// tags and ops dimensions did not show significant impact, so keeping to one value only for now.
		{name: "tags", values: []interface{}{5}},
		{name: "ops", values: []interface{}{10}},
	}
	for _, entry := range combinations(axes) {
		b.Run(entry.encode(axes)+"cpus", func(b *testing.B) {
			sampler := entry["sampler"].(benchSampler).sampler
			if entry["remoteSampler"].(bool) {
				sampler = NewRemotelyControlledSampler("service",
					SamplerOptions.InitialSampler(sampler),
					SamplerOptions.SamplingRefreshInterval(time.Minute),
				)
			}
			options := benchOptions{
				tags:     entry["tags"].(int),
				ops:      entry["ops"].(int),
				sampler:  sampler,
				children: entry["children"].(bool),
			}
			benchmarkTracer(b, options)
		})
	}
}

type benchOptions struct {
	tags     int
	ops      int
	sampler  Sampler
	children bool
}

func benchmarkTracer(b *testing.B, options benchOptions) {
	tags := makeStrings("tag", options.tags)
	ops := makeOps(options.ops)

	sampler := options.sampler
	tracer, closer := NewTracer("service", sampler, NewNullReporter())
	defer closer.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var parent opentracing.SpanContext
		var op int
		for pb.Next() {
			span := tracer.StartSpan(ops[op], opentracing.ChildOf(parent))
			for i := range tags {
				span.SetTag(tags[i], tags[i])
			}
			if options.children {
				parent = span.Context()
			}
			span.Finish()
			op = (op + 1) % len(ops)
		}
	})
}

func makeOps(num int) []string {
	return makeStrings("span", num)
}

func makeStrings(prefix string, num int) []string {
	values := make([]string, num)
	for i := 0; i < num; i++ {
		values[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	return values
}

// combinations takes a list axes and their values and
// returns a collection of entries which contain all combinations of each axis
// value with every other axis' values.
func combinations(axes []axis) []permutation {
	if len(axes) == 0 {
		return nil
	}

	if len(axes) == 1 {
		return axes[0].entries()
	}

	var entries []permutation
	// combos := combinations(axes[1:])
	last := len(axes) - 1
	combos := combinations(axes[:last])
	for _, remaining := range combos {
		for _, entry := range axes[last].entries() {
			for k, v := range remaining {
				entry[k] = v
			}
			entries = append(entries, entry)
		}
	}

	return entries
}

type axis struct {
	name   string
	values []interface{}
}

func (x axis) entries() []permutation {
	items := make([]permutation, len(x.values))
	for i, value := range x.values {
		items[i] = permutation{x.name: value}
	}
	return items
}

type permutation map[string]interface{}

// encode converts a permutation into a string "k=v/k=v/...".
// The axes argument is used to provide a definitive order of keys.
func (p permutation) encode(axes []axis) string {
	name := ""
	for _, axis := range axes {
		k := axis.name
		v := p[k]
		name = name + fmt.Sprintf("%s=%v", k, v) + "/"
	}
	return name
}
