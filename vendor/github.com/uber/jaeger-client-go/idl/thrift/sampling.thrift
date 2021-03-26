# Copyright (c) 2016 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

namespace cpp jaegertracing.sampling_manager.thrift
namespace java io.jaegertracing.thrift.sampling_manager
namespace php Jaeger.Thrift.Agent
namespace netcore Jaeger.Thrift.Agent
namespace lua jaeger.thrift.agent

enum SamplingStrategyType { PROBABILISTIC, RATE_LIMITING }

// ProbabilisticSamplingStrategy randomly samples a fixed percentage of all traces.
struct ProbabilisticSamplingStrategy {
    1: required double samplingRate // percentage expressed as rate (0..1]
}

// RateLimitingStrategy samples traces with a rate that does not exceed specified number of traces per second.
// The recommended implementation approach is leaky bucket.
struct RateLimitingSamplingStrategy {
    1: required i16 maxTracesPerSecond
}

// OperationSamplingStrategy defines a sampling strategy that randomly samples a fixed percentage of operation traces.
struct OperationSamplingStrategy {
    1: required string operation
    2: required ProbabilisticSamplingStrategy probabilisticSampling
}

// PerOperationSamplingStrategies defines a sampling strategy per each operation name in the service
// with a guaranteed lower bound per second. Once the lower bound is met, operations are randomly sampled
// at a fixed percentage.
struct PerOperationSamplingStrategies {
    1: required double defaultSamplingProbability
    2: required double defaultLowerBoundTracesPerSecond
    3: required list<OperationSamplingStrategy> perOperationStrategies
    4: optional double defaultUpperBoundTracesPerSecond
}

struct SamplingStrategyResponse {
    1: required SamplingStrategyType strategyType
    2: optional ProbabilisticSamplingStrategy probabilisticSampling
    3: optional RateLimitingSamplingStrategy rateLimitingSampling
    4: optional PerOperationSamplingStrategies operationSampling
}

service SamplingManager {
    SamplingStrategyResponse getSamplingStrategy(1: string serviceName)
}
