# Copyright (c) 2017 Uber Technologies, Inc.
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

namespace cpp jaegertracing.thrift
namespace java io.jaegertracing.thriftjava
namespace php Jaeger.Thrift.Agent
namespace netcore Jaeger.Thrift.Agent
namespace lua jaeger.thrift.agent

# ValidateTraceResponse returns ok when a trace has been written to redis.
struct ValidateTraceResponse {
    1: required bool ok
    2: required i64 traceCount
}

service AggregationValidator {
    ValidateTraceResponse validateTrace(1: required string traceId)
}
