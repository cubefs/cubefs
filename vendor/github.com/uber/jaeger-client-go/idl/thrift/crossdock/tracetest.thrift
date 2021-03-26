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

namespace cpp jaegertracing.crossdock.thrift
namespace java io.jaegertracing.crossdock.thrift
namespace php Jaeger.Thrift.Crossdock
namespace netcore Jaeger.Thrift.Crossdock

enum Transport { HTTP, TCHANNEL, DUMMY }

struct Downstream {
    1: required string serviceName
    2: required string serverRole
    3: required string host
    4: required string port
    5: required Transport transport
    6: optional Downstream downstream
}

struct StartTraceRequest {
    1: required string serverRole  // role of the server (always S1)
    2: required bool sampled
    3: required string baggage
    4: required Downstream downstream
}

struct JoinTraceRequest {
    1: required string serverRole  // role of the server, S2 or S3
    2: optional Downstream downstream
}

struct ObservedSpan {
    1: required string traceId
    2: required bool sampled
    3: required string baggage
}

/**
 * Each server must include the information about the span it observed.
 * It can only be omitted from the response if notImplementedError field is not empty.
 * If the server was instructed to make a downstream call, it must embed the
 * downstream response in its own response.
 */
struct TraceResponse {
    1: optional ObservedSpan span
    2: optional TraceResponse downstream
    3: required string notImplementedError
}

service TracedService {
    TraceResponse startTrace(1: StartTraceRequest request)
    TraceResponse joinTrace(1: JoinTraceRequest request)
}
