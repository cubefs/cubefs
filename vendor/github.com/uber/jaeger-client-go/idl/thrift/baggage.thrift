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

# BaggageRestriction contains the baggage key and the maximum length of the baggage value.
struct BaggageRestriction {
   1: required string baggageKey
   2: required i32 maxValueLength
}

service BaggageRestrictionManager  {
    /**
     * getBaggageRestrictions retrieves the baggage restrictions for a specific service.
     * Usually, baggageRestrictions apply to all services however there may be situations
     * where a baggageKey might only be allowed to be set by a specific service.
     */
    list<BaggageRestriction> getBaggageRestrictions(1: string serviceName)
}
