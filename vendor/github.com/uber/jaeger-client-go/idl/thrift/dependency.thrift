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

struct DependencyLink {
  // parent service name (caller)
  1: required string parent
  // child service name (callee)
  2: required string child
  // calls made during the duration of this link
  4: required i64 callCount
}

// An aggregate representation of services paired with every service they call.
struct Dependencies {
  1: required list<DependencyLink> links
}

service Dependency {
    Dependencies getDependenciesForTrace(1: required string traceId)
    oneway void saveDependencies(1: Dependencies dependencies)
}
