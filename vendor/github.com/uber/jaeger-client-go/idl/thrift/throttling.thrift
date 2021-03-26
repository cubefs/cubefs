# Copyright (c) 2018 The Jaeger Authors.
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

namespace cpp jaegertracing.throttling.thrift
namespace java io.jaegertracing.thrift.throttling
namespace php Jaeger.Thrift.Agent
namespace netcore Jaeger.Thrift.Agent

// ThrottlingConfig describes the throttling behavior for a given service.
// Throttling is controlled with a credit account per operation that is refilled
// at a steady rate, like a token bucket. The account may reach a maximum, but
// will not grow beyond that.
struct ThrottlingConfig {
    // Max operations to track with individual throttling credit accounts.
    1: required i32 maxOperations;
    // Number of credits to refill per second.
    2: required double creditsPerSecond;
    // Max balance to cap credits.
    3: required double maxBalance;
}

// Mapping of service name to throttling configuration.
struct ServiceThrottlingConfig {
    1: required string serviceName;
    2: required ThrottlingConfig config;
}

// Response mapping service names to specific throttling configs. Any other
// service not contained in the map should follow the default configuration
// provided in the response.
struct ThrottlingResponse {
    1: required ThrottlingConfig defaultConfig;
    2: required list<ServiceThrottlingConfig> serviceConfigs;
}

// Service to provide service throttling configurations to throttling component.
service ThrottlingService {
    ThrottlingResponse getThrottlingConfigs(1: list<string> serviceNames);
}
