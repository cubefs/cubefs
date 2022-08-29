// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package trace

// TracerOption is a function that sets some option on the tracer
type TracerOption func(tracer *Tracer)

// TracerOptions is a factory for all available TracerOption's
var TracerOptions tracerOptions

type tracerOptions struct{}

func (tracerOptions) MaxLogsPerSpan(maxLogsPerSpan int) TracerOption {
	return func(tracer *Tracer) {
		tracer.options.maxLogsPerSpan = maxLogsPerSpan
	}
}
