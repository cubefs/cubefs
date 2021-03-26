// Copyright 2021 The ChubaoFS Authors.
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

package tracing

import (
	"context"
	"io"
)

type noopTracer struct {
}

func (noopTracer) ChildTracer(name string) Tracer {
	return NewTracer(name)
}

func (noopTracer) Context() context.Context {
	return context.Background()
}

func (noopTracer) SetTag(key string, val interface{}) Tracer {
	return singletonNoopTracer
}

func (noopTracer) Inject(w io.Writer) error {
	return nil
}

func (noopTracer) Finish() {
	return
}
