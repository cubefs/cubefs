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

	"github.com/opentracing/opentracing-go"
)

type opentracingTracer struct {
	span opentracing.Span
	ctx  context.Context
}

func (t *opentracingTracer) ChildTracer(name string) Tracer {
	if t.span.Tracer() == nil {
		return singletonNoopTracer
	}
	return &opentracingTracer{
		span: t.span.Tracer().StartSpan(name, opentracing.ChildOf(t.span.Context())),
		ctx:  t.ctx,
	}
}

func (t *opentracingTracer) Context() context.Context {
	return opentracing.ContextWithSpan(t.ctx, t.span)
}

func (t *opentracingTracer) SetTag(key string, val interface{}) Tracer {
	t.span.SetTag(key, val)
	return t
}

func (t *opentracingTracer) Inject(w io.Writer) (err error) {
	err = t.span.Tracer().Inject(t.span.Context(), opentracing.Binary, w)
	return
}

func (t *opentracingTracer) Finish() {
	t.span.Finish()
}
