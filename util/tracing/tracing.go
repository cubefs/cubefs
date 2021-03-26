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
	"reflect"

	raftTracing "github.com/tiglabs/raft/tracing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

var (
	singletonNoopTracer  = &noopTracer{}
	singletonNoopTracers = &noopTracers{}
)

func IsEnabled() bool {
	return Tracing
}

func SetEnable(enable bool) {
	Tracing = enable
	raftTracing.SetEnable(enable)
}

type Tracer interface {
	SetTag(key string, val interface{}) Tracer
	Finish()
	Inject(w io.Writer) error
	Context() context.Context
	ChildTracer(name string) Tracer
}

func ExtractTracer(r io.Reader, name string) Tracer {
	if !Tracing {
		return singletonNoopTracer
	}
	var span opentracing.Span = nil
	if sc, extractErr := opentracing.GlobalTracer().Extract(opentracing.Binary, r); extractErr == nil {
		span = opentracing.StartSpan(name, ext.RPCServerOption(sc))
	}
	if span == nil {
		span = opentracing.StartSpan(name)
	}
	return &opentracingTracer{
		span: span,
		ctx:  context.Background(),
	}
}

func TracerFromContext(ctx context.Context) Tracer {
	if !Tracing || ctx == nil || reflect.ValueOf(ctx).IsNil() {
		return singletonNoopTracer
	}
	if span := opentracing.SpanFromContext(ctx); span != nil {
		return &opentracingTracer{
			span: span,
			ctx:  ctx,
		}
	}
	return singletonNoopTracer
}

func NewTracer(name string) Tracer {
	if !Tracing {
		return singletonNoopTracer
	}
	return &opentracingTracer{
		span: opentracing.StartSpan(name),
		ctx:  context.Background(),
	}
}

func DefaultTracer() Tracer {
	return singletonNoopTracer
}

type Tracers interface {
	AddTracer(tracer Tracer)
	Finish()
	Clean()
}

type sliceTracers []Tracer

func (ts *sliceTracers) AddTracer(tracer Tracer) {
	*ts = append(*ts, tracer)
}

func (ts *sliceTracers) Finish() {
	for _, t := range *ts {
		t.Finish()
	}
}

type noopTracers struct{}

func (*noopTracers) AddTracer(tracer Tracer) {}

func (*noopTracers) Finish() {}

func (*noopTracers) Clean() {}

func (ts *sliceTracers) Clean() {
	*ts = (*ts)[0:0]
}

func NewTracers(cap int) Tracers {
	if !Tracing {
		return singletonNoopTracers
	}
	ts := sliceTracers(make([]Tracer, 0, cap))
	return &ts
}
