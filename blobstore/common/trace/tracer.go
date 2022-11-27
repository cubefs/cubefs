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

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const (
	defaultRootSpanName   = "defaultBlobnodeRootSpanName"
	defaultMaxLogsPerSpan = 50
	reqidKey              = "X-Reqid"
)

// ChildOf is the alias of opentracing.ChildOf
var ChildOf = opentracing.ChildOf

// FollowsFrom is the alias of opentracing.FollowsFrom
var FollowsFrom = opentracing.FollowsFrom

// StartTime is alias of opentracing.StartTime.
type StartTime = opentracing.StartTime

// Tags are the expand of opentracing.Tags
type Tags opentracing.Tags

// Apply satisfies the StartSpanOption interface.
func (t Tags) Apply(options *opentracing.StartSpanOptions) {
	if options.Tags == nil {
		options.Tags = make(opentracing.Tags)
	}
	for k, v := range t {
		options.Tags[k] = v
	}
}

// ToSlice change tags to slice
func (t Tags) ToSlice() (ret []string) {
	for k := range t {
		ret = append(ret, k+":"+fmt.Sprint(t[k]))
	}
	return
}

// Marshal marshal tracer tags
func (t Tags) Marshal() (ret []byte, err error) {
	ret, err = json.Marshal(t)
	return
}

// Tag is the alias of opentracing.Tag,
type Tag = opentracing.Tag

// Options tracer options
type Options struct {
	maxLogsPerSpan int
}

// Tracer implements opentracing.Tracer
type Tracer struct {
	serviceName string

	options Options
}

// init sets default global tracer
func init() {
	tracer := NewTracer(path.Base(os.Args[0]))
	SetGlobalTracer(tracer)
}

// NewTracer creates a tracer with serviceName
func NewTracer(serviceName string, opts ...TracerOption) *Tracer {
	t := &Tracer{
		serviceName: serviceName,
	}
	for _, option := range opts {
		option(t)
	}

	if t.options.maxLogsPerSpan <= 0 {
		t.options.maxLogsPerSpan = defaultMaxLogsPerSpan
	}

	return t
}

// StartSpan implements StartSpan() method of opentracing.Tracer.
// Create, start, and return a new Span with the given `operationName` and
// incorporate the given StartSpanOption `opts`.
func (t *Tracer) StartSpan(operationName string, options ...opentracing.StartSpanOption) opentracing.Span {
	sso := opentracing.StartSpanOptions{}
	for _, o := range options {
		o.Apply(&sso)
	}
	return t.startSpanWithOptions(operationName, sso)
}

func (t *Tracer) startSpanWithOptions(operationName string, opts opentracing.StartSpanOptions) Span {
	startTime := opts.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}

	var (
		hasParent  bool
		parent     *SpanContext
		references []opentracing.SpanReference
		ctx        = &SpanContext{}
	)

	for _, reference := range opts.References {
		spanCtx, ok := reference.ReferencedContext.(*SpanContext)
		if !ok {
			continue
		}

		if spanCtx == nil || spanCtx.IsEmpty() {
			continue
		}

		if spanCtx.IsValid() {
			references = append(references, reference)
		}

		if !hasParent {
			parent = spanCtx
			hasParent = reference.Type == opentracing.ChildOfRef
		}
	}

	if !hasParent && parent != nil && !parent.IsEmpty() {
		hasParent = true
	}

	if !hasParent || (parent != nil && !parent.IsValid()) {
		ctx.traceID = RandomID().String()
		ctx.spanID = RandomID()
		ctx.parentID = 0
	} else {
		ctx.traceID = parent.traceID
		ctx.spanID = RandomID()
		ctx.parentID = parent.spanID
	}
	if hasParent {
		// copy baggage items
		parent.ForeachBaggageItems(func(k string, v []string) bool {
			ctx.setBaggageItem(k, v)
			return true
		})
	}

	tags := opts.Tags

	span := &spanImpl{
		operationName: operationName,
		startTime:     startTime,
		tags:          tags,
		context:       ctx,
		tracer:        t,
		references:    references,
		duration:      0,
	}
	span.rootSpan = ctx.parentID == 0
	return span
}

// Inject implements Inject() method of opentracing.Tracer
func (t *Tracer) Inject(sc opentracing.SpanContext, format interface{}, carrier interface{}) error {
	s, ok := sc.(*SpanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	switch format {
	case TextMap, HTTPHeaders:
		return defaultTexMapPropagator.Inject(s, carrier)
	}
	return ErrUnsupportedFormat
}

// Extract implements Extract() method of opentracing.Tracer
func (t *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	switch format {
	case TextMap, HTTPHeaders:
		return defaultTexMapPropagator.Extract(carrier)
	}
	return nil, ErrUnsupportedFormat
}

// Close releases all resources
func (t *Tracer) Close() error {
	// TODO report span
	return nil
}

// StartSpanFromContext starts and returns a Span with `operationName`, using
// any Span found within `ctx` as a ChildOfRef. If no such parent could be
// found, StartSpanFromContext creates a root (parentless) Span.
func StartSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	return span.(Span), ctx
}

// StartSpanFromContextWithTraceID starts and return a new span with `operationName` and traceID.
func StartSpanFromContextWithTraceID(ctx context.Context, operationName string, traceID string, opts ...opentracing.StartSpanOption) (Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	s := span.(*spanImpl)
	s.context.traceID = traceID
	return s, ctx
}

// StartSpanFromHTTPHeaderSafe starts and return a Span with `operationName` and http.Request
func StartSpanFromHTTPHeaderSafe(r *http.Request, operationName string) (Span, context.Context) {
	spanCtx, _ := Extract(HTTPHeaders, HTTPHeadersCarrier(r.Header))
	traceID := r.Header.Get(reqidKey)
	if traceID == "" {
		return StartSpanFromContext(context.Background(), operationName, ext.RPCServerOption(spanCtx))
	}
	return StartSpanFromContextWithTraceID(context.Background(), operationName, traceID, ext.RPCServerOption(spanCtx))
}

// ContextWithSpan returns a new `context.Context` that holds a reference to
// the span. If span is nil, a new context without an active span is returned.
func ContextWithSpan(ctx context.Context, span Span) context.Context {
	return opentracing.ContextWithSpan(ctx, span)
}

// SpanFromContext returns the `Span` previously associated with `ctx`, or
// `nil` if no such `Span` could be found.
func SpanFromContext(ctx context.Context) Span {
	span := opentracing.SpanFromContext(ctx)
	s, ok := span.(Span)
	if !ok {
		return nil
	}
	return s
}

// SpanFromContextSafe returns the `Span` previously associated with `ctx`, or
// creates a root Span with name default.
func SpanFromContextSafe(ctx context.Context) Span {
	span := opentracing.SpanFromContext(ctx)
	s, ok := span.(Span)
	if !ok || s == nil {
		return opentracing.GlobalTracer().StartSpan(defaultRootSpanName).(Span)
	}
	return s
}

// SetGlobalTracer sets the [singleton] opentracing.Tracer returned by
// GlobalTracer(). Those who use GlobalTracer (rather than directly manage an
// opentracing.Tracer instance) should call SetGlobalTracer as early as
// possible in main(), prior to calling the `StartSpan` global func below.
// Prior to calling `SetGlobalTracer`, any Spans started via the `StartSpan`
// (etc) globals are noops.
func SetGlobalTracer(tracer *Tracer) {
	opentracing.SetGlobalTracer(tracer)
}

// CloseGlobalTracer closes global tracer gracefully.
func CloseGlobalTracer() {
	tracer, ok := opentracing.GlobalTracer().(*Tracer)
	if !ok {
		return
	}
	tracer.Close()
}

// GlobalTracer returns the global singleton `Tracer` implementation.
func GlobalTracer() *Tracer {
	t := opentracing.GlobalTracer()
	return t.(*Tracer)
}

// Extract returns a SpanContext instance given `format` and `carrier`.
func Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	return GlobalTracer().Extract(format, carrier)
}

// InjectWithHTTPHeader takes the `sm` SpanContext instance and injects it for
// propagation within `HTTPHeadersCarrier` and `HTTPHeaders`.
func InjectWithHTTPHeader(ctx context.Context, r *http.Request) error {
	span := SpanFromContextSafe(ctx)

	ext.SpanKindRPCClient.Set(span)
	ext.HTTPMethod.Set(span, r.Method)

	return span.Tracer().Inject(span.Context(), HTTPHeaders, HTTPHeadersCarrier(r.Header))
}
