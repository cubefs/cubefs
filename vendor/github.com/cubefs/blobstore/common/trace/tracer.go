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
	defaultRootSpanName   = "defaultEBSRootSpanName"
	defaultMaxLogsPerSpan = 50
	reqidKey              = "X-Reqid"
)

// ChildOf returns a StartSpanOption pointing to a dependent parent span.
// If sc == nil, the option has no effect.
//
// See ChildOfRef, SpanReference
func ChildOf(sc opentracing.SpanContext) opentracing.SpanReference {
	return opentracing.SpanReference{
		Type:              opentracing.ChildOfRef,
		ReferencedContext: sc,
	}
}

// FollowsFrom returns a StartSpanOption pointing to a parent Span that caused
// the child Span but does not directly depend on its result in any way.
// If sc == nil, the option has no effect.
//
// See FollowsFromRef, SpanReference
func FollowsFrom(sc opentracing.SpanContext) opentracing.SpanReference {
	return opentracing.SpanReference{
		Type:              opentracing.FollowsFromRef,
		ReferencedContext: sc,
	}
}

// StartTime is a StartSpanOption that sets an explicit start timestamp for the
// new Span.
type StartTime time.Time

// Apply satisfies the StartSpanOption interface.
func (t StartTime) Apply(o *opentracing.StartSpanOptions) {
	o.StartTime = time.Time(t)
}

// Tags are a generic map from an arbitrary string key to an opaque value type.
// The underlying tracing system is responsible for interpreting and
// serializing the values.
type Tags map[string]interface{}

// Apply satisfies the StartSpanOption interface.
func (t Tags) Apply(o *opentracing.StartSpanOptions) {
	if o.Tags == nil {
		o.Tags = make(map[string]interface{})
	}
	for k, v := range t {
		o.Tags[k] = v
	}
}

func (t Tags) ToSlice() (ret []string) {
	for k := range t {
		ret = append(ret, k+":"+fmt.Sprint(t[k]))
	}
	return
}

func (t Tags) Marshal() (ret []byte, err error) {
	ret, err = json.Marshal(t)
	return
}

// Tag may be passed as a StartSpanOption to add a tag to new spans,
// or its Set method may be used to apply the tag to an existing Span,
// for example:
//
// tracer.StartSpan("opName", Tag{"Key", value})
//
//   or
//
// Tag{"key", value}.Set(span)
type Tag struct {
	Key   string
	Value interface{}
}

// Apply satisfies the StartSpanOption interface.
func (t Tag) Apply(o *opentracing.StartSpanOptions) {
	if o.Tags == nil {
		o.Tags = make(map[string]interface{})
	}
	o.Tags[t.Key] = t.Value
}

// Set applies the tag to an existing Span.
func (t Tag) Set(s Span) {
	s.SetTag(t.Key, t.Value)
}

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
	//TODO report span
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

//StartSpanFromHTTPHeaderSafe starts and return a Span with `operationName` and http.Request
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

// SpanFromContext returns the `Span` previously associated with `ctx`, or
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

//CloseGlobalTracer closes global tracer gracefully
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
