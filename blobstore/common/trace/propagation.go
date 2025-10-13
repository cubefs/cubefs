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
	"bytes"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
)

const (
	prefixTracer = "blobstore-tracer-"

	tracerFieldCount = 2
)

// Propagate with those keys.
// Define your own key by setting those variables before your application started.
var (
	RequestIDKey = "X-Reqid"

	PrefixBaggage   = "blobstore-baggage-"
	FieldKeyTraceID = prefixTracer + "traceid"
	FieldKeySpanID  = prefixTracer + "spanid"
)

var (
	// ErrUnsupportedFormat is the alias of opentracing.ErrUnsupportedFormat.
	ErrUnsupportedFormat = opentracing.ErrUnsupportedFormat

	// ErrSpanContextNotFound is the alias of opentracing.ErrSpanContextNotFound.
	ErrSpanContextNotFound = opentracing.ErrSpanContextNotFound

	// ErrInvalidSpanContext is the alias of opentracing.ErrInvalidSpanContext.
	ErrInvalidSpanContext = opentracing.ErrInvalidSpanContext

	// ErrInvalidCarrier is the alias of opentracing.ErrInvalidCarrier.
	ErrInvalidCarrier = opentracing.ErrInvalidCarrier

	// ErrSpanContextCorrupted is the alias of opentracing.ErrSpanContextCorrupted.
	ErrSpanContextCorrupted = opentracing.ErrSpanContextCorrupted
)

const (
	// Binary is the alias of opentracing.Binary.
	Binary = opentracing.Binary

	// TextMap is the alias of opentracing.TextMap.
	TextMap = opentracing.TextMap

	// HTTPHeaders is the alias of opentracing.HTTPHeaders.
	HTTPHeaders = opentracing.HTTPHeaders
)

// TextMapCarrier is the alias of opentracing.TextMapCarrier.
type TextMapCarrier = opentracing.TextMapCarrier

// HTTPHeadersCarrier is the alias of opentracing.HTTPHeadersCarrier.
type HTTPHeadersCarrier = opentracing.HTTPHeadersCarrier

// TextMapPropagator is a combined Injector and Extractor for TextMap format.
type TextMapPropagator struct{}

var defaultTexMapPropagator = TextMapPropagator{}

// Inject implements Injector of TextMapPropagator
func (t *TextMapPropagator) Inject(sc *SpanContext, carrier interface{}) error {
	writer, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return ErrInvalidCarrier
	}
	writer.Set(FieldKeyTraceID, sc.traceID())
	writer.Set(FieldKeySpanID, sc.spanID.String())

	sc.ForeachBaggageItems(func(k string, buffers []*bytes.Buffer) bool {
		if k != internalTrackLogKey { // internal baggage will not inject
			if len(buffers) == 1 {
				writer.Set(PrefixBaggage+k, buffers[0].String())
				return true
			}
			buf := strings.Builder{}
			buf.Reset()
			for idx, b := range buffers {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.Write(b.Bytes())
			}
			writer.Set(PrefixBaggage+k, buf.String())
		}
		return true
	})
	return nil
}

// Extract implements Extractor of TextMapPropagator.
func (t *TextMapPropagator) Extract(carrier interface{}) (opentracing.SpanContext, error) {
	reader, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return nil, ErrInvalidCarrier
	}
	var (
		traceID    string
		spanID     ID
		fieldCount int
		err        error
	)
	err = reader.ForeachKey(func(key, val string) error {
		switch strings.ToLower(key) {
		case FieldKeyTraceID:
			traceID = val
			fieldCount++
		case FieldKeySpanID:
			id, err := strconv.ParseUint(val, 16, 64)
			if err != nil {
				return ErrSpanContextCorrupted
			}
			spanID = ID(id)
			fieldCount++
		default:
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if fieldCount == 0 {
		return nil, ErrSpanContextNotFound
	}
	if fieldCount < tracerFieldCount {
		return nil, ErrSpanContextCorrupted
	}

	span := poolSpan.Get().(*spanImpl)
	reader.ForeachKey(func(key, val string) error {
		switch strings.ToLower(key) {
		case FieldKeyTraceID, FieldKeySpanID:
		default:
			lowerKey := strings.ToLower(key)
			if strings.HasPrefix(lowerKey, PrefixBaggage) {
				k := strings.TrimPrefix(lowerKey, PrefixBaggage)
				if b := span.context.nextBuffer(k, 8); b != nil {
					b.WriteString(val)
				}
			}
		}
		return nil
	})
	span.context.spanID = spanID
	span.context.resetID(traceID)
	// span.context.spanFromPool = span // notify release to pool // TODO: cacheable later
	return span.context, nil
}

// GetTraceIDKey returns http header name of traceid
func GetTraceIDKey() string {
	return FieldKeyTraceID
}
