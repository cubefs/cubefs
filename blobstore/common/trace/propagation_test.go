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
	"context"
	"net/http"
	"testing"

	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
)

func TestSpanPropagator(t *testing.T) {
	tracer := NewTracer("blobstore")
	defer tracer.Close()
	SetGlobalTracer(tracer)

	span, _ := StartSpanFromContext(context.Background(), "test baggage")
	defer span.Finish()

	span.SetBaggageItem("k1", "v1")

	carriers := []struct {
		carrierType interface{}
		carrier     interface{}
	}{
		{HTTPHeaders, HTTPHeadersCarrier(http.Header{})},
		{TextMap, TextMapCarrier(make(map[string]string))},
	}

	for _, c := range carriers {
		err := span.Tracer().Inject(span.Context(), c.carrierType, c.carrier)
		require.NoError(t, err)

		sp, err := Extract(c.carrierType, c.carrier)
		require.NoError(t, err)

		child := tracer.StartSpan("child", ChildOf(sp))
		require.Equal(t, "v1", child.BaggageItem("k1"))
		require.Equal(t, span.Context().(*SpanContext).traceID, child.Context().(*SpanContext).traceID)
		require.Equal(t, span.Context().(*SpanContext).spanID, child.Context().(*SpanContext).parentID)
		child.Finish()
	}

	err := span.Tracer().Inject(span.Context(), Binary, &bytes.Buffer{})
	require.EqualError(t, err, ErrUnsupportedFormat.Error())
	_, err = Extract(Binary, &bytes.Buffer{})
	require.EqualError(t, err, ErrUnsupportedFormat.Error())

	err = tracer.Inject(mocktracer.MockSpanContext{}, Binary, &bytes.Buffer{})
	require.EqualError(t, err, ErrInvalidSpanContext.Error())

	err = defaultTexMapPropagator.Inject(span.(*spanImpl).context, &bytes.Buffer{})
	require.EqualError(t, err, ErrInvalidCarrier.Error())
	_, err = defaultTexMapPropagator.Extract(&bytes.Buffer{})
	require.EqualError(t, err, ErrInvalidCarrier.Error())

	_, err = defaultTexMapPropagator.Extract(HTTPHeadersCarrier(http.Header{}))
	require.Error(t, err)

	_, err = defaultTexMapPropagator.Extract(HTTPHeadersCarrier(http.Header{FieldKeySpanID: []string{"abcdefg"}}))
	require.ErrorIs(t, err, ErrSpanContextCorrupted)
	_, err = defaultTexMapPropagator.Extract(HTTPHeadersCarrier(http.Header{FieldKeySpanID: []string{"abcdeff"}}))
	require.ErrorIs(t, err, ErrSpanContextCorrupted)

	require.Equal(t, FieldKeyTraceID, GetTraceIDKey())
}

func TestSpanPropagateWith(t *testing.T) {
	oldPrefixBaggage := PrefixBaggage
	oldFieldKeyTraceID := FieldKeyTraceID
	defer func() {
		PrefixBaggage = oldPrefixBaggage
		FieldKeyTraceID = oldFieldKeyTraceID
	}()
	PrefixBaggage = "test-bbb-"
	FieldKeyTraceID = "xxx-traceid"

	sci, err := defaultTexMapPropagator.Extract(HTTPHeadersCarrier(http.Header{
		FieldKeySpanID: []string{"10000"},
		"xxx-traceid":  []string{"Propagator-ID"},
		"test-bbb-b1":  []string{"foo"},
		"test-bbb-b2":  []string{"bar", "foo", "bar"},
	}))
	require.NoError(t, err)
	sc := sci.(*SpanContext)

	require.Equal(t, "Propagator-ID", sc.traceID)
	require.Equal(t, ID(1<<16), sc.spanID)
	require.Equal(t, 2, len(sc.baggage))
	require.Equal(t, []string{"foo"}, sc.baggageItem("b1"))
	require.Equal(t, []string{"bar", "foo", "bar"}, sc.baggageItem("b2"))

	require.Equal(t, FieldKeyTraceID, GetTraceIDKey())
}
