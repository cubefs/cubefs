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

package ext

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestPeerTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace").(trace.Span)
	PeerService.Set(span, "my-service")
	PeerAddress.Set(span, "my-hostname:8080")
	PeerHostname.Set(span, "my-hostname")
	PeerHostIPv4.Set(span, uint32(127<<24|1))
	PeerHostIPv4.SetString(span, "127.0.0.1")
	PeerHostIPv6.Set(span, "::")
	PeerPort.Set(span, uint16(8080))
	SamplingPriority.Set(span, uint16(1))
	SpanKind.Set(span, SpanKindRPCServerEnum)
	SpanKindRPCClient.Set(span)
	span.Finish()

	require.Equal(t, trace.Tags{
		"peer.service":      "my-service",
		"peer.address":      "my-hostname:8080",
		"peer.hostname":     "my-hostname",
		"peer.ipv4":         "127.0.0.1",
		"peer.ipv6":         "::",
		"peer.port":         uint16(8080),
		"sampling.priority": uint16(1),
		"span.kind":         SpanKindRPCClientEnum,
	}, span.Tags())
}

func TestHTTPTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace", SpanKindRPCServer).(trace.Span)
	HTTPUrl.Set(span, "test.biz/uri?protocol=false")
	HTTPMethod.Set(span, "GET")
	HTTPStatusCode.Set(span, 301)
	span.Finish()

	require.Equal(t, trace.Tags{
		"http.url":         "test.biz/uri?protocol=false",
		"http.method":      "GET",
		"http.status_code": uint16(301),
		"span.kind":        SpanKindRPCServerEnum,
	}, span.Tags())
}

func TestDBTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace", SpanKindRPCClient).(trace.Span)
	DBInstance.Set(span, "127.0.0.1:3306/customers")
	DBStatement.Set(span, "SELECT * FROM user_table")
	DBType.Set(span, "sql")
	DBUser.Set(span, "customer_user")
	span.Finish()

	require.Equal(t, trace.Tags{
		"db.instance":  "127.0.0.1:3306/customers",
		"db.statement": "SELECT * FROM user_table",
		"db.type":      "sql",
		"db.user":      "customer_user",
		"span.kind":    SpanKindRPCClientEnum,
	}, span.Tags())
}

func TestMiscTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace").(trace.Span)
	Component.Set(span, "my-awesome-library")
	SamplingPriority.Set(span, 1)
	Error.Set(span, true)

	span.Finish()

	require.Equal(t, trace.Tags{
		"component":         "my-awesome-library",
		"sampling.priority": uint16(1),
		"error":             true,
	}, span.Tags())
}

func TestRPCServerOption(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	parent := tracer.StartSpan("my-trace")
	parent.SetBaggageItem("bag", "gage")

	carrier := trace.HTTPHeadersCarrier{}
	err := tracer.Inject(parent.Context(), trace.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tracer.Extract(trace.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessageBusProducerTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace", SpanKindProducer).(trace.Span)
	MessageBusDestination.Set(span, "topic name")
	span.Finish()

	require.Equal(t, trace.Tags{
		"message_bus.destination": "topic name",
		"span.kind":               SpanKindProducerEnum,
	}, span.Tags())
}

func TestMessageBusConsumerTags(t *testing.T) {
	tracer := trace.NewTracer("blobstore")
	span := tracer.StartSpan("my-trace", SpanKindConsumer).(trace.Span)
	MessageBusDestination.Set(span, "topic name")
	span.Finish()

	require.Equal(t, trace.Tags{
		"message_bus.destination": "topic name",
		"span.kind":               SpanKindConsumerEnum,
	}, span.Tags())
}
