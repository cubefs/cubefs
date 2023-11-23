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
	"github.com/opentracing/opentracing-go/ext"
)

var (
	// SpanKind (client/server or producer/consumer)
	SpanKind              = ext.SpanKind
	SpanKindRPCClientEnum = ext.SpanKindRPCClientEnum
	SpanKindRPCClient     = ext.SpanKindRPCClient
	SpanKindRPCServerEnum = ext.SpanKindRPCServerEnum
	SpanKindRPCServer     = ext.SpanKindRPCServer
	SpanKindProducerEnum  = ext.SpanKindProducerEnum
	SpanKindProducer      = ext.SpanKindProducer
	SpanKindConsumerEnum  = ext.SpanKindConsumerEnum
	SpanKindConsumer      = ext.SpanKindConsumer

	// Component name
	Component = ext.Component

	// Sampling hint
	SamplingPriority = ext.SamplingPriority

	// Peer tags
	PeerService  = ext.PeerService
	PeerAddress  = ext.PeerAddress
	PeerHostname = ext.PeerHostname
	PeerHostIPv4 = ext.PeerHostIPv4
	PeerHostIPv6 = ext.PeerHostIPv6
	PeerPort     = ext.PeerPort

	// HTTP tags
	HTTPUrl        = ext.HTTPUrl
	HTTPMethod     = ext.HTTPMethod
	HTTPStatusCode = ext.HTTPStatusCode

	// DB tags
	DBInstance  = ext.DBInstance
	DBStatement = ext.DBStatement
	DBType      = ext.DBType
	DBUser      = ext.DBUser

	// Message Bus Tag
	MessageBusDestination = ext.MessageBusDestination

	// Error Tag
	Error = ext.Error
)

// SpanKindEnum represents common span types
type SpanKindEnum = ext.SpanKindEnum
