// Copyright 2024 The CubeFS Authors.
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

package rpc2

import "context"

type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

type ClientStream interface {
	Context() context.Context

	Header() (Header, error)
	Trailer() Header

	CloseSend() error

	SendMsg(a Codec) error
	RecvMsg(a Codec) error
}

type ServerStream interface {
	Context() context.Context

	SetHeader(Header) error
	SendHeader(Header) error
	SetTrailer(Header)

	SendMsg(a Codec) error
	RecvMsg(a Codec) error
}

type (
	Res interface{}
	Req interface{}
)

// ServerStreamingClient represents the client side of a server-streaming (one
// request, many responses) RPC. It is generic over the type of the response
// message. It is used in generated code.
type ServerStreamingClient interface {
	Recv() (Res, error)
	ClientStream
}

// ServerStreamingServer represents the server side of a server-streaming (one
// request, many responses) RPC. It is generic over the type of the response
// message. It is used in generated code.
type ServerStreamingServer interface {
	Send(Res) error
	ServerStream
}

// ClientStreamingClient represents the client side of a client-streaming (many
// requests, one response) RPC. It is generic over both the type of the request
// message stream and the type of the unary response message. It is used in
// generated code.
type ClientStreamingClient interface {
	Send(Req) error
	CloseAndRecv() (Res, error)
	ClientStream
}

// ClientStreamingServer represents the server side of a client-streaming (many
// requests, one response) RPC. It is generic over both the type of the request
// message stream and the type of the unary response message. It is used in
// generated code.
type ClientStreamingServer interface {
	Recv() (Req, error)
	SendAndClose(Res) error
	ServerStream
}

// BidiStreamingClient represents the client side of a bidirectional-streaming
// (many requests, many responses) RPC. It is generic over both the type of the
// request message stream and the type of the response message stream. It is
// used in generated code.
type BidiStreamingClient interface {
	Send(Req) error
	Recv() (Res, error)
	ClientStream
}

// BidiStreamingServer represents the server side of a bidirectional-streaming
// (many requests, many responses) RPC. It is generic over both the type of the
// request message stream and the type of the response message stream. It is
// used in generated code.
type BidiStreamingServer interface {
	Recv() (Req, error)
	Send(Res) error
	ServerStream
}

type Message struct {
	Version uint8
	Magic   uint8
	Fin     bool

	ContentLength int
}
