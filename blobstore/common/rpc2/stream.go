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

import (
	"context"
	"io"
)

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

type Codec interface {
	Marshaler
	Unmarshaler
}

type ClientStream interface {
	Context() context.Context

	Header() (Header, error)
	Trailer() Header

	CloseSend() error

	SendMsg(a any) error
	RecvMsg(a any) error
}

type ServerStream interface {
	Context() context.Context

	SetHeader(Header) error
	SendHeader(obj Marshaler) error
	SetTrailer(Header)

	SendMsg(a any) error
	RecvMsg(a any) error
}

// TO implements

// type ServerStreamingClient[Res any] interface {
// 	Recv() (Res, error)
// 	ClientStream
// }

// type ServerStreamingServer[Res any] interface {
// 	Send(Res) error
// 	ServerStream
// }

// type ClientStreamingClient[Req any, Res any] interface {
// 	Send(Req) error
// 	CloseAndRecv() (Res, error)
// 	ClientStream
// }

// type ClientStreamingServer[Req any, Res any] interface {
// 	Recv() (Req, error)
// 	SendAndClose(Res) error
// 	ServerStream
// }

// StreamingClient represents the client side of a bidirectional-streaming
// (many requests, many responses) RPC. It is generic over both the type of the
// request message stream and the type of the response message stream.
type StreamingClient[Req any, Res any] interface {
	Send(*Req) error
	Recv() (*Res, error)
	ClientStream
}

// StreamingServer represents the server side of a bidirectional-streaming
// (many requests, many responses) RPC. It is generic over both the type of the
// request message stream and the type of the response message stream.
type StreamingServer[Req any, Res any] interface {
	Recv() (*Req, error)
	Send(*Res) error
	ServerStream
}

type noneCodec struct{}

func (*noneCodec) Marshal() ([]byte, error) { return nil, nil }
func (*noneCodec) Unmarshal([]byte) error   { return nil }

var _ Codec = (*noneCodec)(nil)

type GenericClientStream[Req any, Res any] struct {
	ClientStream
}

func (x *GenericClientStream[Req, Res]) Send(msg *Req) error {
	return x.ClientStream.SendMsg(msg)
}

func (x *GenericClientStream[Req, Res]) Recv() (*Res, error) {
	msg := new(Res)
	if err := x.ClientStream.RecvMsg(msg); err != nil {
		return msg, err
	}
	return msg, nil
}

var _ StreamingClient[noneCodec, noneCodec] = (*GenericClientStream[noneCodec, noneCodec])(nil)

type GenericServerStream[Req any, Res any] struct {
	ServerStream
}

var _ StreamingServer[noneCodec, noneCodec] = (*GenericServerStream[noneCodec, noneCodec])(nil)

func (x *GenericServerStream[Req, Res]) Send(msg *Res) error {
	return x.ServerStream.SendMsg(msg)
}

func (x *GenericServerStream[Req, Res]) Recv() (*Req, error) {
	msg := new(Req)
	if err := x.ServerStream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

type clientStream struct {
	req     *Request
	header  Header // response header
	trailer Header // response trailer
}

var _ ClientStream = (*clientStream)(nil)

func (cs *clientStream) Context() context.Context {
	return cs.req.Context()
}

func (cs *clientStream) Header() (Header, error) {
	return cs.header, nil
}

func (cs *clientStream) Trailer() Header {
	return cs.trailer
}

func (cs *clientStream) CloseSend() error {
	req := cs.newRequest()
	req.StreamCmd = StreamCmd_FIN
	req.Trailer = cs.trailer.ToFixedHeader()
	return req.write(req.cli.requestDeadline(req.ctx))
}

func (cs *clientStream) SendMsg(a any) error {
	msg, is := a.(Codec)
	if !is {
		panic("rpc2: stream send message must implement rpc2.Codec")
	}
	b, err := msg.Marshal()
	if err != nil {
		return err
	}

	req := cs.newRequest()
	req.StreamCmd = StreamCmd_PSH
	req.Parameter = b
	req.Body = NoBody
	return req.write(req.cli.requestDeadline(req.ctx))
}

func (cs *clientStream) RecvMsg(a any) (err error) {
	msg, is := a.(Codec)
	if !is {
		panic("rpc2: stream recv message must implement rpc2.Codec")
	}
	conn := cs.req.conn

	var resp ResponseHeader
	frame, err := readHeaderFrame(conn, &resp)
	if err != nil {
		return
	}
	defer func() {
		if errClose := frame.Close(); err == nil {
			err = errClose
		}
	}()

	if resp.Status > 0 { // end
		cs.trailer = resp.Trailer.ToHeader()
		cs.req.conn.Close()
		if resp.Status != 200 {
			return &Error{
				Status: resp.Status,
				Reason: resp.Reason,
				Detail: resp.Error,
			}
		}
		return io.EOF
	}

	err = msg.Unmarshal(resp.Parameter)
	return
}

func (cs *clientStream) newRequest() *Request {
	req := baseRequest()
	req.ctx = cs.req.ctx
	req.cli = cs.req.cli
	req.conn = cs.req.conn
	return req
}

type serverStream struct {
	req *Request

	hdr ResponseHeader
}

var _ ServerStream = (*serverStream)(nil)

func (ss *serverStream) Context() context.Context {
	return ss.req.Context()
}

func (ss *serverStream) SetHeader(h Header) error {
	ss.hdr.Header.Merge(h)
	return nil
}

func (ss *serverStream) SendHeader(obj Marshaler) error {
	if obj == nil {
		obj = NoParameter
	}
	b, err := obj.Marshal()
	if err != nil {
		return err
	}
	ss.hdr.Parameter = b
	return writeHeaderFrame(ss.req.conn, &ss.hdr)
}

func (ss *serverStream) SetTrailer(h Header) {
	ss.hdr.Trailer.MergeHeader(h)
}

func (ss *serverStream) SendMsg(a any) error {
	msg, is := a.(Codec)
	if !is {
		panic("rpc2: stream send message must implement rpc2.Codec")
	}
	b, err := msg.Marshal()
	if err != nil {
		return err
	}

	resp := baseResponse()
	hdr := resp.ResponseHeader
	hdr.Parameter = b
	return writeHeaderFrame(ss.req.conn, &hdr)
}

func (ss *serverStream) RecvMsg(a any) (err error) {
	msg, is := a.(Codec)
	if !is {
		panic("rpc2: stream recv message must implement rpc2.Codec")
	}
	_ = msg
	var req RequestHeader
	frame, err := readHeaderFrame(ss.req.conn, &req)
	if err != nil {
		return
	}
	defer func() {
		if errClose := frame.Close(); err == nil {
			err = errClose
		}
	}()

	if req.StreamCmd == StreamCmd_FIN {
		err = io.EOF
		return
	}

	err = msg.Unmarshal(req.GetParameter())
	return
}
