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
	"io"
	"testing"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/stretchr/testify/require"
)

type (
	streamReq  = strMessage
	streamResp = strMessage
)

func handleStreamFull(_ ResponseWriter, req *Request) error {
	var para strMessage
	req.ParseParameter(&para)

	var header, trailer Header
	header.Set("stream-header", "aaa")
	trailer.Set("stream-trailer", "")

	stream := GenericServerStream[streamReq, streamResp]{ServerStream: req.ServerStream()}
	stream.Context()
	stream.SetHeader(header)
	stream.SetTrailer(trailer)
	stream.SendHeader(&para)
	defer func() {
		trailer.Set("stream-trailer", "bbb")
		stream.SetTrailer(trailer)
	}()
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.str == "error" {
			return &Error{Status: 500}
		}
		if err = stream.Send(&streamResp{msg.str}); err != nil {
			return err
		}
	}
}

func TestStreamBase(t *testing.T) {
	var tc *TransportConfig
	require.Nil(t, tc.Transport())

	handler := &Router{}
	handler.Register("/", handleStreamFull)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()
	sc := StreamClient[streamReq, streamResp]{Client: cli}

	{
		ecc := &clientStream{}
		require.Panics(t, func() { ecc.RecvMsg(struct{}{}) })
		require.Panics(t, func() { ecc.SendMsg(struct{}{}) })
		esc := &serverStream{}
		require.Panics(t, func() { esc.RecvMsg(struct{}{}) })
		require.Panics(t, func() { esc.SendMsg(struct{}{}) })
	}

	para := strMessage{"para"}
	req, err := NewStreamRequest(testCtx, server.Name, "/", &para)
	require.NoError(t, err)

	cc, err := sc.Streaming(req, &para)
	require.NoError(t, err)
	header, _ := cc.Header()
	require.Equal(t, "aaa", header.Get("stream-header"), cc.Context())
	trailer := cc.Trailer()
	require.Equal(t, "", trailer.Get("stream-trailer"))

	waitc := make(chan struct{})
	go func() {
		for {
			_, errx := cc.Recv()
			if errx != nil {
				close(waitc)
				return
			}
		}
	}()
	for idx := range [10]struct{}{} {
		req := streamReq{str: fmt.Sprintf("request-%d", idx)}
		if idx == 7 {
			req.str = "error"
		}
		require.NoError(t, cc.Send(&req))
	}
	cc.CloseSend()
	<-waitc
	require.Equal(t, "bbb", trailer.Get("stream-trailer"))
}
