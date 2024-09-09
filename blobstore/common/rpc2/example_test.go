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
	"bytes"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	mrand "math/rand"
)

type strMessage struct{ AnyCodec[string] }

func ExampleServer_base() {
	addr, cli, shutdown := newTcpServer()
	defer shutdown()
	cli.ConnectorConfig.BufioReaderSize = 4 << 20

	// Request & Response
	req, _ := NewRequest(testCtx, addr, "/", nil, nil)
	if err := cli.DoWith(req, nil); err != nil {
		fmt.Println(err)
	}

	// Stream
	sreq, _ := NewStreamRequest(testCtx, addr, "/stream", nil)
	sc := StreamClient[noneCodec, noneCodec]{cli}
	scli, _ := sc.Streaming(sreq, nil)
	waitc := make(chan struct{})
	go func() {
		for {
			_, err := scli.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				fmt.Println(err)
			}
		}
	}()
	for range [10]struct{}{} {
		if err := scli.Send(&noneCodec{}); err != nil {
			fmt.Println(err)
		}
	}
	scli.CloseSend()
	<-waitc

	// Output:
}

type strMessageUnread struct{ AnyCodec[string] }

func (s *strMessageUnread) Readable() bool { return false }

func handleMessage(w ResponseWriter, req *Request) error {
	var args strMessageUnread
	req.ParseParameter(&args)
	req.Body.Close()
	args.Value = "-> " + args.Value
	return w.WriteOK(&args)
}

func ExampleServer_request_message() {
	handler := &Router{}
	handler.Register("/", handleMessage)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()

	args := &strMessage{AnyCodec[string]{Value: "request message"}}
	// message in request & response body
	if err := cli.Request(testCtx, server.Name, "/", args, args); err != nil {
		fmt.Println(err)
	}
	fmt.Println(args.Value)

	// Output:
	// -> request message
}

func handleUpload(w ResponseWriter, req *Request) error {
	var args strMessage
	req.ParseParameter(&args)
	hasher := req.checksum.Hasher()
	if _, err := req.Body.WriteTo(LimitWriter(hasher, req.ContentLength)); err != nil {
		return err
	}
	args.Value = fmt.Sprint(req.checksum.Readable(hasher.Sum(nil)))
	req.Body.Close()
	return w.WriteOK(&args)
}

func ExampleServer_request_upload() {
	handler := &Router{}
	handler.Register("/", handleUpload)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()
	cli.ConnectorConfig.ConnectionWriteV = true

	args := &strMessage{AnyCodec[string]{Value: "request data"}}
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	// request message in parameter & response message in body
	req, _ := NewRequest(testCtx, server.Name, "/", args, bytes.NewReader(buff))
	req.OptionCrcUpload()
	req.ContentLength = int64(len(buff))
	if err := cli.DoWith(req, args); err != nil {
		fmt.Println(err)
	}
	hasher := req.checksum.Hasher()
	hasher.Write(buff)
	fmt.Println(args.Value == fmt.Sprint(req.checksum.Readable(hasher.Sum(nil))))

	// Output:
	// true
}

func handleUpDown(w ResponseWriter, req *Request) error {
	var args strMessage
	req.ParseParameter(&args)
	uhasher := req.checksum.Hasher()

	if _, err := req.Body.WriteTo(LimitWriter(uhasher, req.ContentLength)); err != nil {
		return err
	}
	req.Body.Close()

	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	dhasher := req.checksum.Hasher()
	dhasher.Write(buff)

	rr := req.checksum.Readable
	args.Value = fmt.Sprintf("%v %v", rr(uhasher.Sum(nil)), rr(dhasher.Sum(nil)))

	w.SetContentLength(int64(len(buff)))
	w.WriteHeader(200, &args)
	_, err := w.ReadFrom(bytes.NewReader(buff))
	return err
}

func ExampleServer_request_updown() {
	handler := &Router{}
	handler.Register("/", handleUpDown)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()
	cli.ConnectorConfig.BufioReaderSize = 4 << 20
	cli.ConnectorConfig.ConnectionWriteV = true

	args := &strMessage{AnyCodec[string]{Value: "upload & download"}}
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	// request & response message in parameter
	req, _ := NewRequest(testCtx, server.Name, "/", args, bytes.NewReader(buff))
	req.Option(func(r *Request) { _ = req.RequestHeader.GoString() })
	req.OptionCrc()
	req.ContentLength = int64(len(buff))

	uhasher := req.checksum.Hasher()
	uhasher.Write(buff)
	dhasher := req.checksum.Hasher()

	resp, _ := cli.Do(req, args)
	defer resp.Body.Close()

	b := make([]byte, 32<<20)
	for {
		n, err := resp.Body.Read(b)
		dhasher.Write(b[:n])
		if err == io.EOF {
			break
		}
	}
	rr := req.checksum.Readable
	fmt.Println(args.Value == fmt.Sprintf("%v %v", rr(uhasher.Sum(nil)), rr(dhasher.Sum(nil))))

	// Output:
	// true
}

func handleTrailer(w ResponseWriter, req *Request) error {
	hasher := md5.New()
	req.Body.WriteTo(LimitWriter(hasher, req.ContentLength))
	req.Body.Close()

	w.Header().Merge(req.Header)
	w.Header().Add("add", "header-stable")
	w.Trailer().Add("add", "trailer-stable")
	w.Trailer().SetLen("md5", 32)

	w.AfterBody(func() error { return nil })
	w.AfterBody(func() error {
		w.Trailer().Set("md5", hex.EncodeToString(hasher.Sum(nil)))
		return nil
	})
	return w.WriteOK(nil)
}

func ExampleServer_trailer() {
	handler := &Router{}
	handler.Register("/", handleTrailer)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()

	hasher := md5.New()
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)

	req, _ := NewRequest(testCtx, server.Name, "/", nil,
		io.TeeReader(bytes.NewReader(buff), hasher))
	req.ContentLength = int64(len(buff))
	req.Header.Add("add", "x")
	req.Trailer.SetLen("md5", 32)
	req.AfterBody = func() error {
		req.Trailer.Set("md5", hex.EncodeToString(hasher.Sum(nil)))
		return nil
	}

	resp, _ := cli.Do(req, nil)
	resp.Body.Close()
	fmt.Println(resp.Header.Get("add"))
	fmt.Println(resp.Trailer.Get("add"))
	fmt.Println(hex.EncodeToString(hasher.Sum(nil)) == resp.Trailer.Get("md5"))

	// Output:
	// header-stable
	// trailer-stable
	// true
}
