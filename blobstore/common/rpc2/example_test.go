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
	"hash/crc32"
	"io"
	mrand "math/rand"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
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
	hasher := req.checksum.Hasher()
	hasher.Write(buff)
	if err := cli.DoWith(req, args); err != nil {
		fmt.Println(err)
	}
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
	rr := req.checksum.Readable

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
	fmt.Println(args.Value == fmt.Sprintf("%v %v", rr(uhasher.Sum(nil)), rr(dhasher.Sum(nil))))

	// Output:
	// true
}

type alignedWriter struct {
	buff []byte
}

func (w *alignedWriter) Write(p []byte) (int, error) {
	addr := uintptr(unsafe.Pointer(&p[0]))
	if addr%_checksumAlignment != 0 {
		panic("not aligned address")
	}
	w.buff = append(w.buff, p...)
	return len(p), nil
}

func (w *alignedWriter) Read(p []byte) (int, error) {
	if len(w.buff) == 0 {
		return 0, io.EOF
	}
	addr := uintptr(unsafe.Pointer(&p[0]))
	if addr%_checksumAlignment != 0 {
		panic("not aligned address")
	}
	n := copy(p, w.buff)
	w.buff = w.buff[n:]
	return n, nil
}

func handleAligned(w ResponseWriter, req *Request) error {
	var args strMessage
	req.ParseParameter(&args)
	writer := new(alignedWriter)
	if _, err := req.Body.WriteTo(LimitWriter(writer, req.ContentLength)); err != nil {
		return err
	}
	w.SetContentLength(req.ContentLength)
	w.WriteHeader(200, &args)
	_, err := w.ReadFrom(bytes.NewReader(writer.buff))
	return err
}

func ExampleServer_request_aligned() {
	handler := &Router{}
	handler.Register("/", handleAligned)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()

	args := &strMessage{AnyCodec[string]{Value: "body aligned upload & download"}}
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	req, _ := NewRequest(testCtx, server.Name, "/", args, bytes.NewReader(buff))
	req.OptionChecksum(ChecksumBlock{
		Algorithm: ChecksumAlgorithm_Crc_IEEE,
		Direction: ChecksumDirection_Duplex,
		BlockSize: DefaultBlockSize,
		Aligned:   true,
	})
	req.OptionBodyAligned()
	req.ContentLength = int64(len(buff))

	uhasher := req.checksum.Hasher()
	uhasher.Write(buff)

	resp, _ := cli.Do(req, args)
	defer resp.Body.Close()

	writer := new(alignedWriter)
	for {
		_, err := resp.Body.WriteTo(LimitWriter(writer, resp.ContentLength))
		if err == io.EOF {
			break
		}
	}
	dhasher := req.checksum.Hasher()
	dhasher.Write(writer.buff)

	rr := req.checksum.Readable
	fmt.Println(rr(uhasher.Sum(nil)) == rr(dhasher.Sum(nil)))

	// Output:
	// true
}

func handleWriteBody(w ResponseWriter, req *Request) error {
	var args strMessage
	req.ParseParameter(&args)
	reader := new(alignedWriter)
	if _, err := req.Body.WriteTo(LimitWriter(reader, req.ContentLength)); err != nil {
		return err
	}
	w.SetContentLength(req.ContentLength - 10 - 11)

	w.Trailer().SetLen("crc", 8)
	hasher := crc32.NewIEEE()
	w.AfterBody(func() error {
		w.Trailer().Set("crc", hex.EncodeToString(hasher.Sum(nil)))
		return nil
	})

	w.WriteHeader(200, &args)
	_, err := w.WriteBody(func(_ ChecksumBlock, conn *transport.Stream) (int64, error) {
		_, err := conn.RangedWrite(testCtx, reader, int(req.ContentLength), 10, 11, true,
			func(data []byte) error {
				hasher.Write(data)
				return nil
			})
		return req.ContentLength - 10 - 11, err
	})
	return err
}

func ExampleServer_response_write_body() {
	handler := &Router{}
	handler.Register("/", handleWriteBody)
	server, cli, shutdown := newServer("tcp", handler)
	defer shutdown()

	args := &strMessage{AnyCodec[string]{Value: "body aligned upload & download"}}
	buff := make([]byte, mrand.Intn(4<<20)+1<<20)
	crand.Read(buff)
	req, _ := NewRequest(testCtx, server.Name, "/", args, bytes.NewReader(buff))
	req.OptionChecksum(ChecksumBlock{
		Algorithm: ChecksumAlgorithm_Crc_IEEE,
		Direction: ChecksumDirection_Upload,
		BlockSize: DefaultBlockSize,
		Aligned:   true,
	})
	req.OptionBodyAligned()
	req.ContentLength = int64(len(buff))

	resp, _ := cli.Do(req, args)
	defer resp.Body.Close()
	dhasher := crc32.NewIEEE()
	for {
		_, err := resp.Body.WriteTo(LimitWriter(dhasher, resp.ContentLength))
		if err != nil {
			break
		}
	}
	fmt.Println(hex.EncodeToString(dhasher.Sum(nil)) == resp.Trailer.Get("crc"))

	// Output:
	// true
}

func handleTrailer(w ResponseWriter, req *Request) error {
	hasher := md5.New()
	req.Body.WriteTo(LimitWriter(hasher, req.ContentLength))

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
