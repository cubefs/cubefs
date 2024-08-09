package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var handler = &rpc2.Router{}

func init() {
	handler.Middleware(handleMiddleware1, handleMiddleware2)
	handler.Register("/ping", handlePing)
	handler.Register("/stream", handleStream)
}

func handleMiddleware1(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info("middleware-1")
	return nil
}

func handleMiddleware2(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info("middleware-2")
	return nil
}

func handlePing(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info(req.RequestHeader.ToString())
	var para pingPara
	para.Unmarshal(req.GetParameter())
	w.SetContentLength(req.ContentLength)
	buff := make([]byte, req.ContentLength)
	if _, err := io.ReadFull(req.Body, buff); err != nil {
		return err
	}
	if err := req.Body.Close(); err != nil {
		return err
	}
	w.WriteOK(&para)
	w.Header().Set("ignored", "x") // ignore
	log.Info(req.Trailer.M)
	w.Write(buff)
	return nil
}

func handleStream(_ rpc2.ResponseWriter, req *rpc2.Request) error {
	var para pingPara
	para.Unmarshal(req.GetParameter())
	para.S = "response -> " + para.S

	stream := rpc2.GenericServerStream[streamReq, streamResp]{ServerStream: req.ServerStream()}
	var header, trailer rpc2.Header
	header.Set("stream-header-a", "aaa")
	trailer.Set("stream-trailer-b", "")
	stream.SetHeader(header)
	stream.SetTrailer(trailer)
	stream.SendHeader(&para)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			trailer.Set("stream-trailer-b", "bbb")
			stream.SetTrailer(trailer)
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(&streamResp{"response -> " + req.str}); err != nil {
			return err
		}
	}
}

func runServer() {
	ln1, err := net.Listen("tcp", listenon[0])
	if err != nil {
		panic(err)
	}
	log.Info("listen on 1:", ln1.Addr().String())

	ln2, err := net.Listen("tcp", listenon[1])
	if err != nil {
		panic(err)
	}
	log.Info("listen on 2:", ln2.Addr().String())

	server := rpc2.Server{
		Name:         ln1.Addr().String() + " | " + ln2.Addr().String(),
		Handler:      handler,
		StatDuration: 3 * time.Second,
	}
	go func() {
		if err := server.Listen(ln1); err != nil && err != rpc2.ErrServerClosed {
			panic(err)
		}
	}()
	if err := server.Serve(ln2); err != nil && err != rpc2.ErrServerClosed {
		panic(err)
	}
	server.Shutdown(context.Background())
}
