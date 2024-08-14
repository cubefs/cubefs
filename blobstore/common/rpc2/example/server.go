package main

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var handler = &rpc2.Router{}

func init() {
	handler.Middleware(handleMiddleware1, handleMiddleware2)
	handler.Register("/ping", handlePing)
	handler.Register("/kick", handleKick)
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
	req.ParseParameter(&para)

	resp := bytes.NewBuffer(nil)
	resp.WriteString("response -> ")
	w.SetContentLength(int64(resp.Len()) + req.ContentLength)
	buff := make([]byte, req.ContentLength)
	if _, err := io.ReadFull(req.Body, buff); err != nil {
		return err
	}
	req.Body.Close()
	log.Info("body   :", string(buff))
	log.Info("trailer:", req.Trailer.M)
	w.Trailer().SetLen("server-trailer", 3)
	w.AfterBody(func() error {
		log.Info("run after body stack - 1")
		w.Trailer().Set("server-trailer", "123")
		return nil
	})
	w.AfterBody(func() error {
		log.Info("run after body stack - 2")
		return nil
	})

	w.WriteHeader(200, &para)
	w.Header().Set("ignored", "x") // ignore
	resp.Write(buff)
	_, err := w.ReadFrom(resp)
	return err
}

func handleKick(w rpc2.ResponseWriter, req *rpc2.Request) error {
	var para pingPara
	req.ParseParameter(&para)
	para.S = "response -> " + para.S
	return w.WriteOK(&para)
}

func handleStream(_ rpc2.ResponseWriter, req *rpc2.Request) error {
	var para pingPara
	req.ParseParameter(&para)
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
			trailer.Set("stream-trailer-x", "another")
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
	server := rpc2.Server{
		Name: listenon[0] + " | " + listenon[1],
		Addresses: []rpc2.NetworkAddress{
			{Network: "tcp", Address: listenon[0]},
			{Network: "tcp", Address: listenon[1]},
		},
		Handler:      handler,
		StatDuration: 3 * time.Second,
	}
	if err := server.Serve(); err != nil && err != rpc2.ErrServerClosed {
		panic(err)
	}
	server.Shutdown(context.Background())
}
