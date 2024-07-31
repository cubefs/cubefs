package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type handler struct{}

func (h *handler) Handle(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info(req.RequestHeader)
	w.WriteHeader(200)
	buff := make([]byte, req.ContentLength)
	io.ReadFull(req.Body, buff)
	w.Write(buff)
	return nil
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
		Handler:      &handler{},
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
