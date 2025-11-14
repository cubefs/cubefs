package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
)

var (
	client = flag.Bool("client", false, "server or client")
	addr   = flag.String("addr", "127.0.0.1:8888", "bind address")

	// client
	connection  = flag.Int("connection", 1, "num of connections")
	concurrence = flag.Int("concurrence", 1, "num of concurrence per connection")
)

var transportConfig = &rpc2.TransportConfig{
	Version:          2,
	MaxFrameSize:     128 << 10,
	MaxReceiveBuffer: 16 * (1 << 20),
	MaxStreamBuffer:  16 << 20,

	KeepAliveInterval: util.Duration{Duration: time.Second},
	KeepAliveTimeout:  util.Duration{Duration: 30 * time.Second},
	KeepAliveDisabled: true,
}

func newCtx() context.Context {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	return ctx
}

func runServer() {
	var handler rpc2.Router
	handler.Middleware(
		func(_ rpc2.ResponseWriter, req *rpc2.Request) error {
			req.Span().Infof("middle-1: [%d]%s", req.RemotePathIndex, req.RemotePath)
			return nil
		},
		func(_ rpc2.ResponseWriter, req *rpc2.Request) error {
			if req.RemotePathIndex == 100 {
				return rpc2.NewError(409, "demo: CustomStop", "")
			}
			req.Span().Infof("middle-2: [%d]%s", req.RemotePathIndex, req.RemotePath)
			return nil
		},
	)
	handler.Register("/middle", func(w rpc2.ResponseWriter, req *rpc2.Request) error {
		panic("should not be here")
	})
	handler.Register("/ping", func(w rpc2.ResponseWriter, req *rpc2.Request) error {
		req.Span().Infof("ping remote: %s", req.RemoteAddrString())
		return w.WriteOK(nil)
	})
	handler.Register("/error", func(_ rpc2.ResponseWriter, _ *rpc2.Request) error {
		return rpc2.NewError(567, "demo: CustomError", "")
	})
	handler.Register("/kick", func(w rpc2.ResponseWriter, req *rpc2.Request) error {
		n, err := req.Body.WriteTo(rpc2.LimitWriter(io.Discard, req.ContentLength))
		if err != nil {
			return err
		}
		if n != req.ContentLength {
			return fmt.Errorf("invalid body")
		}
		req.Span().Infof("kick remote: %s len=%d", req.RemoteAddrString(), req.ContentLength)
		return w.WriteOK(nil)
	})
	server := rpc2.Server{
		Name: *addr,
		Addresses: []rpc2.NetworkAddress{
			{Network: "tcp", Address: *addr},
		},
		Transport: transportConfig,
		Handler:   handler.MakeHandler(),
	}
	server.Serve()
}

func runClient() {
	var wg sync.WaitGroup
	conn := *connection
	conc := *concurrence
	wg.Add(conn * conc)
	if conn == 1 {
		doConnection(true)
	} else if conn > 1 {
		for ii := 0; ii < conn; ii++ {
			first := ii == 0
			go doConnection(first)
		}
	}
	wg.Wait()
}

func doConnection(first bool) {
	client := &rpc2.Client{
		MapPathIndex: map[string]int32{
			"/ping":     1,
			"/kick":     2,
			"/error":    3,
			"/middle":   100,
			"/notfound": 404,
		},
		ConnectorConfig: rpc2.ConnectorConfig{
			Transport:            transportConfig,
			Network:              "tcp",
			MaxSessionPerAddress: 1,
		},
		Retry: 1,
	}

	if first {
		runPath := func(path string, code int) {
			req, err := rpc2.NewRequest(newCtx(), *addr, path, nil, nil)
			if err != nil {
				panic(err)
			}
			client.FillPathIndex(req)
			fmt.Println(req)
			err = client.DoWith(req, nil)
			if rpc2.DetectStatusCode(err) != code {
				panic(err)
			}
		}
		runPath("/middle", 409)
		runPath("/notfound", 404)
		runPath("/error", 567)

		if err := client.Request(newCtx(), *addr, "/ping", rpc2.NoParameter, nil); err != nil {
			panic(err)
		}
	}

	conc := *concurrence
	if conc == 1 {
		doConcurrence(client)
	} else if conc > 1 {
		for ii := 0; ii < conc; ii++ {
			go doConcurrence(client)
		}
	}
}

func doConcurrence(client *rpc2.Client) {
	l := 4 << 10
	buff := make([]byte, l)
	ctx := newCtx()
	for {
		time.Sleep(time.Second)
		r := bytes.NewReader(buff)
		req, err := rpc2.NewRequest(ctx, *addr, "/kick", nil, r)
		if err != nil {
			panic(err)
		}
		client.FillPathIndex(req)
		req.ContentLength = int64(l)
		if err = client.DoWith(req, nil); err != nil {
			panic(err)
		}
	}
}

func main() {
	flag.Parse()
	if !*client {
		runServer()
	}
	runClient()
}
