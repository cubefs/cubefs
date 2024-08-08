package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

func rpc2HandleUpload(w rpc2.ResponseWriter, req *rpc2.Request) error {
	if *discard {
		n, _ := req.Body.WriteTo(rpc2.LimitWriter(io.Discard, req.ContentLength))
		atomic.AddInt64(&readn, int64(n))
	} else {
		buff := pool.Get().([]byte)[:req.ContentLength]
		n, _ := io.ReadFull(req.Body, buff)
		pool.Put(buff) // nolint: staticcheck
		atomic.AddInt64(&readn, int64(n))
	}
	return w.WriteOK(nil)
}

func serverRpc2() {
	var rpc2Handler rpc2.Router
	rpc2Handler.Register("/", rpc2HandleUpload)
	server := rpc2.Server{
		Name: *addr,
		Addresses: []rpc2.NetworkAddress{
			{Network: "tcp", Address: *addr},
		},
		Transport:       config.Transport,
		BufioReaderSize: *connbuffer,
		Handler:         rpc2Handler.MakeHandler(),
	}
	server.Serve()
}

func handleUpload(c *rpc.Context) {
	if *discard {
		n, _ := io.Copy(io.Discard, c.Request.Body)
		atomic.AddInt64(&readn, int64(n))
	} else {
		buff := pool.Get().([]byte)[:c.Request.ContentLength]
		n, _ := io.ReadFull(c.Request.Body, buff)
		pool.Put(buff) // nolint: staticcheck
		atomic.AddInt64(&readn, int64(n))
	}
	c.Respond()
}

func serverRpc() {
	router := rpc.New()
	router.Handle(http.MethodPut, "/:size", handleUpload)
	httpServer := &http.Server{
		Addr:    *addr,
		Handler: rpc.MiddlewareHandlerWith(router),
	}
	httpServer.ListenAndServe()
}

type buffConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *buffConn) Read(b []byte) (n int, err error) {
	return c.r.Read(b)
}

func handleStream(stream *transport.Stream) {
	buff := pool.Get().([]byte)
	for {
		frame, err := stream.ReadFrame(context.Background())
		if err != nil {
			fmt.Printf("(%s %d) error(%s)\n", stream.RemoteAddr(), stream.ID(), err)
			return
		}
		if *discard {
			n, _ := frame.WriteTo(io.Discard)
			atomic.AddInt64(&readn, int64(n))
		} else {
			n, _ := frame.Read(buff)
			atomic.AddInt64(&readn, int64(n))
		}
		frame.Close()
	}
}

func serverSmux() {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		fmt.Println("Accept conn", conn.RemoteAddr())

		if bsize := *connbuffer; bsize > 0 {
			conn = &buffConn{
				Conn: conn,
				r:    bufio.NewReaderSize(conn, bsize),
			}
		}

		go func() {
			sess, err := transport.Server(transport.NetConn(conn, nil, false),
				config.Transport.Transport())
			if err != nil {
				panic(err)
			}
			for {
				stream, err := sess.AcceptStream()
				if err != nil {
					return
				}
				fmt.Println("Accept stream", stream.ID(), stream.RemoteAddr())
				go handleStream(stream)
			}
		}()
	}
}

func handleConnection(conn net.Conn) {
	buff := pool.Get().([]byte)[:*requestsize]
	for {
		n, err := io.ReadFull(conn, buff)
		if err != nil {
			fmt.Println(conn.RemoteAddr(), err)
			return
		}
		atomic.AddInt64(&readn, int64(n))
	}
}

func serverTcp() {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		fmt.Println("Accept conn", conn.RemoteAddr())

		if bsize := *connbuffer; bsize > 0 {
			conn = &buffConn{
				Conn: conn,
				r:    bufio.NewReaderSize(conn, bsize),
			}
		}
		go handleConnection(conn)
	}
}
