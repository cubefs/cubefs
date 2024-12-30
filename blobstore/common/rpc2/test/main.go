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

package main

import (
	"bufio"
	"flag"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	//"github.com/xtaci/smux"
	smux "github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

var (
	addr        = "127.0.0.1:9000"
	size        = flag.Int("size", 32<<10, "input packet size")
	connections = flag.Int("connections", 1, "input max connection of client")
	streams     = flag.Int("streams", 1, "input max streams peer connection")
	mode        = flag.String("mode", "server", "input running mode: server/client")
	maxProcess  = flag.Int("max-process", 1, "input max process")
)

type bufferReadCloser struct {
	//*bufio.ReadWriter
	*bufio.Reader
	conn net.Conn
}

func (b *bufferReadCloser) Write(p []byte) (int, error) {
	return b.conn.Write(p)
}

func (b *bufferReadCloser) Close() error {
	return b.conn.Close()
}

type bufferWriteCloser struct {
	//*bufio.ReadWriter
	//*bufio.Writer
	conn net.Conn
}

func (b *bufferWriteCloser) WriteBuffers(v [][]byte) (n int, err error) {
	buffers := net.Buffers(v)
	_n, err := buffers.WriteTo(b.conn)
	return int(_n), err
}

func (b *bufferWriteCloser) Write(p []byte) (int, error) {
	return b.conn.Write(p)
}

func (b *bufferWriteCloser) Read(p []byte) (int, error) {
	return b.conn.Read(p)
}

func (b *bufferWriteCloser) Close() error {
	return b.conn.Close()
}

func main() {
	flag.Parse()

	if *mode == "client" {
		runtime.GOMAXPROCS(*maxProcess)
		client()
		return
	}

	runtime.GOMAXPROCS(*maxProcess)
	server()
}

func client() {
	ph := profile.NewProfileHandler(":6061")
	httpServer := &http.Server{
		Addr:    ":6061",
		Handler: rpc.MiddlewareHandlerWith(rpc.DefaultRouter, ph),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("http server exits:", err)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(*connections)

	for i := 0; i < *connections; i++ {
		go func() {
			defer wg.Done()

			// Get a TCP connection
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				panic(err)
			}

			//bwc := conn

			/*bwc := &bufferReadWriteCloser{
				ReadWriter: bufio.NewReadWriter(bufio.NewReaderSize(conn, 128<<10), bufio.NewWriterSize(conn, 128<<10)),
				conn:       conn,
			}*/

			bwc := &bufferWriteCloser{
				//Writer: bufio.NewWriterSize(conn, 512<<10),
				conn: conn,
			}

			// Setup client side of smux
			config := smux.DefaultConfig()
			config.KeepAliveTimeout = 3600 * time.Second
			config.MaxStreamBuffer = 4 << 20
			config.MaxReceiveBuffer = 8 << 20
			config.FlushIntervalTimes = 32
			config.Version = 1
			session, err := smux.Client(bwc, config)
			if err != nil {
				panic(err)
			}

			swg := sync.WaitGroup{}
			swg.Add(*streams)
			for j := 0; j < *streams; j++ {
				go func() {
					defer swg.Done()

					// Open a new stream
					stream, err := session.OpenStream()
					if err != nil {
						panic(err)
					}

					// Stream implements io.ReadWriteCloser
					//b1 := make([]byte, 8)
					b2 := make([]byte, *size)

					frame, _ := stream.AllocFrame(*size)
					frame.Write(b2)
					for {
						if _, err := stream.WriteFrame(frame); err != nil {
							log.Error(err)
							break
						}
						/*if _, err := stream.WriteFrame(b2); err != nil {
							log.Error(err)
							break
						}*/
					}
					stream.Close()
				}()
			}
			swg.Wait()

			session.Close()
		}()
	}

	wg.Wait()
}

func server() {
	ph := profile.NewProfileHandler(":6060")
	httpServer := &http.Server{
		Addr:    ":6060",
		Handler: rpc.MiddlewareHandlerWith(rpc.DefaultRouter, ph),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("http server exits:", err)
		}
	}()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	// Accept a TCP connection
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		//bwc := conn
		/*bwc := &bufferReadWriteCloser{
			ReadWriter: bufio.NewReadWriter(bufio.NewReaderSize(conn, 128<<10), bufio.NewWriterSize(conn, 128<<10)),
			//ReadWriter: bufio.NewReadWriter(bufio.NewReaderSize(conn, 128<<10), conn),
			conn: conn,
		}*/

		bwc := &bufferReadCloser{
			Reader: bufio.NewReaderSize(conn, 128<<10),
			//ReadWriter: bufio.NewReadWriter(bufio.NewReaderSize(conn, 128<<10), conn),
			conn: conn,
		}

		// Setup server side of smux
		config := smux.DefaultConfig()
		config.KeepAliveTimeout = 3600 * time.Second
		config.MaxStreamBuffer = 4 << 20
		config.MaxReceiveBuffer = 64 << 20
		config.Version = 1
		session, err := smux.Server(bwc, config)
		if err != nil {
			panic(err)
		}

		go handleSession(session)
	}
}

func handleSession(s *smux.Session) {
	for {
		// Accept a stream
		stream, err := s.AcceptStream()
		if err != nil {
			panic(err)
		}
		go handleStream(stream)
	}
}

type nilWriter struct{}

func (n *nilWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func handleStream(s *smux.Stream) {
	// header
	//b1 := make([]byte, 8)
	// b2 := make([]byte, *size)

	n := &nilWriter{}

	for {
		// read header with copy
		/*if _, err := s.ReadFrame(b1); err != nil {
			log.Error(err)
			break
		}*/
		// read data without copy
		frame, err := s.ReadFrame()
		if err != nil {
			log.Error("read frame failed: ", err)
			break
		}
		if _, err := frame.WriteTo(n); err != nil {
			log.Error("write to failed: ", err)
			break
		}
		if err := frame.Close(); err != nil {
			log.Error("close failed: ", err)
			break
		}
	}

	s.Close()
}
