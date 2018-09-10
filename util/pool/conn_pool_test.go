// Copyright 2018 The ChuBao Authors.
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

package pool_test

import (
	"fmt"
	"github.com/tiglabs/containerfs/util/pool"
	"net"
	"sync/atomic"
	"testing"
)

func TestConnPool(t *testing.T) {
	// Start a tcp server for moc
	ms := &mockTcpServer{}
	if err := ms.start(9090); err != nil {
		t.Fatal(fmt.Sprintf("fail on start mock server cause %s.", err))
	}

	cp := pool.NewConnPool()
	for i := 0; i < 1000; i++ {
		conn, err := cp.Get("127.0.0.1:9090")
		if err != nil {
			t.Fatal(err)
		}
		cp.Put(conn, false)
	}
	ms.stop()
	if ms.getAcceptCount() > 5 {
		t.Fatal(fmt.Sprintf("conn pool makes %d connections to remote.", ms.getAcceptCount()))
	}
}

type mockTcpServer struct {
	state       uint32
	ln          net.Listener
	acceptCount uint64
}

func (s *mockTcpServer) start(port int) (err error) {
	if atomic.CompareAndSwapUint32(&s.state, 0, 1) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&s.state, 0)
			}
		}()
		s.ln, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
		go func(ln net.Listener, acceptCount *uint64) {
			for {
				conn, err := ln.Accept()
				if err != nil {
					break
				}
				atomic.AddUint64(acceptCount, 1)
				go func(conn net.Conn) {
					fmt.Printf("remote %s connected.\n", conn.RemoteAddr().String())
					buff := make([]byte, 256)
					for {
						if _, err := conn.Read(buff); err != nil {
							break
						}
					}
					conn.Close()
					fmt.Printf("remote %s disconnected.\n", conn.RemoteAddr().String())
				}(conn)
			}
		}(s.ln, &s.acceptCount)
	}
	return
}

func (s *mockTcpServer) stop() {
	if atomic.CompareAndSwapUint32(&s.state, 1, 0) {
		if s.ln != nil {
			s.ln.Close()
		}
	}
}

func (s *mockTcpServer) getAcceptCount() uint64 {
	return atomic.LoadUint64(&s.acceptCount)
}
