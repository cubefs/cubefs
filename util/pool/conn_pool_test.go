package pool_test

import (
	"fmt"
	"github.com/chubaoio/cbfs/util/pool"
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
