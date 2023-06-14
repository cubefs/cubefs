package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:9090", nil))
	}()
}

// setupServer starts new server listening on a random localhost port and
// returns address of the server, function to stop the server, new client
// connection to this server or an error.
func setupServer(tb testing.TB) (addr string, stopfunc func(), err error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConnection(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }, nil
}

func handleConnection(conn net.Conn) {
	session, _ := smux.Server(conn, nil)
	for {
		if stream, err := session.AcceptStream(); err == nil {
			go func(s io.ReadWriteCloser) {
				buf := make([]byte, 65536)
				for {
					n, err := s.Read(buf)
					if err != nil {
						return
					}
					s.Write(buf[:n])
				}
			}(stream)
		} else {
			return
		}
	}
}

// setupServer starts new server listening on a random localhost port and
// returns address of the server, function to stop the server, new client
// connection to this server or an error.
func setupServerV2(tb testing.TB) (addr string, stopfunc func(), err error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConnectionV2(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }, nil
}

func handleConnectionV2(conn net.Conn) {
	config := smux.DefaultConfig()
	config.Version = 2
	session, _ := smux.Server(conn, config)
	for {
		if stream, err := session.AcceptStream(); err == nil {
			go func(s io.ReadWriteCloser) {
				buf := make([]byte, 65536)
				for {
					n, err := s.Read(buf)
					if err != nil {
						return
					}
					s.Write(buf[:n])
				}
			}(stream)
		} else {
			return
		}
	}
}

func newConfig() *SmuxConnPoolConfig {
	cfg := DefaultSmuxConnPoolConfig()
	cfg.TotalStreams = 1000000
	return cfg
}

func newPool() *SmuxConnectPool {
	return NewSmuxConnectPool(newConfig())
}

func newPoolV2() *SmuxConnectPool {
	cfg := newConfig()
	cfg.TotalStreams = 1000000
	cfg.Version = 2
	return NewSmuxConnectPool(cfg)
}

func TestShiftAddrPort(t *testing.T) {
	if ans := ShiftAddrPort(":17010", 100); ans != ":17110" {
		t.Fatal()
	}
	if ans := ShiftAddrPort("1.0.0.0:0", 100); ans != "1.0.0.0:100" {
		t.Fatal()
	}
}

func TestVerifySmuxPoolConfig(t *testing.T) {
	cfg := DefaultSmuxConnPoolConfig()
	if err := VerifySmuxPoolConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestEcho(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := pool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.PutConnect(stream, true)
	const N = 100
	buf := make([]byte, 10)
	var sent string
	var received string
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf("hello%v", i)
		stream.Write([]byte(msg))
		sent += msg
		if n, err := stream.Read(buf); err != nil {
			t.Fatal(err)
		} else {
			received += string(buf[:n])
		}
	}
	if sent != received {
		t.Fatal("data mimatch")
	}
}

func TestWriteTo(t *testing.T) {
	pool := newPool()
	const N = 1 << 20
	// server
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		session, _ := smux.Server(conn, nil)
		for {
			if stream, err := session.AcceptStream(); err == nil {
				go func(s io.ReadWriteCloser) {
					numBytes := 0
					buf := make([]byte, 65536)
					for {
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						s.Write(buf[:n])
						numBytes += n

						if numBytes == N {
							s.Close()
							return
						}
					}
				}(stream)
			} else {
				return
			}
		}
	}()

	addr := ln.Addr().String()
	stream, err := pool.GetConnect(addr)
	defer pool.PutConnect(stream, true)
	if err != nil {
		t.Fatal(err)
	}

	sndbuf := make([]byte, N)
	for i := range sndbuf {
		sndbuf[i] = byte(rand.Int())
	}

	go stream.Write(sndbuf)

	var rcvbuf bytes.Buffer
	nw, ew := stream.WriteTo(&rcvbuf)
	if ew != io.EOF {
		t.Fatal(ew)
	}

	if nw != N {
		t.Fatal("WriteTo nw mismatch", nw)
	}

	if !bytes.Equal(sndbuf, rcvbuf.Bytes()) {
		t.Fatal("mismatched echo bytes")
	}
}

func TestWriteToV2(t *testing.T) {
	poolV2 := newPoolV2()
	config := smux.DefaultConfig()
	config.Version = 2
	const N = 1 << 20
	// server
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		session, _ := smux.Server(conn, config)
		for {
			if stream, err := session.AcceptStream(); err == nil {
				go func(s io.ReadWriteCloser) {
					numBytes := 0
					buf := make([]byte, 65536)
					for {
						n, err := s.Read(buf)
						if err != nil {
							return
						}
						s.Write(buf[:n])
						numBytes += n

						if numBytes == N {
							s.Close()
							return
						}
					}
				}(stream)
			} else {
				return
			}
		}
	}()

	addr := ln.Addr().String()
	stream, err := poolV2.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer poolV2.PutConnect(stream, true)

	// client
	sndbuf := make([]byte, N)
	for i := range sndbuf {
		sndbuf[i] = byte(rand.Int())
	}

	go stream.Write(sndbuf)

	var rcvbuf bytes.Buffer
	nw, ew := stream.WriteTo(&rcvbuf)
	if ew != io.EOF {
		t.Fatal(ew)
	}

	if nw != N {
		t.Fatal("WriteTo nw mismatch", nw)
	}

	if !bytes.Equal(sndbuf, rcvbuf.Bytes()) {
		t.Fatal("mismatched echo bytes")
	}
}

func TestSpeed(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := pool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(stream.LocalAddr(), stream.RemoteAddr())

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		buf := make([]byte, 1024*1024)
		nrecv := 0
		for {
			n, err := stream.Read(buf)
			if err != nil {
				t.Error(err)
				break
			} else {
				nrecv += n
				if nrecv == 4096*4096 {
					break
				}
			}
		}
		pool.PutConnect(stream, true)
		t.Log("time for 16MB rtt", time.Since(start))
		wg.Done()
	}()
	msg := make([]byte, 8192)
	for i := 0; i < 2048; i++ {
		stream.Write(msg)
	}
	wg.Wait()
}

func TestTokenLimit(t *testing.T) {
	cfg := newConfig()
	cfg.TotalStreams = 1000
	pool := NewSmuxConnectPool(cfg)
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	streams := make([]*smux.Stream, 0, 1001)
	for i := 0; i < 1000; i++ {
		s, err := pool.GetConnect(addr)
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, s)
	}
	s, err := pool.GetConnect(addr)
	if err != ErrTooMuchSmuxStreams {
		t.Fatal(s, err)
	}
	for _, s := range streams {
		pool.PutConnect(s, true)
	}
}

func TestParallel(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	par := 1000
	messages := 10
	wg := sync.WaitGroup{}
	wg.Add(par)
	for i := 0; i < par; i++ {
		stream, _ := pool.GetConnect(addr)
		go func(s *smux.Stream) {
			buf := make([]byte, 20)
			for j := 0; j < messages; j++ {
				msg := fmt.Sprintf("hello%v", j)
				s.Write([]byte(msg))
				if _, err := s.Read(buf); err != nil {
					t.Log(err)
					break
				}
			}
			pool.PutConnect(s, false)
			wg.Done()
		}(stream)
	}
	wg.Wait()
}

func TestParallelV2(t *testing.T) {
	poolV2 := newPoolV2()
	addr, stop, err := setupServerV2(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	par := 1000
	messages := 10
	wg := sync.WaitGroup{}
	wg.Add(par)
	for i := 0; i < par; i++ {
		stream, _ := poolV2.GetConnect(addr)
		go func(s *smux.Stream) {
			buf := make([]byte, 20)
			for j := 0; j < messages; j++ {
				msg := fmt.Sprintf("hello%v", j)
				s.Write([]byte(msg))
				if _, err := s.Read(buf); err != nil {
					t.Log(err)
					break
				}
			}
			poolV2.PutConnect(s, false)
			wg.Done()
		}(stream)
	}
	wg.Wait()
}

func TestSmuxConnectPool_GetStat(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	pool := newPool()
	for _, streamCnt := range []int{1, 100, 10000} {
		streams := make([]*smux.Stream, streamCnt)
		for i := 0; i < streamCnt; i++ {
			streams[i], err = pool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}
		}
		stat := pool.GetStat()
		output, _ := json.MarshalIndent(stat, "", "\t")
		t.Logf("streamCnt:%d, stat:%s", streamCnt, string(output))
		if stat.TotalStreams != streamCnt {
			t.Fatal("stat.TotalStreams not correct!")
		}
		if stat.TotalSessions > pool.cfg.ConnsPerAddr {
			t.Fatal("too much connections!")
		}
		for _, s := range streams {
			pool.PutConnect(s, true)
		}
		stat = pool.GetStat()
		if stat.TotalStreams > 0 {
			t.Fatal("total streams not clean after closed")
		}
	}
}

func TestConcurrentPutConn(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	numStreams := 300
	streams := make([]*smux.Stream, 0, numStreams)
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		stream, _ := pool.GetConnect(addr)
		streams = append(streams, stream)
	}
	for _, stream := range streams {
		go func(stream *smux.Stream) {
			pool.PutConnect(stream, true)
			wg.Done()
		}(stream)
	}
	wg.Wait()
}

func TestConcurrentGetConn(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	numStreams := 10000
	streamsCh := make(chan *smux.Stream, numStreams)
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 0; i < numStreams; i++ {
		go func() {
			defer wg.Done()
			stream, err := pool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}
			streamsCh <- stream
		}()
	}

	wg.Wait()
	if len(streamsCh) != numStreams {
		t.Fatalf("stream ch not correct, want %d, got %d", numStreams, len(streamsCh))
	}

	stat := pool.GetStat()
	output, _ := json.MarshalIndent(stat, "", "\t")
	t.Logf("streamCnt:%d, stat:%s", numStreams, string(output))

	if stat.TotalStreams != numStreams {
		t.Fatalf("stat total streams not right, want %d, got %d", numStreams, stat.TotalStreams)
	}

	if stat.TotalSessions > pool.cfg.ConnsPerAddr {
		t.Fatalf("sessions is too big, expect %d, got %d", pool.cfg.ConnsPerAddr, stat.TotalSessions)
	}

	close(streamsCh)
	for stream := range streamsCh {
		pool.PutConnect(stream, true)
	}

}

func TestConcurrentGetPutConn(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	numStreams := 10000
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 0; i < numStreams; i++ {
		go func() {
			defer wg.Done()
			stream, err := pool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+1))
			pool.PutConnect(stream, true)
		}()
	}

	stat := pool.GetStat()
	output, _ := json.MarshalIndent(stat, "", "\t")
	t.Logf("streamCnt:%d, stat:%s", numStreams, string(output))

	if stat.TotalSessions > pool.cfg.ConnsPerAddr {
		t.Fatalf("sessions is too big, expect %d, got %d", pool.cfg.ConnsPerAddr, stat.TotalSessions)
	}

	wg.Wait()
}

func TestTinyReadBuffer(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := pool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.PutConnect(stream, true)
	const N = 100
	tinybuf := make([]byte, 6)
	var sent string
	var received string
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf("hello%v", i)
		sent += msg
		nsent, err := stream.Write([]byte(msg))
		if err != nil {
			t.Fatal("cannot write")
		}
		nrecv := 0
		for nrecv < nsent {
			if n, err := stream.Read(tinybuf); err == nil {
				nrecv += n
				received += string(tinybuf[:n])
			} else {
				t.Fatal("cannot read with tiny buffer")
			}
		}
	}
	if sent != received {
		t.Fatal("data mimatch")
	}
}

func TestKeepAliveTimeout(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	const sec = 5
	cfg := DefaultSmuxConnPoolConfig()
	cfg.StreamIdleTimeout = int64(sec * time.Second)
	par := 1000
	errAllowed := make(chan error, par/2)
	notifyError := func(err error) {
		t.Log(err)
		select {
		case errAllowed <- err:
		default:
			t.Fatal(err)
		}
	}
	pool := NewSmuxConnectPool(cfg)
	wg := sync.WaitGroup{}
	wg.Add(par)
	for i := 0; i < par; i++ {
		go func() {
			defer wg.Done()
			stream, err := pool.GetConnect(addr)
			if err != nil {
				notifyError(err)
				return
			}
			defer func() {
				pool.PutConnect(stream, err != nil)
			}()
			buf := make([]byte, 10)
			_, err = stream.Write(buf)
			if err != nil {
				notifyError(err)
				return
			}
			_, err = stream.Read(buf)
			if err != nil {
				notifyError(err)
				return
			}
		}()
	}
	wg.Wait()
	time.Sleep(sec * time.Second)
	failedSec := 0
	for pool.GetStat().TotalStreams > 0 || pool.GetStat().TotalSessions > 0 {
		out, _ := json.MarshalIndent(pool.GetStat(), "", "\t")
		if failedSec > sec {
			t.Fatalf("(%v), still not clean after sec(%v)", string(out), failedSec)
		}
		failedSec++
		time.Sleep(time.Second)
	}

	t.Log("test session timeout success")
}

func TestServerEcho(t *testing.T) {
	pool := newPool()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		err := func() error {
			conn, err := ln.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			session, err := smux.Server(conn, nil)
			if err != nil {
				return err
			}
			defer session.Close()
			buf := make([]byte, 10)
			stream, err := session.AcceptStream()
			if err != nil {
				return err
			}
			defer stream.Close()
			for i := 0; i < 100; i++ {
				msg := fmt.Sprintf("hello%v", i)
				stream.Write([]byte(msg))
				n, err := stream.Read(buf)
				if err != nil {
					return err
				}
				if got := string(buf[:n]); got != msg {
					return fmt.Errorf("got: %q, want: %q", got, msg)
				}
			}
			return nil
		}()
		if err != nil {
			t.Error(err)
		}
	}()
	if stream, err := pool.GetConnect(ln.Addr().String()); err == nil {
		buf := make([]byte, 65536)
		for {
			n, err := stream.Read(buf)
			if err != nil {
				break
			}
			stream.Write(buf[:n])
		}
		pool.PutConnect(stream, true)
	} else {
		t.Fatal(err)
	}
}

func TestSendWithoutRecv(t *testing.T) {
	pool := newPool()
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := pool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	const N = 100
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf("hello%v", i)
		stream.Write([]byte(msg))
	}
	buf := make([]byte, 1)
	if _, err := stream.Read(buf); err != nil {
		t.Fatal(err)
	}
	pool.PutConnect(stream, true)
}

func BenchmarkGetConn(b *testing.B) {
	pool := newPool()
	addr, stop, err := setupServer(b)
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	created := make([]*smux.Stream, 0, b.N)
	for i := 0; i < b.N; i++ {
		if stream, err := pool.GetConnect(addr); err == nil {
			if stream == nil {
				panic("!!!")
			}
			created = append(created, stream)
		} else {
			b.Fatal(err)
		}
	}
	for _, s := range created {
		pool.PutConnect(s, true)
	}
}

func BenchmarkParallelGetConn(b *testing.B) {
	pool := newPool()
	addr, stop, err := setupServer(b)
	if err != nil {
		b.Fatal(err)
	}
	par := 32
	closeCh := make(chan struct{})
	closeOnce := sync.Once{}
	created := make(chan *smux.Stream, par)
	wg := sync.WaitGroup{}
	wg.Add(par)
	go func() {
		for range closeCh {
			wg.Wait()
			close(created)
			stop()
			return
		}
	}()
	for i := 0; i < par; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-closeCh:
					return
				default:
					if stream, err := pool.GetConnect(addr); err == nil {
						created <- stream
					} else {
						panic(err)
					}
				}
			}
		}()
	}
	n := 0
	for stream := range created {
		n++
		if n >= b.N {
			closeOnce.Do(func() {
				close(closeCh)
			})
		}
		pool.PutConnect(stream, true)
	}
}

func BenchmarkConnSmux(b *testing.B) {
	cs, ss, recycle, err := getSmuxStreamPair()
	if err != nil {
		b.Fatal(err)
	}
	defer recycle()
	bench(b, cs, ss)
}

func BenchmarkConnTCP(b *testing.B) {
	cs, ss, err := getTCPConnectionPair()
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
	bench(b, cs, ss)
}

func getSmuxStreamPair() (cs *smux.Stream, ss *smux.Stream, recycle func(), err error) {
	pool := newPool()
	var lst net.Listener
	lst, err = net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, nil, err
	}
	defer lst.Close()
	done := make(chan error)
	go func() {
		var (
			conn net.Conn
			s    *smux.Session
			rerr error
		)
		conn, rerr = lst.Accept()
		if rerr != nil {
			done <- rerr
		}
		s, rerr = smux.Server(conn, DefaultSmuxConfig())
		if rerr != nil {
			done <- rerr
		}
		ss, rerr = s.AcceptStream()
		done <- rerr
		close(done)
	}()
	cs, err = pool.GetConnect(lst.Addr().String())
	if err != nil {
		return nil, nil, nil, err
	}
	err = <-done
	if err != nil {
		return nil, nil, nil, err
	}
	return cs, ss, func() {
		pool.PutConnect(cs, false)
		ss.Close()
	}, err
}

func getTCPConnectionPair() (net.Conn, net.Conn, error) {
	lst, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, err
	}
	defer lst.Close()

	var conn0 net.Conn
	var err0 error
	done := make(chan struct{})
	go func() {
		conn0, err0 = lst.Accept()
		close(done)
	}()

	conn1, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	<-done
	if err0 != nil {
		return nil, nil, err0
	}
	return conn0, conn1, nil
}

func bench(b *testing.B, rd io.Reader, wr io.Writer) {
	buf := make([]byte, 128*1024)
	buf2 := make([]byte, 128*1024)
	b.SetBytes(128 * 1024)
	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			n, _ := rd.Read(buf2)
			count += n
			if count == 128*1024*b.N {
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		wr.Write(buf)
	}
	wg.Wait()
}
