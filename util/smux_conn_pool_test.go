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
	"smux"
	"sync"
	"testing"
	"time"
)

var gTestPool = newPool()
var gTestPoolV2 = newPoolV2()

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
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleConnection(conn)
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
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleConnectionV2(conn)
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
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := gTestPool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer gTestPool.PutConnect(stream, true)
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
	stream, err := gTestPool.GetConnect(addr)
	defer gTestPool.PutConnect(stream, true)
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

	if bytes.Compare(sndbuf, rcvbuf.Bytes()) != 0 {
		t.Fatal("mismatched echo bytes")
	}
}

func TestWriteToV2(t *testing.T) {
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
	stream, err := gTestPoolV2.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer gTestPoolV2.PutConnect(stream, true)

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

	if bytes.Compare(sndbuf, rcvbuf.Bytes()) != 0 {
		t.Fatal("mismatched echo bytes")
	}
}

func TestSpeed(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := gTestPool.GetConnect(addr)
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
		gTestPool.PutConnect(stream, true)
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
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	par := 10000
	messages := 10
	streams := make([]*smux.Stream, par)
	for i := 0; i < par; i++ {
		streams[i], err = gTestPool.GetConnect(addr)
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(par)
	for i := 0; i < par; i++ {
		go func(stream *smux.Stream) {
			defer gTestPool.PutConnect(stream, true)
			buf := make([]byte, 4<<20)
			for j := 0; j < messages; j++ {
				msg := fmt.Sprintf("hello%v", j)
				stream.Write([]byte(msg))
				if _, err := stream.Read(buf); err != nil {
					break
				}
			}
			wg.Done()
		}(streams[i])
	}
	wg.Wait()
}

func TestParallelV2(t *testing.T) {
	addr, stop, err := setupServerV2(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	par := 10000
	messages := 10
	streams := make([]*smux.Stream, par)
	for i := 0; i < par; i++ {
		streams[i], err = gTestPoolV2.GetConnect(addr)
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(par)
	for i := 0; i < par; i++ {
		go func(stream *smux.Stream) {
			defer gTestPoolV2.PutConnect(stream, true)
			buf := make([]byte, 20)
			for j := 0; j < messages; j++ {
				msg := fmt.Sprintf("hello%v", j)
				stream.Write([]byte(msg))
				if _, err := stream.Read(buf); err != nil {
					break
				}
			}
			wg.Done()
		}(streams[i])
	}
	wg.Wait()
}

func TestSmuxConnectPool_GetStat(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	for _, streamCnt := range []int{1, 10, 100, 1000, 10000, 100000} {
		streams := make([]*smux.Stream, streamCnt)
		for i := 0; i < streamCnt; i++ {
			streams[i], err = gTestPool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}
		}
		stat := gTestPool.GetStat()
		output, _ := json.MarshalIndent(stat, "", "\t")
		t.Logf("%s", string(output))
		if stat.TotalStreams != streamCnt {
			t.Fatal("stat.TotalStreams not correct!")
		}
		if stat.TotalSessions > gTestPool.cfg.ConnsPerAddr+5 {
			t.Fatal("too much connections!")
		}
		streamLimitPerConn := (gTestPool.cfg.StreamsPerConn) + (streamCnt/gTestPool.cfg.ConnsPerAddr + 1)
		for remoteAddr, poolStat := range stat.Pools {
			for localAddr, streamPerSess := range poolStat.StreamsPerSession {
				if streamPerSess > streamLimitPerConn {
					t.Fatalf("remoteAddr(%v), localAddr(%v), streamPerSess(%v) > streamLimitPerConn(%v)\n",
						remoteAddr, localAddr, streamPerSess, streamLimitPerConn)
				}
			}
		}
		for _, s := range streams {
			gTestPool.PutConnect(s, true)
		}
		stat = gTestPool.GetStat()
		if stat.TotalStreams > 0 {
			t.Fatal("total streams not clean after closed")
		}
	}
}

func TestConcurrentPutConn(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	numStreams := 3000
	streams := make([]*smux.Stream, 0, numStreams)
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		stream, _ := gTestPool.GetConnect(addr)
		streams = append(streams, stream)
	}
	for _, stream := range streams {
		go func(stream *smux.Stream) {
			gTestPool.PutConnect(stream, true)
			wg.Done()
		}(stream)
	}
	wg.Wait()
}

func TestTinyReadBuffer(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := gTestPool.GetConnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer gTestPool.PutConnect(stream, true)
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
	cfg := DefaultSmuxConnPoolConfig()
	par := 3000
	streams := make([]*smux.Stream, 0, par)
	streamsLk := sync.Mutex{}
	pool := NewSmuxConnectPool(cfg)
	wg := sync.WaitGroup{}
	wg.Add(par)
	for i := 0; i < par; i++ {
		go func() {
			defer wg.Done()
			stream, err := pool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}
			streamsLk.Lock()
			streams = append(streams, stream)
			streamsLk.Unlock()
			buf := make([]byte, 10)
			_, err = stream.Write(buf)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			pool.PutConnect(stream, false)
		}()
	}
	wg.Wait()
	time.Sleep(60 * time.Second)
	for pool.GetStat().TotalStreams > 0 || pool.GetStat().TotalSessions > 0 {
		out, _ := json.MarshalIndent(pool.GetStat(), "", "\t")
		t.Error(string(out))
	}

	t.Log("test session timeout success")
}

func TestKeepAliveIntervalMoreThanTimeout(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	cfg := DefaultSmuxConnPoolConfig()
	par := 3000
	streams := make([]*smux.Stream, 0, par)
	streamsLk := sync.Mutex{}
	pool := NewSmuxConnectPool(cfg)
	wg := sync.WaitGroup{}
	wg.Add(par)
	for i := 0; i < par; i++ {
		go func() {
			defer wg.Done()
			stream, err := pool.GetConnect(addr)
			if err != nil {
				t.Fatal(err)
			}
			streamsLk.Lock()
			streams = append(streams, stream)
			streamsLk.Unlock()
			buf := make([]byte, 10)
			_, err = stream.Write(buf)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			pool.PutConnect(stream, false)
		}()
	}
	wg.Wait()
	time.Sleep(60 * time.Second)
	for pool.GetStat().TotalStreams > 0 || pool.GetStat().TotalSessions > 0 {
		out, _ := json.MarshalIndent(pool.GetStat(), "", "\t")
		t.Error(string(out))
	}

	t.Log("test session timeout success")
}

func TestServerEcho(t *testing.T) {
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
	if stream, err := gTestPool.GetConnect(ln.Addr().String()); err == nil {
		buf := make([]byte, 65536)
		for {
			n, err := stream.Read(buf)
			if err != nil {
				break
			}
			stream.Write(buf[:n])
		}
		gTestPool.PutConnect(stream, true)
	} else {
		t.Fatal(err)
	}
}

func TestSendWithoutRecv(t *testing.T) {
	addr, stop, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	stream, err := gTestPool.GetConnect(addr)
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
	gTestPool.PutConnect(stream, true)
}

func BenchmarkGetConn(b *testing.B) {
	addr, stop, err := setupServer(b)
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	created := make([]*smux.Stream, 0, b.N)
	for i := 0; i < b.N; i++ {
		if stream, err := gTestPool.GetConnect(addr); err == nil {
			if stream == nil {
				panic("!!!")
			}
			created = append(created, stream)
		} else {
			b.Fatal(err)
		}
	}
	for _, s := range created {
		gTestPool.PutConnect(s, true)
	}
	return
}

func BenchmarkParallelGetConn(b *testing.B) {
	addr, stop, err := setupServer(b)
	if err != nil {
		b.Fatal(err)
	}
	par := 1000
	closeCh := make(chan struct{})
	closeOnce := sync.Once{}
	created := make(chan *smux.Stream, par)
	wg := sync.WaitGroup{}
	wg.Add(par)
	go func() {
		for {
			select {
			case <-closeCh:
				wg.Wait()
				close(created)
				stop()
				return
			}
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
					if stream, err := gTestPool.GetConnect(addr); err == nil {
						created <- stream
					} else {
						b.Fatal(err)
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
		gTestPool.PutConnect(stream, true)
	}
	return
}

func BenchmarkConnSmux(b *testing.B) {
	cs, ss, err := getSmuxStreamPair()
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
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

func getSmuxStreamPair() (cs *smux.Stream, ss *smux.Stream, err error) {
	var lst net.Listener
	lst, err = net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, err
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
	cs, err = gTestPool.GetConnect(lst.Addr().String())
	if err != nil {
		return nil, nil, err
	}
	err = <-done
	if err != nil {
		return nil, nil, err
	}
	return cs, ss, nil
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
