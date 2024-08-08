package transport

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(1)
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
}

func setupServer(tb testing.TB) (addr string, stopfunc func(), client net.Conn, err error) {
	return _setupServer(tb, false)
}

func setupServerV2(tb testing.TB) (addr string, stopfunc func(), client net.Conn, err error) {
	return _setupServer(tb, true)
}

// setupServer starts new server listening on a random localhost port and
// returns address of the server, function to stop the server, new client
// connection to this server or an error.
func _setupServer(tb testing.TB, v2 bool) (addr string, stopfunc func(), client net.Conn, err error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, nil, err
	}
	go func() {
		conn, errx := ln.Accept()
		if errx != nil {
			return
		}
		go handleConnection(tb, conn, v2)
	}()
	addr = ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		ln.Close()
		return "", nil, nil, err
	}
	return ln.Addr().String(), func() { ln.Close() }, conn, nil
}

func handleConnection(tb testing.TB, conn net.Conn, v2 bool) {
	config := DefaultConfig()
	if v2 {
		config.Version = 2
	}
	session, _ := Server(conn, config)
	for {
		if stream, err := session.AcceptStream(); err == nil {
			go func(s *Stream) {
				for {
					fr, err := s.ReadFrame()
					if err != nil {
						if err != io.EOF {
							tb.Error(err)
						}
						return
					}
					fw, err := s.AllocFrame(fr.Len())
					if err != nil {
						tb.Error(err)
						return
					}
					_, err = io.CopyN(fw, fr, int64(fr.Len()))
					if err != nil {
						tb.Error(err)
						return
					}
					s.WriteFrame(fw)
					fr.Close()
					fw.Close()
				}
			}(stream)
		} else {
			return
		}
	}
}

func writeThenRead(s *Stream, msg string) {
	size := len(msg)
	buf := make([]byte, size)
	_, err := s.SizedWrite(strings.NewReader(msg), size)
	if err != nil {
		panic(err)
	}

	fr := s.SizedReader(size)
	defer fr.Close()
	if _, err := fr.Read(buf); err != nil {
		panic(err)
	}
}

func TestEcho(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	conf := DefaultConfig()
	conf.hdrRegistered = true
	session, _ := Client(cli, conf)
	stream, _ := session.OpenStream()
	if _, err := stream.AllocFrame(0); err != ErrFrameOversize {
		t.Fatal("frame over size")
	}
	if _, err := stream.AllocFrame(stream.MaxPayloadSize() + 1); err != ErrFrameOversize {
		t.Fatal("frame over size")
	}
	const N = 100
	buf := make([]byte, 10)
	var sent string
	var received string
	for i := range [N]struct{}{} {
		msg := fmt.Sprintf("hello%v", i)
		fw, err := stream.AllocFrame(len(msg))
		if err != nil {
			t.Fatal(err)
		}
		fw.Write([]byte(msg))
		stream.WriteFrame(fw)
		fw.Close()
		sent += msg
		fr, err := stream.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if n, err := fr.Read(buf); err != nil {
			t.Fatal(err)
		} else {
			received += string(buf[:n])
		}
	}
	if sent != received {
		t.Fatal("data mimatch")
	}
	session.Close()
}

func TestGoAway(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	session.nextStreamID = (1 << 32) - 1
	defer session.Close()
	for range [3]struct{}{} {
		_, err = session.OpenStream()
		if err != ErrGoAway {
			t.Fatal(ErrGoAway)
		}
	}
}

func TestGetDieCh(t *testing.T) {
	cs, ss, err := getSmuxStreamPair(false)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	dieCh := ss.CloseChan()
	go func() {
		select {
		case <-dieCh:
		case <-time.Tick(time.Second):
			panic("wait die chan timeout")
		}
	}()
	cs.Close()
}

type sizedWriter struct {
	N int
}

func (w *sizedWriter) Write(p []byte) (int, error) {
	w.N += len(p)
	return len(p), nil
}

func TestSpeed(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	t.Log(stream.ID(), stream.LocalAddr(), stream.RemoteAddr())
	stream.SetDeadline(time.Now().Add(time.Minute))

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w := &sizedWriter{}
		rc := stream.SizedReader(4096 * 4096)
		defer rc.Close()
		_, err := rc.WriteTo(w)
		if err != nil {
			t.Error(err)
		} else {
			if w.N != 4096*4096 {
				t.Error("read failed")
			}
		}
		stream.Close()
		t.Log("time for 16MB rtt", time.Since(start))
		wg.Done()
	}()
	msg := make([]byte, 8192)
	for range [2048]struct{}{} {
		stream.SizedWrite(bytes.NewReader(msg), len(msg))
	}
	wg.Wait()
	session.Close()
}

func TestSizedReadWrite(t *testing.T) {
	const k64 = 64 * (1 << 10)
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	stream.frameSize = k64 // payload k64 - headerSize

	{
		size := k64 / 2
		_, err := stream.SizedWrite(bytes.NewReader(make([]byte, size)), size)
		if err != nil {
			t.Fatal(err)
		}
		rc := stream.SizedReader(size)
		_, err = rc.WriteTo(rwErr{})
		if err == nil {
			t.Fatal(err)
		}
		rc.Close()
	}
	{
		size := 2 * k64
		_, err := stream.SizedWrite(bytes.NewReader(make([]byte, size)), size)
		if err != nil {
			t.Fatal(err)
		}
		rc := stream.SizedReader(size)
		n, err := rc.Read(make([]byte, k64))
		if err != nil {
			t.Fatal(err)
		}
		if n != k64-headerSize {
			t.Fatal("error frame size", n)
		}
		if rc.Finished() {
			t.Fatal("has frames to read")
		}

		w := &sizedWriter{}
		_, err = rc.WriteTo(w)
		if err != nil {
			t.Fatal(err)
		}
		_, err = rc.WriteTo(w)
		if err != io.EOF {
			t.Fatal(err)
		}
		if w.N != size-n {
			t.Fatal("error size", size, n, w.N)
		}
		_, err = rc.Read(make([]byte, 8))
		if err != io.EOF {
			t.Fatal(err)
		}
		if !rc.Finished() {
			t.Fatal("no frames to read")
		}

		rc.Close()
	}
	{
		size := k64 / 2
		_, err := stream.SizedWrite(bytes.NewReader(make([]byte, size)), size)
		if err != nil {
			t.Fatal(err)
		}
		rc := stream.SizedReader(size - 1)
		w := &sizedWriter{}
		_, err = rc.WriteTo(w)
		if err != ErrFrameOdd {
			t.Fatal(err)
		}
		rc.Close()
	}
	{
		size := k64 / 2
		_, err := stream.SizedWrite(bytes.NewReader(make([]byte, size)), size)
		if err != nil {
			t.Fatal(err)
		}
		rc := stream.SizedReader(size + 1)
		stream.SetDeadline(time.Now().Add(time.Second))
		w := &sizedWriter{}
		_, err = rc.WriteTo(w)
		if err != ErrTimeout {
			t.Fatal(err)
		}
		rc.Close()
	}

	stream.Close()
	session.Close()
}

func TestParallel(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)

	par := 1000
	messages := 100
	var wg sync.WaitGroup
	wg.Add(par)
	for i := 0; i < par; i++ {
		stream, _ := session.OpenStream()
		go func(s *Stream) {
			for j := 0; j < messages; j++ {
				writeThenRead(s, fmt.Sprintf("hello%v", j))
			}
			s.Close()
			wg.Done()
		}(stream)
	}
	t.Log("created", session.NumStreams(), "streams")
	wg.Wait()
	session.Close()
}

func TestParallelV2(t *testing.T) {
	_, stop, cli, err := setupServerV2(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	config := DefaultConfig()
	config.Version = 2
	session, _ := Client(cli, config)

	par := 1000
	messages := 100
	var wg sync.WaitGroup
	wg.Add(par)
	for i := 0; i < par; i++ {
		stream, _ := session.OpenStream()
		go func(s *Stream) {
			for j := 0; j < messages; j++ {
				writeThenRead(s, fmt.Sprintf("hello%v", j))
			}
			s.Close()
			wg.Done()
		}(stream)
	}
	t.Log("created", session.NumStreams(), "streams")
	wg.Wait()
	session.Close()
}

func TestCloseThenOpen(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	session.Close()
	if _, err := session.OpenStream(); err == nil {
		t.Fatal("opened after close")
	}

	session, _ = Server(cli, nil)
	session.Close()
	<-session.CloseChan()
	if _, err := session.AcceptStream(); err != io.ErrClosedPipe {
		t.Fatal("accepted after close")
	}
}

func TestAcceptTimeout(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Server(cli, nil)
	session.SetDeadline(time.Now().Add(-time.Second))
	if _, err := session.AcceptStream(); err != ErrTimeout {
		t.Fatal("accepted without timeout")
	}
}

func TestSessionDoubleClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	session.Close()
	if err := session.Close(); err == nil {
		t.Fatal("session double close doesn't return error")
	}
}

func TestStreamDoubleClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	stream.Close()
	if err := stream.Close(); err == nil {
		t.Fatal("stream double close doesn't return error")
	}
	session.Close()
}

func TestConcurrentClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	numStreams := 100
	streams := make([]*Stream, 0, numStreams)
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for i := 0; i < 100; i++ {
		stream, _ := session.OpenStream()
		streams = append(streams, stream)
	}
	for _, s := range streams {
		stream := s
		go func() {
			stream.Close()
			wg.Done()
		}()
	}
	session.Close()
	wg.Wait()
}

func TestTinyReadBuffer(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	const N = 100
	tinybuf := make([]byte, 6)
	var sent string
	var received string
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf("hello%v", i)
		sent += msg
		fw, err := stream.AllocFrame(len(msg))
		if err != nil {
			t.Fatal(err)
		}
		fw.Write([]byte(msg))
		nsent, err := stream.WriteFrame(fw)
		fw.Close()
		if err != nil {
			t.Fatal("cannot write")
		}
		nrecv := 0
		fr, err := stream.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		for nrecv < nsent {
			if n, err := fr.Read(tinybuf); err == nil {
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
	session.Close()
}

func TestIsClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	if session.IsClosed() {
		t.Fatal("session closed before close")
	}
	if stream.IsClosed() {
		t.Fatal("stream closed before close")
	}
	session.Close()
	if !session.IsClosed() {
		t.Fatal("session still open after close")
	}
	if !stream.IsClosed() {
		t.Fatal("stream still open after close")
	}
}

func TestKeepAliveTimeoutServer(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		ln.Accept()
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	config := DefaultConfig()
	config.KeepAliveInterval = time.Second
	config.KeepAliveTimeout = 2 * time.Second
	session, _ := Server(cli, config)
	time.Sleep(3 * time.Second)
	if !session.IsClosed() {
		t.Fatal("keepalive-timeout failed")
	}
}

type blockWriteConn struct {
	net.Conn

	deadline time.Time
}

func (c *blockWriteConn) Write(b []byte) (n int, err error) {
	if !c.deadline.IsZero() {
		timer := time.NewTimer(time.Until(c.deadline))
		defer timer.Stop()
		<-timer.C
		return 0, ErrTimeout
	}
	forever := time.Hour * 24
	time.Sleep(forever)
	return c.Conn.Write(b)
}

func (c *blockWriteConn) SetWriteDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func TestKeepAliveTimeoutClient(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		ln.Accept()
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	// when writeFrame block, keepalive in old version never timeout
	blockWriteCli := &blockWriteConn{Conn: cli}

	config := DefaultConfig()
	config.KeepAliveInterval = time.Second
	config.KeepAliveTimeout = 2 * time.Second
	session, _ := Client(blockWriteCli, config)
	time.Sleep(3 * time.Second)
	if !session.IsClosed() {
		t.Fatal("keepalive-timeout failed")
	}
}

func TestServerEcho(t *testing.T) {
	ln, errx := net.Listen("tcp", "localhost:0")
	if errx != nil {
		t.Fatal(errx)
	}
	defer ln.Close()
	go func() {
		if err := func() error {
			conn, err := ln.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			session, err := Server(conn, nil)
			if err != nil {
				return err
			}
			defer session.Close()
			buf := make([]byte, 10)
			stream, err := session.OpenStream()
			if err != nil {
				return err
			}
			defer stream.Close()
			for i := 0; i < 100; i++ {
				msg := fmt.Sprintf("hello%v", i)
				fw, _ := stream.AllocFrame(len(msg))
				fw.Write([]byte(msg))
				stream.WriteFrame(fw)
				fw.Close()
				fr, err := stream.ReadFrame()
				if err != nil {
					return err
				}
				n, err := fr.Read(buf)
				fr.Close()
				if err != nil {
					return err
				}
				if got := string(buf[:n]); got != msg {
					return fmt.Errorf("got: %q, want: %q", got, msg)
				}
			}
			return nil
		}(); err != nil {
			t.Error(err)
		}
	}()

	cli, errx := net.Dial("tcp", ln.Addr().String())
	if errx != nil {
		t.Fatal(errx)
	}
	defer cli.Close()
	if session, errx := Client(cli, nil); errx == nil {
		if s, erry := session.AcceptStream(); erry == nil {
			for {
				fr, err := s.ReadFrame()
				if err != nil {
					break
				}
				fw, err := s.AllocFrame(fr.Len())
				if err != nil {
					break
				}
				_, err = io.CopyN(fw, fr, int64(fr.Len()))
				if err != nil {
					break
				}
				s.WriteFrame(fw)
			}
		} else {
			t.Fatal(erry)
		}
	} else {
		t.Fatal(errx)
	}
}

func TestSendWithoutRecv(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	const N = 100
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf("hello%v", i)
		fw, _ := stream.AllocFrame(len(msg))
		fw.Write([]byte(msg))
		stream.WriteFrame(fw)
		fw.Close()
	}
	stream.Close()
}

func TestWriteAfterClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	fw, _ := stream.AllocFrame(8)
	fw.Write(make([]byte, 8))
	defer fw.Close()
	stream.Close()
	if _, err := stream.WriteFrame(fw); err == nil {
		t.Fatal("write after close failed")
	}
}

func TestReadStreamAfterSessionClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	session.Close()
	if _, err := stream.ReadFrame(); err != nil {
		t.Log(err)
	} else {
		t.Fatal("read stream after session close succeeded")
	}
}

func TestWriteStreamAfterConnectionClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	fw, _ := stream.AllocFrame(8)
	fw.Write(make([]byte, 8))
	defer fw.Close()
	session.conn.Close()
	if _, err := stream.WriteFrame(fw); err == nil {
		t.Fatal("write after connection close failed")
	}
}

func TestNumStreamAfterClose(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	if _, err := session.OpenStream(); err == nil {
		if session.NumStreams() != 1 {
			t.Fatal("wrong number of streams after opened")
		}
		session.Close()
		if session.NumStreams() != 0 {
			t.Fatal("wrong number of streams after session closed")
		}
	} else {
		t.Fatal(err)
	}
	cli.Close()
}

func TestRandomFrame(t *testing.T) {
	addr, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// pure random
	session, _ := Client(cli, nil)
	for i := 0; i < 100; i++ {
		rnd := make([]byte, rand.Uint32()%1024)
		io.ReadFull(crand.Reader, rnd)
		session.conn.Write(rnd)
	}
	cli.Close()

	// double syn
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(cmdSYN, 1000, 0)
		session.writeFrame(f)
	}
	cli.Close()

	// random cmds
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	allcmds := []byte{cmdSYN, cmdFIN, cmdPSH, cmdPIN, cmdPON}
	session, _ = Client(cli, nil)
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(allcmds[rand.Int()%len(allcmds)], rand.Uint32(), 0)
		session.writeFrame(f)
	}
	cli.Close()

	// random cmds & sids
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), 0)
		session.writeFrame(f)
	}
	cli.Close()

	// random version
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), 0)
		f.ver = byte(rand.Uint32())
		session.writeFrame(f)
	}
	cli.Close()

	// incorrect size
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)

	rnd := make([]byte, rand.Uint32()%1024)
	io.ReadFull(crand.Reader, rnd)
	f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), len(rnd))
	f.Write(rnd)

	buf := make([]byte, headerSize+len(f.data))

	first := (uint32(f.ver) << 28) | (uint32(f.cmd&0x0f) << 24) |
		(uint32(f.Len()+1) & 0xffffff)
	binary.LittleEndian.PutUint32(buf[:], first) // incorrect size
	binary.LittleEndian.PutUint32(buf[4:], f.sid)
	copy(buf[headerSize:], rnd)

	session.conn.Write(buf)
	cli.Close()

	// writeFrame after die
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)
	// close first
	session.Close()
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), 0)
		session.writeFrame(f)
	}
}

func TestWriteFrameInternal(t *testing.T) {
	addr, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// pure random
	session, _ := Client(cli, nil)
	for i := 0; i < 100; i++ {
		rnd := make([]byte, rand.Uint32()%1024)
		io.ReadFull(crand.Reader, rnd)
		session.conn.Write(rnd)
	}
	cli.Close()

	// writeFrame after die
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	session, _ = Client(cli, nil)
	// close first
	session.Close()
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), 0)
		deadline := writeDealine{
			time: time.Now().Add(session.config.KeepAliveTimeout),
			wait: time.After(session.config.KeepAliveTimeout),
		}
		session.writeFrameInternal(f, deadline, CLSDATA)
	}

	// random cmds
	cli, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	allcmds := []byte{cmdSYN, cmdFIN, cmdPSH, cmdPIN, cmdPON}
	session, _ = Client(cli, nil)
	for i := 0; i < 100; i++ {
		f, _ := session.newFrameWrite(allcmds[rand.Int()%len(allcmds)], rand.Uint32(), 0)
		deadline := writeDealine{
			time: time.Now().Add(session.config.KeepAliveTimeout),
			wait: time.After(session.config.KeepAliveTimeout),
		}
		session.writeFrameInternal(f, deadline, CLSDATA)
	}
	// deadline occur
	{
		c := make(chan time.Time)
		close(c)
		f, _ := session.newFrameWrite(allcmds[rand.Int()%len(allcmds)], rand.Uint32(), 0)
		_, err = session.writeFrameInternal(f, writeDealine{wait: c}, CLSDATA)
		if !strings.Contains(err.Error(), "timeout") {
			t.Fatal("write frame with deadline failed", err)
		}
	}
	cli.Close()

	{
		cli, err = net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		config := DefaultConfig()
		config.KeepAliveInterval = time.Second
		config.KeepAliveTimeout = 2 * time.Second
		session, _ = Client(&blockWriteConn{Conn: cli}, config)
		f, _ := session.newFrameWrite(byte(rand.Uint32()), rand.Uint32(), 0)
		c := make(chan time.Time)
		go func() {
			// die first, deadline second, better for coverage
			time.Sleep(time.Second)
			session.Close()
			time.Sleep(time.Second)
			close(c)
		}()
		_, err = session.writeFrameInternal(f, writeDealine{wait: c}, CLSDATA)
		if !strings.Contains(err.Error(), "closed pipe") {
			t.Fatal("write frame with to closed conn failed", err)
		}
	}
}

func TestReadDeadline(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	const N = 100
	var readErr error
	for i := 0; i < N; i++ {
		stream.SetReadDeadline(time.Now().Add(-1 * time.Minute))
		if _, readErr = stream.ReadFrame(); readErr != nil {
			break
		}
	}
	if readErr != nil {
		if !strings.Contains(readErr.Error(), "timeout") {
			t.Fatalf("Wrong error: %v", readErr)
		}
	} else {
		t.Fatal("No error when reading with past deadline")
	}
	session.Close()
}

func TestWriteDeadline(t *testing.T) {
	_, stop, cli, err := setupServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	stream, _ := session.OpenStream()
	var writeErr error
	fw, _ := stream.AllocFrame(10)
	fw.Write(make([]byte, 10))
	stream.SetWriteDeadline(time.Now().Add(-1 * time.Minute))
	if _, writeErr = stream.WriteFrame(fw); writeErr != nil {
		if !strings.Contains(writeErr.Error(), "timeout") {
			t.Fatalf("Wrong error: %v", writeErr)
		}
	}
	fw.Close()
	session.Close()
}

func BenchmarkAcceptClose(b *testing.B) {
	_, stop, cli, err := setupServer(b)
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	session, _ := Client(cli, nil)
	for i := 0; i < b.N; i++ {
		if stream, err := session.OpenStream(); err == nil {
			stream.Close()
		} else {
			b.Fatal(err)
		}
	}
}

type streamRW struct {
	s *Stream
}

func (s streamRW) Read(p []byte) (n int, err error) {
	f, _ := s.s.ReadFrame()
	n, err = f.Read(p)
	f.Close()
	return
}

func (s streamRW) Write(p []byte) (n int, err error) {
	f, _ := s.s.AllocFrame(len(p))
	f.Write(p)
	s.s.WriteFrame(f)
	f.Close()
	return len(p), nil
}

func BenchmarkConnSmux(b *testing.B) {
	cs, ss, err := getSmuxStreamPair(false)
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
	bench(b, streamRW{cs}, streamRW{ss})
}

func BenchmarkPipeSmux(b *testing.B) {
	cs, ss, err := getSmuxStreamPair(true)
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
	bench(b, streamRW{cs}, streamRW{ss})
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

type readWriter struct {
	remain []byte
	ch     chan []byte
}

func (rw *readWriter) Read(p []byte) (n int, err error) {
	if len(rw.remain) == 0 {
		b, ok := <-rw.ch
		if !ok {
			return 0, io.EOF
		}
		rw.remain = b
	}
	n = copy(p, rw.remain)
	rw.remain = rw.remain[n:]
	return
}

func (rw *readWriter) Write(p []byte) (n int, err error) {
	rw.ch <- p
	return len(p), nil
}

func (rw *readWriter) Close() error {
	close(rw.ch)
	return nil
}

type rwPipe struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (rw *rwPipe) Read(p []byte) (n int, err error) {
	return rw.r.Read(p)
}

func (rw *rwPipe) WriteTo(w io.Writer) (n int64, err error) {
	return io.Copy(w, rw)
}

func (rw *rwPipe) Write(p []byte) (n int, err error) {
	return rw.w.Write(p)
}

func (rw *rwPipe) Close() error {
	rw.r.Close()
	rw.w.Close()
	return nil
}

func newPipe() (io.ReadWriteCloser, io.ReadWriteCloser) {
	p1 := &readWriter{ch: make(chan []byte, 1)}
	p2 := &readWriter{ch: make(chan []byte, 1)}
	return &rwPipe{r: p1, w: p2}, &rwPipe{r: p2, w: p1}
}

func getSmuxStreamPair(piped bool) (*Stream, *Stream, error) {
	var c1, c2 io.ReadWriteCloser
	var err error
	if piped {
		c1, c2 = newPipe()
	} else {
		c1, c2, err = getTCPConnectionPair()
		if err != nil {
			return nil, nil, err
		}
	}

	s, err := Server(c2, nil)
	if err != nil {
		return nil, nil, err
	}
	c, err := Client(c1, nil)
	if err != nil {
		return nil, nil, err
	}
	var ss *Stream
	done := make(chan error)
	go func() {
		var rerr error
		ss, rerr = s.AcceptStream()
		done <- rerr
		close(done)
	}()
	cs, err := c.OpenStream()
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
	runtime.GC()

	const size = 128 * 1024

	buf := make([]byte, size)
	buf2 := make([]byte, size)
	b.SetBytes(size)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			n, _ := rd.Read(buf2)
			count += n
			if count == size*b.N {
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		wr.Write(buf)
	}
	wg.Wait()
}
