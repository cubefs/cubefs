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

package rpc2

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type NetworkAddress struct {
	Network string `json:"network"`
	Address string `json:"address"`
}

func newListener(addr NetworkAddress) (net.Listener, error) {
	switch addr.Network {
	case "tcp":
		return net.Listen(addr.Network, addr.Address)
	default:
		return nil, errors.New("rpc2: not implements " + addr.Network)
	}
}

type Server struct {
	Name      string           `json:"name"`
	Addresses []NetworkAddress `json:"addresses"`

	Handler Handler `json:"-"`

	// Requst Header |
	//  No Timeout   |
	//               |  Request Body  |
	//               |  ReadTimeout   |
	//               |  Response Header Body |
	//               |  WriteTimeout         |
	ReadTimeout  util.Duration `json:"read_timeout"`
	WriteTimeout util.Duration `json:"write_timeout"`

	Transport          *TransportConfig `json:"transport,omitempty"`
	BufioReaderSize    int              `json:"bufio_reader_size"`
	BufioWriterSize    int              `json:"bufio_writer_size"`
	BufioFlushDuration util.Duration    `json:"bufio_flush_duration"`

	StatDuration util.Duration `json:"stat_duration"`
	statOnce     sync.Once

	inServe    atomic.Value // true when server waiting to accept
	inShutdown atomic.Value // true when server is in shutdown

	listenerGroup sync.WaitGroup
	mu            sync.Mutex
	listeners     map[*net.Listener]struct{}
	sessions      map[*transport.Session]struct{}
	onShutdown    []func()
}

func (s *Server) setReadTimeout(stream *transport.Stream) {
	if s.ReadTimeout.Duration > 0 {
		stream.SetReadDeadline(time.Now().Add(s.ReadTimeout.Duration))
	}
}

func (s *Server) setWriteTimeout(stream *transport.Stream) {
	if s.WriteTimeout.Duration > 0 {
		stream.SetWriteDeadline(time.Now().Add(s.WriteTimeout.Duration))
	}
}

func (s *Server) stating() {
	s.statOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(s.StatDuration.Duration)
			defer ticker.Stop()
			for range ticker.C {
				if s.shuttingDown() {
					return
				}
				log.Debug("stating on", s.Name)
				s.mu.Lock()
				log.Debugf("server has %d listeners", len(s.listeners))
				log.Debugf("server has %d sessions", len(s.sessions))
				for sess := range s.sessions {
					log.Debugf("session %v has %d streams", sess.LocalAddr(), sess.NumStreams())
				}
				s.mu.Unlock()
			}
		}()
	})
}

func (s *Server) WaitServe() {
	for {
		if val := s.inServe.Load(); val != nil {
			if val.(bool) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Server) shuttingDown() bool {
	if val := s.inShutdown.Load(); val != nil {
		return val.(bool)
	}
	return false
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.inShutdown.Store(true)

	var err error
	s.mu.Lock()
	for ln := range s.listeners {
		if cerr := (*ln).Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	for _, f := range s.onShutdown {
		go f()
	}
	s.mu.Unlock()

	log.Warn("shutdown and try to sleep 5 senconds")
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	return err
}

func (s *Server) RegisterOnShutdown(f func()) {
	s.mu.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.mu.Unlock()
}

func (s *Server) Serve() error {
	if len(s.Addresses) == 0 {
		return nil
	}
	if len(s.Addresses) > 1 {
		for _, addr := range s.Addresses[1:] {
			ln, err := newListener(addr)
			if err != nil {
				return err
			}
			go s.Listen(ln)
		}
	}
	ln, err := newListener(s.Addresses[0])
	if err != nil {
		return err
	}
	err = s.Listen(ln)
	s.listenerGroup.Wait()
	return err
}

func (s *Server) Listen(ln net.Listener) error {
	ln = &onceCloseListener{Listener: ln}

	key := &ln
	s.mu.Lock()
	if s.listeners == nil {
		s.listeners = make(map[*net.Listener]struct{})
	}
	if s.sessions == nil {
		s.sessions = make(map[*transport.Session]struct{})
	}
	_, has := s.listeners[key]
	if has {
		s.mu.Unlock()
		return nil
	}

	s.listeners[key] = struct{}{}
	s.listenerGroup.Add(1)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if _, has := s.listeners[key]; has {
			ln.Close()
			delete(s.listeners, key)
			s.listenerGroup.Done()
		}
		s.mu.Unlock()
	}()

	if s.StatDuration.Duration > 0 {
		s.stating()
	}
	if s.Transport == nil {
		s.Transport = DefaultTransportConfig()
	}

	for {
		s.inServe.Store(true)
		conn, err := ln.Accept()
		if err != nil {
			if s.shuttingDown() {
				return ErrServerClosed
			}
			log.Errorf("listener %v accept, %s", ln.Addr(), err.Error())
			return err
		}

		bufConn := newBufioConn(tcpConn{conn},
			s.BufioReaderSize, s.BufioWriterSize, s.BufioFlushDuration.Duration)
		sess, err := transport.Server(bufConn, s.Transport.Transport())
		if err != nil {
			log.Errorf("listener %v transport %v, %s",
				ln.Addr(), conn.RemoteAddr(), err.Error())
			return err
		}
		go s.handleSession(sess)
	}
}

func (s *Server) handleSession(sess *transport.Session) {
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
	for {
		if stream, err := sess.AcceptStream(); err == nil {
			go s.handleStream(stream)
		} else {
			log.Errorf("session %v accept stream %v, %s",
				sess.LocalAddr(), sess.RemoteAddr(), err.Error())
			break
		}
	}
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}

func (s *Server) handleStream(stream *transport.Stream) {
	ctx := context.Background()
	if err := func() error {
		for {
			req, err := s.readRequest(stream)
			if err != nil {
				return err
			}
			ctx = req.Context()

			resp := &response{conn: stream}
			if ss := req.stream; ss != nil {
				if err = s.Handler.Handle(resp, req); err != nil {
					status, reason, detail := DetectError(err)
					ss.hdr.Status = int32(status)
					ss.hdr.Reason = reason
					ss.hdr.Error = detail.Error()
					getSpan(ctx).Warn(err)
				} else {
					ss.hdr.Status = 200
				}
				ss.sentHeader = false
				ss.SendHeader(nil)
				stream.Close()
				return err
			}

			resp.options(req)
			if err = s.Handler.Handle(resp, req); err != nil {
				if resp.hasWroteHeader {
					req.Span().Warn("handle error but header has wrote", err)
				} else {
					status, reason, detail := DetectError(err)
					resp.hdr.Reason = reason
					resp.hdr.Error = detail.Error()
					resp.WriteHeader(status, NoParameter)
					getSpan(ctx).Warn(err)
				}
			}

			resp.WriteOK(nil)
			if err = resp.Flush(); err != nil {
				return err
			}
			if err = req.Body.Close(); err != nil {
				return err
			}
			if resp.connBroken {
				return errors.New("stream conn has broken")
			}
		}
	}(); err != nil {
		span := getSpan(ctx)
		span.Errorf("stream(%d, %v, %v) %s", stream.ID(), stream.LocalAddr(), stream.RemoteAddr(), err.Error())
		stream.Close()
	}
}

func (s *Server) readRequest(stream *transport.Stream) (*Request, error) {
	var hdr RequestHeader
	frame, err := readHeaderFrame(stream, &hdr)
	if err != nil {
		return nil, err
	}

	switch hdr.StreamCmd {
	case StreamCmd_NOT, StreamCmd_SYN:
	case StreamCmd_PSH, StreamCmd_FIN:
		return nil, ErrFrameProtocol
	default:
		return nil, ErrFrameProtocol
	}

	traceID := hdr.TraceID
	if traceID == "" {
		traceID = trace.RandomID().String()
	}
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", traceID)

	req := &Request{RequestHeader: hdr, ctx: ctx, conn: stream}
	if sum := hdr.Header.Get(HeaderInternalChecksum); sum != "" {
		block, err := unmarshalBlock([]byte(sum))
		if err != nil {
			frame.Close()
			return nil, err
		}
		req.checksum = block
	}

	decode := req.checksum != nil && req.checksum.Direction.IsUpload()
	payloadSize := req.Trailer.AllSize()
	if decode {
		payloadSize += int(req.checksum.EncodeSize(req.ContentLength))
	} else {
		payloadSize += int(req.ContentLength)
	}
	req.Body = makeBodyWithTrailer(stream.NewSizedReader(payloadSize, frame),
		req, &req.Trailer, req.ContentLength, decode)

	if hdr.StreamCmd == StreamCmd_SYN {
		req.stream = &serverStream{req: req}
	}

	s.setReadTimeout(stream)
	s.setWriteTimeout(stream)
	return req, nil
}

type onceCloseListener struct {
	net.Listener
	once sync.Once
	err  error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(func() { oc.err = oc.Listener.Close() })
	return oc.err
}
