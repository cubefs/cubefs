// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/require"
)

type MockNSQD struct {
	t           *testing.T
	addr        string
	tcpListener net.Listener
}

func NewMockNSQD(t *testing.T, addr string) *MockNSQD {
	n := &MockNSQD{t: t}

	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		n.t.Fatalf("FATAL: listen (%s) failed - %s", addr, err)
	}

	n.tcpListener = tcpListener
	n.addr = tcpListener.Addr().String()

	go n.listen()

	return n
}

func (m *MockNSQD) listen() {
	m.t.Logf("TCP: listening on %s", m.tcpListener.Addr())

	for {
		conn, err := m.tcpListener.Accept()
		if err != nil {
			m.t.Logf("ERROR: accept failed - %s", err)
			break
		}
		go m.handle(conn)
	}

	m.t.Logf("TCP: closing %s", m.tcpListener.Addr())
}

func (m *MockNSQD) handle(conn net.Conn) {
	m.t.Logf("TCP: new client(%s)", conn.RemoteAddr())
	defer func() {
		m.t.Logf("TCP: closing client(%s) with err: %v", conn.RemoteAddr(), conn.Close())
	}()

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		m.t.Fatalf("ERROR: failed to read protocol version - %s", err)
	}

	rdr := bufio.NewReader(conn)
	for {
		line, err := rdr.ReadBytes('\n')
		if err != nil {
			m.t.Logf("TCP: read error: %v", err)
			return
		}
		line = line[:len(line)-1]
		m.t.Logf("TCP: read %s", line)

		params := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(params[0], []byte("IDENTIFY")):
		case bytes.Equal(params[0], []byte("PUB")):
		default:
			continue
		}

		size := make([]byte, 4)
		if _, err = io.ReadFull(rdr, size); err != nil {
			m.t.Logf("TCP: read error: %v", err)
			return
		}
		b := make([]byte, binary.BigEndian.Uint32(size))
		if _, err = io.ReadFull(rdr, b); err != nil {
			m.t.Logf("TCP: read error: %v", err)
			return
		}
		m.t.Logf("TCP: read %s", b)
		_, err = conn.Write(nsqFramedResponse(nsq.FrameTypeResponse, []byte("OK")))
		if err != nil {
			m.t.Logf("TCP: write error: %v", err)
		}
	}
}

func (m *MockNSQD) Addr() string {
	return m.addr
}

func (m *MockNSQD) Close() error {
	return m.tcpListener.Close()
}

func nsqFramedResponse(frameType int32, data []byte) []byte {
	var w bytes.Buffer

	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	_, err := w.Write(beBuf)
	if err != nil {
		return nil
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	_, err = w.Write(beBuf)
	if err != nil {
		return nil
	}

	_, err = w.Write(data)
	return w.Bytes()
}

func TestNewNsqNotifier(t *testing.T) {
	conf := NsqNotifierConfig{}
	_, err := NewNsqNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Topic = "nsq-notifier-unit-test"
	_, err = NewNsqNotifier("cubefs", conf)
	require.Error(t, err)

	conf.NsqdAddr = "127.0.0.1"
	_, err = NewNsqNotifier("cubefs", conf)
	require.Error(t, err)

	conf.NsqdAddr = "127.0.0.1:14150"
	_, err = NewNsqNotifier("cubefs", conf)
	require.Error(t, err)

	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	nsqd := NewMockNSQD(t, addr.String())
	defer nsqd.Close()

	conf.NsqdAddr = nsqd.Addr()
	notifier, err := NewNsqNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestNsqNotifier(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	nsqd := NewMockNSQD(t, addr.String())
	defer nsqd.Close()

	conf := NsqNotifierConfig{}
	conf.Topic = "nsq-notifier-unit-test"
	conf.NsqdAddr = nsqd.Addr()
	notifier, err := NewNsqNotifier("cubefs", conf)
	require.NoError(t, err)

	require.Equal(t, "cubefs:nsq", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "nsq"}, notifier.ID())

	require.NoError(t, notifier.Send([]byte("test")))
	require.NoError(t, notifier.Close())
	require.ErrorContains(t, notifier.Send([]byte("test")), "closed")

}
