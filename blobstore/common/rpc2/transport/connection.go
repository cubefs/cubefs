package transport

import (
	"io"
	"net"
	"time"
)

// Conn with tcp & rdma connection
type Conn interface {
	// Allocator returns a read-write buffer memory pool on the connection.
	Allocator() Allocator
	// ReadBuffer returns an assigned buffer from allocator.
	// if success, buffer.Len() == n
	ReadBuffer(n int) (buffer AssignedBuffer, err error)
	// WriteBuffer writes buffer.
	WriteBuffer(buffer AssignedBuffer) (n int, err error)

	Read(p []byte) (n int, err error)
	Close() error

	LocalAddr() net.Addr
	RemoteAddr() net.Addr

	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type WriteBuffers interface {
	WriteBuffers(buffers []AssignedBuffer) (n int, err error)
}

type netConn struct {
	net.Conn
	allocator Allocator
}

func (c *netConn) Allocator() Allocator {
	return c.allocator
}

func (c *netConn) ReadBuffer(n int) (AssignedBuffer, error) {
	buffer, err := c.Allocator().Alloc(n)
	if err != nil {
		return nil, err
	}
	if _, err = io.ReadFull(c.Conn, buffer.Bytes()); err != nil {
		buffer.Free()
		return nil, err
	}
	buffer.Written(n)
	return buffer, nil
}

func (c *netConn) WriteBuffer(buffer AssignedBuffer) (n int, err error) {
	buf := buffer.Bytes()[:buffer.Len()]
	n, err = c.Conn.Write(buf)
	if n != len(buf) && err == nil {
		err = io.ErrShortWrite
	}
	return
}

type netConnv struct{ netConn }

var _ WriteBuffers = netConnv{}

func (c netConnv) WriteBuffers(buffers []AssignedBuffer) (int, error) {
	v := make(net.Buffers, 0, len(buffers))
	for _, buffer := range buffers {
		v = append(v, buffer.Bytes()[:buffer.Len()])
	}
	nn, err := v.WriteTo(c.netConn.Conn)
	return int(nn), err
}

// NetConn transfer net connection to Conn.
func NetConn(conn net.Conn, allocator Allocator, writev bool) Conn {
	if allocator == nil {
		allocator = defaultAllocator
	}
	tc := netConn{Conn: conn, allocator: allocator}
	if writev {
		return &netConnv{netConn: tc}
	}
	return &tc
}
