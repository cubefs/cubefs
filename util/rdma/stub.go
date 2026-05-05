//go:build !(linux && rdma)

// Package rdma provides RDMA transport support for CubeFS.
// This file contains stubs for non-RDMA builds (default).
// All functionality is disabled; callers should check rdmaEnable config before use.
package rdma

import (
	"errors"
	"time"

	"github.com/cubefs/cubefs/proto"
)

var errNotSupported = errors.New("rdma: not supported in this build (compile with -tags rdma on Linux)")

// ConnectInfo and AcceptInfo are exported so callers can reference the types
// without conditional compilation in non-hot-path code.
type ConnectInfo struct {
	RespRkey, RespDbRkey   uint32
	RespBaseVA, RespDbVA   uint64
	NumSlots, SlotSize     uint32
}

type AcceptInfo struct {
	ReqRkey, DbRkey    uint32
	ReqBaseVA, DbVA    uint64
	NumSlots, SlotSize uint32
}

type RDMAMem struct{}

func (m *RDMAMem) Free()                          {}
func (m *RDMAMem) Bytes() []byte                  { return nil }
func (m *RDMAMem) SlotBytes(_, _ int) []byte      { return nil }

type RDMAConn struct{}

func (c *RDMAConn) NumSlots() int                      { return 0 }
func (c *RDMAConn) SlotSize() int                      { return 0 }
func (c *RDMAConn) RecvSlotBytes(_ int) []byte         { return nil }
func (c *RDMAConn) SendScratchBytes(_ int) []byte      { return nil }
func (c *RDMAConn) RemoteAddr() string                 { return "" }
func (c *RDMAConn) IsClosed() bool                     { return true }
func (c *RDMAConn) Close() error                       { return nil }
func (c *RDMAConn) WritePacket(_ int, _ *proto.Packet) error { return errNotSupported }
func (c *RDMAConn) WriteData(_ int, _ []byte) error    { return errNotSupported }
func (c *RDMAConn) PollRecvDoorbell(_ int, _ uint32) (uint32, bool) { return 0, false }
func (c *RDMAConn) RecvSeq(_ int) uint32               { return 0 }
func (c *RDMAConn) SetRecvSeq(_ int, _ uint32)         {}

type RDMAConnConfig struct {
	NumSlots int
	SlotSize int
}

type RDMAPoolConfig struct {
	Device      string
	Port        int
	NumSlots    int
	SlotSize    int
	MaxConns    int
	IdleTimeout time.Duration
}

type RDMAConnPool struct{}

func NewRDMAConnPool(_ RDMAPoolConfig) (*RDMAConnPool, error) {
	return nil, errNotSupported
}
func (p *RDMAConnPool) GetConnect(_ string) (*RDMAConn, error) { return nil, errNotSupported }
func (p *RDMAConnPool) PutConnect(_ *RDMAConn, _ bool)         {}
func (p *RDMAConnPool) Close()                                  {}

type RDMAListener struct{}

func Listen(_ int, _ RDMAConnConfig) (*RDMAListener, error)  { return nil, errNotSupported }
func (l *RDMAListener) Accept() (*RDMAConn, error)           { return nil, errNotSupported }
func (l *RDMAListener) Close() error                         { return nil }
