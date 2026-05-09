//go:build !(linux && rdma)

// Package rdma provides RDMA transport support for CubeFS.
// This file contains stubs for non-RDMA builds (default).
// All functionality is disabled; callers should check rdmaEnable config before use.
package rdma

import (
	"errors"

	"github.com/cubefs/cubefs/proto"
)

var errNotSupported = errors.New("rdma: not supported in this build (compile with -tags rdma on Linux)")

// ConnectInfo and AcceptInfo are exported so callers can reference the types
// without conditional compilation in non-hot-path code.
type ConnectInfo struct {
	RespRkey, RespDbRkey uint32
	RespBaseVA, RespDbVA uint64
	NumSlots, SlotSize   uint32
	CreditRkey           uint32
	CreditVA             uint64
}

type AcceptInfo struct {
	ReqRkey, DbRkey    uint32
	ReqBaseVA, DbVA    uint64
	NumSlots, SlotSize uint32
	CreditRkey         uint32
	CreditVA           uint64
}

type RDMAMem struct{}

func (m *RDMAMem) Free()                     {}
func (m *RDMAMem) Bytes() []byte             { return nil }
func (m *RDMAMem) SlotBytes(_, _ int) []byte { return nil }

// RDMAConn is a minimal stub. Methods are no-ops so the same call sites
// compile in both builds; the SlotPool tests in non-rdma builds exercise
// allocation logic without ever invoking these methods on real hardware.
type RDMAConn struct {
	numSlots int
	closed   bool
}

func (c *RDMAConn) NumSlots() int                                   { return c.numSlots }
func (c *RDMAConn) SlotSize() int                                   { return 0 }
func (c *RDMAConn) RecvSlotBytes(_ int) []byte                      { return nil }
func (c *RDMAConn) SendScratchBytes(_ int) []byte                   { return nil }
func (c *RDMAConn) RemoteAddr() string                              { return "" }
func (c *RDMAConn) IsClosed() bool                                  { return c.closed }
func (c *RDMAConn) Close() error                                    { c.closed = true; return nil }
func (c *RDMAConn) WritePacket(_ int, _ *proto.Packet) error        { return errNotSupported }
func (c *RDMAConn) WriteData(_ int, _ []byte) error                 { return errNotSupported }
func (c *RDMAConn) WriteSlotZeroCopy(_, _ int) error                { return errNotSupported }
func (c *RDMAConn) PollRecvDoorbell(_ int, _ uint32) (uint32, bool) { return 0, false }
func (c *RDMAConn) RecvSeq(_ int) uint32                            { return 0 }
func (c *RDMAConn) SetRecvSeq(_ int, _ uint32)                      {}
func (c *RDMAConn) ReturnCredit(_ int) error                        { return nil }
func (c *RDMAConn) RecvDoneSeq(_ int) uint64                        { return 0 }
func (c *RDMAConn) RecvSignalSeq() uint64                           { return 0 }

// CreditStats returns zeros on non-RDMA builds.
func (c *RDMAConn) CreditStats() (sent, received, processed uint64) { return 0, 0, 0 }

// PollConfig returns the zero config on non-RDMA builds.
func (c *RDMAConn) PollConfig() PollConfig { return PollConfig{} }

// Role returns the empty string in non-RDMA builds — disables metric
// emission since metrics_stub.go's helpers no-op when role == "".
func (c *RDMAConn) Role() string { return "" }

// NewRDMAConnPool returns errNotSupported on non-RDMA builds. The
// underlying RDMAConnPool type lives in slot_pool.go and is built
// regardless of tag, so type references work even when the constructor
// can't actually open a connection.
func NewRDMAConnPool(_ RDMAPoolConfig) (*RDMAConnPool, error) {
	return nil, errNotSupported
}

type RDMAListener struct{}

func Listen(_ int, _ RDMAConnConfig) (*RDMAListener, error) { return nil, errNotSupported }
func (l *RDMAListener) Accept() (*RDMAConn, error)          { return nil, errNotSupported }
func (l *RDMAListener) Close() error                        { return nil }
