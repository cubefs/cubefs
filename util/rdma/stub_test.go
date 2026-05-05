//go:build !rdma

package rdma

import "testing"

// TestStubRDMAConn verifies the stub RDMAConn methods return safe zero values
// and that RecvSeq/SetRecvSeq are no-ops on the stub.
func TestStubRDMAConn(t *testing.T) {
	c := &RDMAConn{}

	if c.NumSlots() != 0 {
		t.Fatalf("NumSlots: got %d want 0", c.NumSlots())
	}
	if c.SlotSize() != 0 {
		t.Fatalf("SlotSize: got %d want 0", c.SlotSize())
	}
	if c.RecvSlotBytes(0) != nil {
		t.Fatalf("RecvSlotBytes: want nil")
	}
	if c.SendScratchBytes(0) != nil {
		t.Fatalf("SendScratchBytes: want nil")
	}
	if c.RemoteAddr() != "" {
		t.Fatalf("RemoteAddr: want empty string")
	}
	if !c.IsClosed() {
		t.Fatalf("IsClosed: stub should always return true")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seq, ok := c.PollRecvDoorbell(0, 0)
	if ok || seq != 0 {
		t.Fatalf("PollRecvDoorbell: got (%d, %v) want (0, false)", seq, ok)
	}

	// RecvSeq always returns 0 on the stub
	if got := c.RecvSeq(0); got != 0 {
		t.Fatalf("RecvSeq: got %d want 0", got)
	}
	// SetRecvSeq is a no-op; subsequent RecvSeq should still return 0
	c.SetRecvSeq(0, 42)
	if got := c.RecvSeq(0); got != 0 {
		t.Fatalf("RecvSeq after SetRecvSeq: got %d want 0 (stub must ignore set)", got)
	}
}

// TestStubRDMAConnPool verifies NewRDMAConnPool returns an error on non-RDMA builds.
func TestStubRDMAConnPool(t *testing.T) {
	_, err := NewRDMAConnPool(RDMAPoolConfig{NumSlots: 1, SlotSize: 4096})
	if err == nil {
		t.Fatal("NewRDMAConnPool: expected error on non-RDMA build")
	}
}

// TestStubListen verifies Listen returns an error on non-RDMA builds.
func TestStubListen(t *testing.T) {
	_, err := Listen(9999, RDMAConnConfig{NumSlots: 1, SlotSize: 4096})
	if err == nil {
		t.Fatal("Listen: expected error on non-RDMA build")
	}
}
