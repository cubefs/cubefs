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
	// Fresh stub conn is open by design — the SlotPool unit tests in non-RDMA
	// builds rely on this so they can exercise allocation logic without
	// needing real RDMA hardware. After Close the stub flips to true.
	if c.IsClosed() {
		t.Fatalf("IsClosed: fresh stub should report open")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsClosed() {
		t.Fatalf("IsClosed: closed stub should report closed")
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

	// ReturnCredit is a no-op on the stub — must not return errNotSupported
	// so the same call site works in both builds without build-tag guards.
	if err := c.ReturnCredit(0); err != nil {
		t.Fatalf("ReturnCredit on stub: %v", err)
	}
	if sent, recv, proc := c.CreditStats(); sent != 0 || recv != 0 || proc != 0 {
		t.Fatalf("CreditStats on stub: got (%d,%d,%d) want all 0", sent, recv, proc)
	}
}

// TestStubRDMAConnPool verifies NewRDMAConnPool returns an error on non-RDMA builds.
func TestStubRDMAConnPool(t *testing.T) {
	_, err := NewRDMAConnPool(RDMAPoolConfig{NumSlots: 1, SlotSize: MinValidSlotSize})
	if err == nil {
		t.Fatal("NewRDMAConnPool: expected error on non-RDMA build")
	}
}

// TestStubListen verifies Listen returns an error on non-RDMA builds.
func TestStubListen(t *testing.T) {
	_, err := Listen(9999, RDMAConnConfig{NumSlots: 1, SlotSize: MinValidSlotSize})
	if err == nil {
		t.Fatal("Listen: expected error on non-RDMA build")
	}
}

// TestStubConfigsCarryCreditAckMode verifies the new CreditAckMode field is
// reachable on both config structs in stub builds, so callers can configure
// it unconditionally.
func TestStubConfigsCarryCreditAckMode(t *testing.T) {
	cc := RDMAConnConfig{CreditAckMode: CreditAckAsync}
	if cc.CreditAckMode != CreditAckAsync {
		t.Fatalf("RDMAConnConfig.CreditAckMode lost in stub")
	}
	pc := RDMAPoolConfig{CreditAckMode: CreditAckAsync}
	if pc.CreditAckMode != CreditAckAsync {
		t.Fatalf("RDMAPoolConfig.CreditAckMode lost in stub")
	}
}
