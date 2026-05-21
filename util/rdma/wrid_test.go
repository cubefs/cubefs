package rdma

import "testing"

func TestEncodeDecodeWRID_Roundtrip(t *testing.T) {
	cases := []struct {
		op   wrOp
		slot int
	}{
		{opSlot, 0},
		{opDoorbell, 1},
		{opCredit, 255},
		{opRecv, 1023},
		{opShutdownPing, 0},
	}
	for _, c := range cases {
		wrID := encodeWRID(c.op, c.slot)
		gotOp, gotSlot := decodeWRID(wrID)
		if gotOp != c.op || gotSlot != c.slot {
			t.Errorf("encode(%v,%d)=0x%016x → decode=(%v,%d)", c.op, c.slot, wrID, gotOp, gotSlot)
		}
	}
}

func TestDecodeWRID_UnknownOp(t *testing.T) {
	// Raw 0 should decode to opUnknown, slot 0 — surfaces forgotten encode calls.
	op, slot := decodeWRID(0)
	if op != opUnknown {
		t.Errorf("decode(0): op=%v want opUnknown", op)
	}
	if slot != 0 {
		t.Errorf("decode(0): slot=%d want 0", slot)
	}
}

func TestDecodeWRID_SeparateOps(t *testing.T) {
	// Different op codes must produce distinct WR IDs even at the same slot.
	a := encodeWRID(opSlot, 5)
	b := encodeWRID(opDoorbell, 5)
	c := encodeWRID(opCredit, 5)
	d := encodeWRID(opRecv, 5)
	all := []uint64{a, b, c, d}
	for i, x := range all {
		for j, y := range all {
			if i != j && x == y {
				t.Errorf("WR ID collision: ops at indices %d and %d both encode to 0x%016x", i, j, x)
			}
		}
	}
}

func TestWrOp_String(t *testing.T) {
	cases := map[wrOp]string{
		opSlot:         "slot",
		opDoorbell:     "doorbell",
		opCredit:       "credit",
		opRecv:         "recv",
		opShutdownPing: "shutdown",
		opUnknown:      "unknown",
	}
	for op, want := range cases {
		if got := op.String(); got != want {
			t.Errorf("wrOp(%d).String() = %q want %q", op, got, want)
		}
	}
}
