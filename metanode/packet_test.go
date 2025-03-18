package metanode

import "testing"

func TestClearPacket(t *testing.T) {
	p := &Packet{}
	p.Opcode = 3
	ClearPacket(p)
	if 0 != p.Opcode {
		t.Fatalf("ClearPacket failed")
	}
}
