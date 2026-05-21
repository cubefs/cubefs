//go:build linux && rdma

package rdma

import (
	"testing"
)

func TestMarshalConnectInfoRoundtrip(t *testing.T) {
	orig := ConnectInfo{
		RespRkey:   0xDEADBEEF,
		RespBaseVA: 0x7F00000000001000,
		RespDbRkey: 0xCAFEBABE,
		RespDbVA:   0x7F00000000002000,
		NumSlots:   256,
		SlotSize:   128 * 1024 * 1024,
		CreditRkey: 0x0BADF00D,
		CreditVA:   0x7F00000000005000,
	}
	b := MarshalConnectInfo(orig)
	if len(b) > 56 {
		t.Fatalf("ConnectInfo serialized to %d bytes, max is 56 (rdma_cm limit)", len(b))
	}
	got, err := UnmarshalConnectInfo(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != orig {
		t.Fatalf("roundtrip mismatch:\n  orig=%+v\n  got=%+v", orig, got)
	}
}

func TestMarshalAcceptInfoRoundtrip(t *testing.T) {
	orig := AcceptInfo{
		ReqRkey:    0x11223344,
		ReqBaseVA:  0x7F00000000003000,
		DbRkey:     0x55667788,
		DbVA:       0x7F00000000004000,
		NumSlots:   128,
		SlotSize:   64 * 1024,
		CreditRkey: 0xFEEDC0DE,
		CreditVA:   0x7F00000000006000,
	}
	b := MarshalAcceptInfo(orig)
	if len(b) > 56 {
		t.Fatalf("AcceptInfo serialized to %d bytes, max is 56 (rdma_cm limit)", len(b))
	}
	got, err := UnmarshalAcceptInfo(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != orig {
		t.Fatalf("roundtrip mismatch:\n  orig=%+v\n  got=%+v", orig, got)
	}
}

func TestUnmarshalConnectInfoTooShort(t *testing.T) {
	_, err := UnmarshalConnectInfo(make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for short input")
	}
}

func TestUnmarshalAcceptInfoTooShort(t *testing.T) {
	_, err := UnmarshalAcceptInfo(make([]byte, 5))
	if err == nil {
		t.Fatal("expected error for short input")
	}
}

// TestConnectInfoBytesAtRDMACMLimit ensures we are exactly at the 56-byte
// rdma_cm private_data limit; any further field additions require splitting
// the handshake into a post-ESTABLISHED phase.
func TestConnectInfoBytesAtRDMACMLimit(t *testing.T) {
	if connectInfoSize != 56 {
		t.Fatalf("connectInfoSize=%d; rdma_cm private_data limit is 56", connectInfoSize)
	}
	if acceptInfoSize != 56 {
		t.Fatalf("acceptInfoSize=%d; rdma_cm private_data limit is 56", acceptInfoSize)
	}
}
