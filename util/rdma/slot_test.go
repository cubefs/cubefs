//go:build linux && rdma

package rdma

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

func TestSlotHeaderRoundtrip(t *testing.T) {
	buf := make([]byte, SlotHeaderSize)
	WriteSlotHeader(buf, 42, 1234)
	h, err := ReadSlotHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if h.Magic != slotMagic {
		t.Errorf("magic mismatch: got 0x%08x", h.Magic)
	}
	if h.Seq != 42 {
		t.Errorf("seq: got %d want 42", h.Seq)
	}
	if h.TotalLen != 1234 {
		t.Errorf("totalLen: got %d want 1234", h.TotalLen)
	}
}

func TestSlotHeaderBadMagic(t *testing.T) {
	buf := make([]byte, SlotHeaderSize)
	WriteSlotHeader(buf, 1, 100)
	buf[0] = 0xFF // corrupt magic
	_, err := ReadSlotHeader(buf)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDoorbellRoundtrip(t *testing.T) {
	buf := make([]byte, 4*DoorbellEntrySize)
	WriteDoorbellEntry(buf, 2, 99, 7)
	seq, idx := ReadDoorbellEntry(buf, 2)
	if seq != 99 || idx != 7 {
		t.Errorf("doorbell roundtrip: got seq=%d idx=%d, want seq=99 idx=7", seq, idx)
	}
}

func TestSerializeDeserializePacket_Basic(t *testing.T) {
	p := proto.NewPacket()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.PartitionID = 12345
	p.ExtentID = 67890
	p.ExtentOffset = 4096
	p.ReqID = 999
	p.Size = 8
	p.Data = []byte("helloRDM")

	slotSize := 4 * 1024 * 1024 // 4MB slot
	slot := make([]byte, slotSize)
	n, err := SerializePacket(slot, p)
	if err != nil {
		t.Fatal(err)
	}
	// Stamp actual seq
	WriteSlotHeader(slot, 1, uint32(n))

	got, err := DeserializePacket(slot)
	if err != nil {
		t.Fatal(err)
	}
	if got.PartitionID != p.PartitionID {
		t.Errorf("PartitionID: got %d want %d", got.PartitionID, p.PartitionID)
	}
	if got.ExtentID != p.ExtentID {
		t.Errorf("ExtentID: got %d want %d", got.ExtentID, p.ExtentID)
	}
	if got.ExtentOffset != p.ExtentOffset {
		t.Errorf("ExtentOffset: got %d want %d", got.ExtentOffset, p.ExtentOffset)
	}
	if got.ReqID != p.ReqID {
		t.Errorf("ReqID: got %d want %d", got.ReqID, p.ReqID)
	}
	if string(got.Data) != string(p.Data) {
		t.Errorf("Data: got %q want %q", got.Data, p.Data)
	}
}

func TestSerializeDeserializePacket_WithArg(t *testing.T) {
	p := proto.NewPacket()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpCreateExtent
	p.PartitionID = 1
	p.ExtentID = 2
	p.Arg = []byte("argument-bytes")
	p.ArgLen = uint32(len(p.Arg))
	p.Data = []byte("data-payload")
	p.Size = uint32(len(p.Data))

	slot := make([]byte, 1*1024*1024)
	n, err := SerializePacket(slot, p)
	if err != nil {
		t.Fatal(err)
	}
	WriteSlotHeader(slot, 5, uint32(n))

	got, err := DeserializePacket(slot)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Arg) != string(p.Arg) {
		t.Errorf("Arg: got %q want %q", got.Arg, p.Arg)
	}
	if string(got.Data) != string(p.Data) {
		t.Errorf("Data: got %q want %q", got.Data, p.Data)
	}
}

func TestSerializePacketTooLarge(t *testing.T) {
	p := proto.NewPacket()
	p.Magic = proto.ProtoMagic
	p.Size = 1024
	p.Data = make([]byte, 1024)

	smallSlot := make([]byte, util.PacketHeaderSize+SlotHeaderSize) // too small for data
	_, err := SerializePacket(smallSlot, p)
	if err == nil {
		t.Fatal("expected error for oversized packet")
	}
}
