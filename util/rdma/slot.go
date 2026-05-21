//go:build linux && rdma

package rdma

import (
	"encoding/binary"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

const (
	SlotHeaderSize    = 16
	DoorbellEntrySize = 8
	slotMagic         = uint32(0x52444D41) // "RDMA"

	// MaxPacketHeaderSize is the largest possible serialized packet header.
	// Base (57) + VerSeq (8) + ProtoVersion (4) = 69 bytes.
	MaxPacketHeaderSize = util.PacketHeaderSize + util.PacketVerSeqFiledLen + util.PacketProtoVerFiledLen
)

// SlotHeader is the first SlotHeaderSize bytes of every ring buffer slot.
// The receiver polls Seq to detect new arrivals (Seq increases monotonically per slot).
type SlotHeader struct {
	Magic    uint32 // must equal slotMagic
	Seq      uint32 // monotonically increasing; receiver compares against last known value
	TotalLen uint32 // total bytes written: SlotHeader + packet header + Arg + Data
	Reserved uint32 // zero
}

// WriteSlotHeader serializes hdr into buf[0:SlotHeaderSize] using little-endian.
func WriteSlotHeader(buf []byte, seq uint32, totalLen uint32) {
	binary.LittleEndian.PutUint32(buf[0:4], slotMagic)
	binary.LittleEndian.PutUint32(buf[4:8], seq)
	binary.LittleEndian.PutUint32(buf[8:12], totalLen)
	binary.LittleEndian.PutUint32(buf[12:16], 0)
}

// ReadSlotHeader deserializes and validates the slot header from buf.
func ReadSlotHeader(buf []byte) (SlotHeader, error) {
	if len(buf) < SlotHeaderSize {
		return SlotHeader{}, fmt.Errorf("rdma: slot buf too short: %d", len(buf))
	}
	h := SlotHeader{
		Magic:    binary.LittleEndian.Uint32(buf[0:4]),
		Seq:      binary.LittleEndian.Uint32(buf[4:8]),
		TotalLen: binary.LittleEndian.Uint32(buf[8:12]),
		Reserved: binary.LittleEndian.Uint32(buf[12:16]),
	}
	if h.Magic != slotMagic {
		return SlotHeader{}, fmt.Errorf("rdma: bad slot magic 0x%08x", h.Magic)
	}
	return h, nil
}

// WriteDoorbellEntry writes {seq, slotIdx} as a single 8-byte entry at position
// idx in the doorbell array pointed to by buf.
// Layout: [0:4] = seq, [4:8] = slotIdx (little-endian).
func WriteDoorbellEntry(buf []byte, idx int, seq, slotIdx uint32) {
	off := idx * DoorbellEntrySize
	binary.LittleEndian.PutUint32(buf[off:off+4], seq)
	binary.LittleEndian.PutUint32(buf[off+4:off+8], slotIdx)
}

// ReadDoorbellEntry reads the seq and slotIdx from doorbell entry at idx.
func ReadDoorbellEntry(buf []byte, idx int) (seq, slotIdx uint32) {
	off := idx * DoorbellEntrySize
	return binary.LittleEndian.Uint32(buf[off : off+4]),
		binary.LittleEndian.Uint32(buf[off+4 : off+8])
}

// SerializePacket serializes p into slot, writing SlotHeader then the full packet
// (header bytes + Arg + Data). slot must be large enough.
// Returns total bytes written (== SlotHeader.TotalLen).
func SerializePacket(slot []byte, p *proto.Packet) (int, error) {
	// Determine header size: base + optional version fields
	hdrSize := util.PacketHeaderSize
	if p.ExtentType&proto.PacketProtocolVersionFlag > 0 {
		hdrSize += util.PacketVerSeqFiledLen + util.PacketProtoVerFiledLen
	}

	argLen := int(p.ArgLen)
	dataLen := int(p.Size)
	totalLen := SlotHeaderSize + hdrSize + argLen + dataLen

	if totalLen > len(slot) {
		return 0, fmt.Errorf("rdma: packet too large for slot: need %d, have %d", totalLen, len(slot))
	}

	// Write slot header placeholder (TotalLen filled after we know it)
	WriteSlotHeader(slot, 0, uint32(totalLen)) // Seq is set by caller via WriteSlotHeader before RDMA Write

	// Write packet header
	hdrBuf := slot[SlotHeaderSize : SlotHeaderSize+hdrSize]
	p.MarshalHeader(hdrBuf)

	// Write Arg
	off := SlotHeaderSize + hdrSize
	if argLen > 0 {
		copy(slot[off:off+argLen], p.Arg)
		off += argLen
	}

	// Write Data
	if dataLen > 0 {
		copy(slot[off:off+dataLen], p.Data)
	}

	return totalLen, nil
}

// DeserializePacket reconstructs a proto.Packet from a slot buffer.
// The slot must start with a valid SlotHeader.
func DeserializePacket(slot []byte) (*proto.Packet, error) {
	hdr, err := ReadSlotHeader(slot)
	if err != nil {
		return nil, err
	}
	if int(hdr.TotalLen) > len(slot) {
		return nil, fmt.Errorf("rdma: TotalLen %d exceeds slot %d", hdr.TotalLen, len(slot))
	}

	payload := slot[SlotHeaderSize:hdr.TotalLen]
	if len(payload) < util.PacketHeaderSize {
		return nil, fmt.Errorf("rdma: payload too short for packet header: %d", len(payload))
	}

	p := proto.NewPacket()
	if err = p.UnmarshalHeader(payload[:util.PacketHeaderSize]); err != nil {
		return nil, fmt.Errorf("rdma: UnmarshalHeader: %w", err)
	}

	// Handle optional version fields (same logic as TryReadExtraFieldsFromConn)
	off := util.PacketHeaderSize
	if p.ExtentType&proto.PacketProtocolVersionFlag > 0 {
		if len(payload) < off+util.PacketVerSeqFiledLen+util.PacketProtoVerFiledLen {
			return nil, fmt.Errorf("rdma: payload too short for version fields")
		}
		p.VerSeq = binary.BigEndian.Uint64(payload[off : off+8])
		off += util.PacketVerSeqFiledLen
		p.ProtoVersion = binary.BigEndian.Uint32(payload[off : off+4])
		off += util.PacketProtoVerFiledLen
	}

	argLen := int(p.ArgLen)
	dataLen := int(p.Size)

	if len(payload) < off+argLen+dataLen {
		return nil, fmt.Errorf("rdma: payload too short for Arg+Data: have %d need %d",
			len(payload), off+argLen+dataLen)
	}

	if argLen > 0 {
		p.Arg = make([]byte, argLen)
		copy(p.Arg, payload[off:off+argLen])
		off += argLen
	}
	if dataLen > 0 {
		p.Data = make([]byte, dataLen)
		copy(p.Data, payload[off:off+dataLen])
	}

	return p, nil
}
