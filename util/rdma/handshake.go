//go:build linux && rdma

package rdma

import (
	"encoding/binary"
	"fmt"
)

// ConnectInfo is sent by the initiator (Client/Leader) to the acceptor
// via rdma_cm private_data at connection time. Max 56 bytes per rdma_cm spec.
// Layout uses little-endian, fixed-size fields to match C struct memory layout.
type ConnectInfo struct {
	RespRkey   uint32 // rkey for acceptor to write response slots
	_          uint32 // padding
	RespBaseVA uint64 // base VA of initiator's response ring
	RespDbRkey uint32 // rkey for acceptor to write response doorbell
	_          uint32 // padding
	RespDbVA   uint64 // base VA of initiator's response doorbell array
	NumSlots   uint32 // number of ring buffer slots
	SlotSize   uint32 // bytes per request slot
	CreditRkey uint32 // rkey for acceptor to write credit-return updates
	_          uint32 // padding
	CreditVA   uint64 // base VA of initiator's 8-byte credit-return cell
} // 56 bytes

// AcceptInfo is sent by the acceptor back to the initiator
// via rdma_cm accept private_data.
type AcceptInfo struct {
	ReqRkey    uint32 // rkey for initiator to write request slots
	_          uint32 // padding
	ReqBaseVA  uint64 // base VA of acceptor's request ring
	DbRkey     uint32 // rkey for initiator to write doorbell entries
	_          uint32 // padding
	DbVA       uint64 // base VA of acceptor's doorbell array
	NumSlots   uint32
	SlotSize   uint32
	CreditRkey uint32 // rkey for initiator to write credit-return updates
	_          uint32 // padding
	CreditVA   uint64 // base VA of acceptor's 8-byte credit-return cell
} // 56 bytes

const (
	connectInfoSize = 56
	acceptInfoSize  = 56
)

func MarshalConnectInfo(ci ConnectInfo) []byte {
	b := make([]byte, connectInfoSize)
	binary.LittleEndian.PutUint32(b[0:4], ci.RespRkey)
	// b[4:8] padding
	binary.LittleEndian.PutUint64(b[8:16], ci.RespBaseVA)
	binary.LittleEndian.PutUint32(b[16:20], ci.RespDbRkey)
	// b[20:24] padding
	binary.LittleEndian.PutUint64(b[24:32], ci.RespDbVA)
	binary.LittleEndian.PutUint32(b[32:36], ci.NumSlots)
	binary.LittleEndian.PutUint32(b[36:40], ci.SlotSize)
	binary.LittleEndian.PutUint32(b[40:44], ci.CreditRkey)
	// b[44:48] padding
	binary.LittleEndian.PutUint64(b[48:56], ci.CreditVA)
	return b
}

func UnmarshalConnectInfo(b []byte) (ConnectInfo, error) {
	if len(b) < connectInfoSize {
		return ConnectInfo{}, fmt.Errorf("rdma: ConnectInfo too short: %d < %d", len(b), connectInfoSize)
	}
	return ConnectInfo{
		RespRkey:   binary.LittleEndian.Uint32(b[0:4]),
		RespBaseVA: binary.LittleEndian.Uint64(b[8:16]),
		RespDbRkey: binary.LittleEndian.Uint32(b[16:20]),
		RespDbVA:   binary.LittleEndian.Uint64(b[24:32]),
		NumSlots:   binary.LittleEndian.Uint32(b[32:36]),
		SlotSize:   binary.LittleEndian.Uint32(b[36:40]),
		CreditRkey: binary.LittleEndian.Uint32(b[40:44]),
		CreditVA:   binary.LittleEndian.Uint64(b[48:56]),
	}, nil
}

func MarshalAcceptInfo(ai AcceptInfo) []byte {
	b := make([]byte, acceptInfoSize)
	binary.LittleEndian.PutUint32(b[0:4], ai.ReqRkey)
	// b[4:8] padding
	binary.LittleEndian.PutUint64(b[8:16], ai.ReqBaseVA)
	binary.LittleEndian.PutUint32(b[16:20], ai.DbRkey)
	// b[20:24] padding
	binary.LittleEndian.PutUint64(b[24:32], ai.DbVA)
	binary.LittleEndian.PutUint32(b[32:36], ai.NumSlots)
	binary.LittleEndian.PutUint32(b[36:40], ai.SlotSize)
	binary.LittleEndian.PutUint32(b[40:44], ai.CreditRkey)
	// b[44:48] padding
	binary.LittleEndian.PutUint64(b[48:56], ai.CreditVA)
	return b
}

func UnmarshalAcceptInfo(b []byte) (AcceptInfo, error) {
	if len(b) < acceptInfoSize {
		return AcceptInfo{}, fmt.Errorf("rdma: AcceptInfo too short: %d < %d", len(b), acceptInfoSize)
	}
	return AcceptInfo{
		ReqRkey:    binary.LittleEndian.Uint32(b[0:4]),
		ReqBaseVA:  binary.LittleEndian.Uint64(b[8:16]),
		DbRkey:     binary.LittleEndian.Uint32(b[16:20]),
		DbVA:       binary.LittleEndian.Uint64(b[24:32]),
		NumSlots:   binary.LittleEndian.Uint32(b[32:36]),
		SlotSize:   binary.LittleEndian.Uint32(b[36:40]),
		CreditRkey: binary.LittleEndian.Uint32(b[40:44]),
		CreditVA:   binary.LittleEndian.Uint64(b[48:56]),
	}, nil
}
