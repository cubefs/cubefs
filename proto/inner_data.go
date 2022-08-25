package proto

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	BaseInnerDataLen = 12
)

type InnerDataSt struct {
	FileOffset uint64
	Size       uint32
	CRC		   uint32
	Data       []byte
}

func (d *InnerDataSt) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("Offset(%v) Size(%v) CRC(%v)", d.FileOffset, d.Size, d.CRC)
}

func (d *InnerDataSt) ValueLen() int {
	return len(d.Data) + BaseInnerDataLen
}

func (d *InnerDataSt) EncodeBinary(data []byte) {
	binary.BigEndian.PutUint64(data[0:8], d.FileOffset)
	binary.BigEndian.PutUint32(data[8:12], d.Size)
	if d.Size == 0 {
		return
	}
	copy(data[12:], d.Data)
}

func (d *InnerDataSt) MarshalBinary() ([]byte, error) {
	innerDataLen := len(d.Data) + BaseInnerDataLen
	data := make([]byte, innerDataLen)
	binary.BigEndian.PutUint64(data[0:8], d.FileOffset)
	binary.BigEndian.PutUint32(data[8:12], d.Size)
	if len(d.Data) != int(d.Size) {
		return nil, fmt.Errorf("data length[%v] is mismatch with size[%v]", len(d.Data), d.Size)
	}
	copy(data[12:], d.Data)
	return data, nil
}

func (d *InnerDataSt) UnmarshalBinary(data []byte) error {
	if len(data) < BaseInnerDataLen {
		return fmt.Errorf("inner data length at least 12, but buff len:%v", len(data))
	}
	d.FileOffset = binary.BigEndian.Uint64(data[0:8])
	d.Size = binary.BigEndian.Uint32(data[8:12])
	if d.Size == 0 {
		return nil
	}
	if len(data[12:]) < int(d.Size) {
		return fmt.Errorf("inner data length at least %v, but buff len:%v", d.Size, len(data[12:]))
	}
	d.Data = append(d.Data, data[12 : 12 + d.Size]...)
	return nil
}

func (d *InnerDataSt) UnmarshalBinaryBuff(buf *bytes.Buffer) (err error) {
	if err = binary.Read(buf, binary.BigEndian, &d.FileOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &d.Size); err != nil {
		return
	}
	d.Data = make([]byte, d.Size)
	if err = binary.Read(buf, binary.BigEndian, &d.Data); err != nil {
		return
	}
	return
}
