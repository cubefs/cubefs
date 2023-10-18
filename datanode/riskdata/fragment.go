package riskdata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Fragment struct {
	ExtentID uint64
	Offset   uint64
	Size     uint64
}

func (f *Fragment) String() string {
	return fmt.Sprintf("Fragment[ExtentID=%v, Offset=%v, Size=%v]", f.ExtentID, f.Offset, f.Size)
}

func (f *Fragment) Overlap(extentID, offset, size uint64) bool {
	return extentID == f.ExtentID && offset+size > f.Offset && f.Offset+f.Size > offset
}

func (f *Fragment) Covered(extentID, offset, size uint64) bool {
	return extentID == f.ExtentID && offset <= f.Offset && offset+size >= f.Offset+f.Size
}

func (f *Fragment) Equals(o *Fragment) bool {
	return f.ExtentID == o.ExtentID && f.Offset == o.Offset && f.Size == o.Size
}

func (f *Fragment) EncodeTo(b []byte) (err error) {
	if len(b) < fragmentBinaryLength {
		return ErrIllegalFragmentLength
	}
	binary.BigEndian.PutUint64(b[:8], f.ExtentID)
	binary.BigEndian.PutUint64(b[8:16], f.Offset)
	binary.BigEndian.PutUint64(b[16:24], f.Size)
	binary.BigEndian.PutUint32(b[24:28], crc32.ChecksumIEEE(b[:24]))
	return nil
}

func (f *Fragment) EncodeLength() int {
	return fragmentBinaryLength
}

func (f *Fragment) DecodeFrom(b []byte) error {
	if len(b) < fragmentBinaryLength {
		return ErrIllegalFragmentLength
	}
	if crc32.ChecksumIEEE(b[:24]) != binary.BigEndian.Uint32(b[24:28]) {
		return ErrBrokenFragmentData
	}
	f.ExtentID = binary.BigEndian.Uint64(b[:8])
	f.Offset = binary.BigEndian.Uint64(b[8:16])
	f.Size = binary.BigEndian.Uint64(b[16:24])
	return nil
}

func (f *Fragment) Empty() bool {
	return f.ExtentID == 0 && f.Offset == 0 && f.Size == 0
}
