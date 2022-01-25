package crc32block

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	defaultCrc32BlockSize = 64 * 1024
)

type blockUnit []byte

func (b blockUnit) length() int {
	return len(b)
}

func (b blockUnit) payload() int {
	return len(b) - crc32Len
}

func (b blockUnit) check() (err error) {
	payloadCrc := crc32.ChecksumIEEE(b[crc32Len:])
	if binary.LittleEndian.Uint32(b) != payloadCrc {
		return ErrMismatchedCrc
	}
	return nil
}

func (b blockUnit) writeCrc() {
	payloadCrc := crc32.ChecksumIEEE(b[crc32Len:])
	binary.LittleEndian.PutUint32(b, payloadCrc)
}
