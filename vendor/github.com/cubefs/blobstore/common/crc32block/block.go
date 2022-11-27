// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package crc32block

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	defaultCrc32BlockSize = 64 * 1024
)

var gBlockSize int64 = defaultCrc32BlockSize

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
