// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"hash/crc32"
)

var table = crc32.MakeTable(crc32.Castagnoli)

// CRC is a CRC-32 checksum computed using Castagnoli's polynomial.
type CRC uint32

// NewCRC creates a new crc based on the given bytes.
func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

// Update updates the crc with the given bytes.
func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

// Value returns a masked crc.
func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}
