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

func Uvarint64(buf []byte) (uint64, int) {
	if buf[0] <= 0xF0 {
		return uint64(buf[0]), 1
	}
	if buf[0] <= 0xF8 {
		return 240 + 256*(uint64(buf[0])-241) + uint64(buf[1]), 2
	}
	if buf[0] == 0xF9 {
		return 2288 + 256*uint64(buf[1]) + uint64(buf[2]), 3
	}
	if buf[0] == 0xFA {
		return uint64(buf[1])<<16 | uint64(buf[2])<<8 | uint64(buf[3]), 4
	}
	if buf[0] == 0xFB {
		return uint64(buf[1])<<24 | uint64(buf[2])<<16 | uint64(buf[3])<<8 | uint64(buf[4]), 5
	}
	if buf[0] == 0xFC {
		return uint64(buf[1])<<32 | uint64(buf[2])<<24 | uint64(buf[3])<<16 | uint64(buf[4])<<8 | uint64(buf[5]), 6
	}
	if buf[0] == 0xFD {
		return uint64(buf[1])<<40 | uint64(buf[2])<<32 | uint64(buf[3])<<24 | uint64(buf[4])<<16 | uint64(buf[5])<<8 | uint64(buf[6]), 7
	}
	if buf[0] == 0xFE {
		return uint64(buf[1])<<48 | uint64(buf[2])<<40 | uint64(buf[3])<<32 | uint64(buf[4])<<24 | uint64(buf[5])<<16 | uint64(buf[6])<<8 | uint64(buf[7]), 8
	}
	return uint64(buf[1])<<56 | uint64(buf[2])<<48 | uint64(buf[3])<<40 | uint64(buf[4])<<32 | uint64(buf[5])<<24 | uint64(buf[6])<<16 | uint64(buf[7])<<8 | uint64(buf[8]), 9
}

func PutUvarint64(buf []byte, x uint64) int {
	if x < 241 {
		buf[0] = byte(x)
		return 1
	}
	if x < 2288 {
		buf[0] = byte((x-240)/256 + 241)
		buf[1] = byte((x - 240) % 256)
		return 2
	}
	if x < 67824 {
		buf[0] = 0xF9
		buf[1] = byte((x - 2288) / 256)
		buf[2] = byte((x - 2288) % 256)
		return 3
	}
	if x < 1<<24 {
		buf[0] = 0xFA
		buf[1] = byte(x >> 16)
		buf[2] = byte(x >> 8)
		buf[3] = byte(x)
		return 4
	}
	if x < 1<<32 {
		buf[0] = 0xFB
		buf[1] = byte(x >> 24)
		buf[2] = byte(x >> 16)
		buf[3] = byte(x >> 8)
		buf[4] = byte(x)
		return 5
	}
	if x < 1<<40 {
		buf[0] = 0xFC
		buf[1] = byte(x >> 32)
		buf[2] = byte(x >> 24)
		buf[3] = byte(x >> 16)
		buf[4] = byte(x >> 8)
		buf[5] = byte(x)
		return 6
	}
	if x < 1<<48 {
		buf[0] = 0xFD
		buf[1] = byte(x >> 40)
		buf[2] = byte(x >> 32)
		buf[3] = byte(x >> 24)
		buf[4] = byte(x >> 16)
		buf[5] = byte(x >> 8)
		buf[6] = byte(x)
		return 7
	}
	if x < 1<<56 {
		buf[0] = 0xFE
		buf[1] = byte(x >> 48)
		buf[2] = byte(x >> 40)
		buf[3] = byte(x >> 32)
		buf[4] = byte(x >> 24)
		buf[5] = byte(x >> 16)
		buf[6] = byte(x >> 8)
		buf[7] = byte(x)
		return 8
	}
	buf[0] = 0xFF
	buf[1] = byte(x >> 56)
	buf[2] = byte(x >> 48)
	buf[3] = byte(x >> 40)
	buf[4] = byte(x >> 32)
	buf[5] = byte(x >> 24)
	buf[6] = byte(x >> 16)
	buf[7] = byte(x >> 8)
	buf[8] = byte(x)
	return 9
}
