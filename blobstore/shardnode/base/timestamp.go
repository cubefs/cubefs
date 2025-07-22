// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"encoding/binary"
	"strconv"
	"time"
)

type Ts uint64

func NewTs(timeUnix int64) Ts {
	return Ts(timeUnix << 32)
}

func (ts Ts) TimeUnix() int64 {
	return int64(ts >> 32)
}

// Compare return -1 when ts less than target ts
// return 1 when ts large than target ts
// return 0 when ts equal to target ts
func (ts Ts) Compare(target Ts) int {
	if ts < target {
		return -1
	}
	if ts > target {
		return 1
	}
	return 0
}

func (ts Ts) Increment() uint32 {
	return uint32(ts)
}

func (ts Ts) String() string {
	return strconv.FormatUint(uint64(ts), 10)
	// return strconv.Itoa(int(int32(ts>>32))) + "-" + strconv.Itoa(int(int32(ts)))
}

func (ts Ts) Marshal() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ts))
	return b
}

func (ts *Ts) Unmarshal(raw []byte) {
	*ts = Ts(binary.BigEndian.Uint64(raw))
}

func (ts Ts) Add(dr time.Duration) Ts {
	return Ts(time.Unix(int64(ts>>32), 0).Add(dr).Unix() << 32)
}
