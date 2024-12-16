// Copyright 2024 The CubeFS Authors.
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

package uring

import (
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestUserdata(t *testing.T) {
	type test struct {
		v   any
		exp uint64
	}
	ts := []test{
		{uint64(0), 0},
		{uint64(0xff), 0xff},
		{uint64(0xfffefd), 0xfffefd},
		{uintptr(0xcafeba), 0xcafeba},
		{unsafe.Pointer(nil), 0},
	}
	bo := binary.LittleEndian
	for _, tc := range ts {
		var u UserData
		switch v := tc.v.(type) {
		case uint64:
			u.SetUint64(v)
		case uintptr:
			u.SetUintptr(v)
		case unsafe.Pointer:
			u.SetUnsafe(v)
		default:
			panic(fmt.Sprintf("unhandled type: %T", v))
		}

		assert.Equal(t, tc.exp, u.GetUint64())

		var exp [8]byte
		bo.PutUint64(exp[:], tc.exp)
		assert.Equal(t, exp[:], u[:])
	}
}
