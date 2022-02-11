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

package proto

import (
	"errors"
	"strconv"
)

type (
	Vuid       uint64
	VuidPrefix uint64
)

const (
	MaxEpoch    = 16777215
	MinEpoch    = 1
	InvalidVuid = Vuid(0)
	MinIndex    = 0
	MaxIndex    = 255
)

func (vu Vuid) IsValid() bool {
	return vu > InvalidVuid && IsValidEpoch(vu.Epoch()) && IsValidIndex(vu.Index())
}

func NewVuid(vid Vid, idx uint8, epoch uint32) (Vuid, error) {
	if !IsValidEpoch(epoch) {
		err := errors.New("fail to new vuid,Epoch is overflow")
		return 0, err
	}

	u64 := uint64(vid)<<32 + uint64(idx)<<24 + uint64(epoch)
	return Vuid(u64), nil
}

func EncodeVuidPrefix(vid Vid, idx uint8) VuidPrefix {
	u64 := uint64(vid)<<32 + uint64(idx)<<24
	return VuidPrefix(u64)
}

func EncodeVuid(v VuidPrefix, epoch uint32) Vuid {
	u64 := uint64(v) + uint64(epoch)
	return Vuid(u64)
}

func (v Vuid) Vid() Vid {
	return Vid(v & 0xffffffff00000000 >> 32)
}

func (v Vuid) ToString() string {
	return strconv.FormatUint(uint64(v), 10)
}

func (v Vuid) Index() uint8 {
	return uint8(v & 0xff000000 >> 24)
}

func (v Vuid) Epoch() uint32 {
	return uint32(v & 0xffffff)
}

func (v Vuid) VuidPrefix() VuidPrefix {
	vuidPre := uint64(v) - uint64(v.Epoch())
	return VuidPrefix(vuidPre)
}

func (v VuidPrefix) Vid() Vid {
	return Vid(v & 0xffffffff00000000 >> 32)
}

func (v VuidPrefix) Index() uint8 {
	return uint8(v & 0xff000000 >> 24)
}

func IsValidEpoch(epoch uint32) bool {
	return epoch <= MaxEpoch && epoch >= MinEpoch
}

func IsValidIndex(index uint8) bool {
	return index <= MaxIndex && index >= MinIndex
}
