// Copyright 2026 The CubeFS Authors.
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

package util

import (
	"fmt"
	"math"

	"github.com/dustin/go-humanize"
)

// IEC Sizes.
const (
	Byte = 1 << (iota * 10)
	KiByte
	MiByte
	GiByte
	TiByte
	PiByte
	EiByte
)

// SI Sizes.
const (
	IByte = 1
	KByte = IByte * 1000
	MByte = KByte * 1000
	GByte = MByte * 1000
	TByte = GByte * 1000
	PByte = TByte * 1000
	EByte = PByte * 1000
)

var ParseBytes = humanize.ParseBytes

func HumanBytes[I Integer](size I, precision uint8) string {
	f64Size := float64(size)
	absSize := math.Abs(f64Size)
	format := fmt.Sprintf("%%.%df %%s", precision)
	switch {
	case absSize < KByte:
		return fmt.Sprintf(format, f64Size, "B")
	case absSize < MByte:
		return fmt.Sprintf(format, f64Size/KByte, "KB")
	case absSize < GByte:
		return fmt.Sprintf(format, f64Size/MByte, "MB")
	case absSize < TByte:
		return fmt.Sprintf(format, f64Size/GByte, "GB")
	case absSize < PByte:
		return fmt.Sprintf(format, f64Size/TByte, "TB")
	default:
		return fmt.Sprintf(format, f64Size/PByte, "PB")
	}
}

func HumanIBytes[I Integer](size I, precision uint8) string {
	f64Size := float64(size)
	absSize := math.Abs(f64Size)
	format := fmt.Sprintf("%%.%df %%s", precision)
	switch {
	case absSize < KiByte:
		return fmt.Sprintf(format, f64Size, "B")
	case absSize < MiByte:
		return fmt.Sprintf(format, f64Size/KiByte, "KiB")
	case absSize < GiByte:
		return fmt.Sprintf(format, f64Size/MiByte, "MiB")
	case absSize < TiByte:
		return fmt.Sprintf(format, f64Size/GiByte, "GiB")
	case absSize < PiByte:
		return fmt.Sprintf(format, f64Size/TiByte, "TiB")
	default:
		return fmt.Sprintf(format, f64Size/PiByte, "PiB")
	}
}
