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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		name      string
		size      int64
		precision uint8
		expected  string
	}{
		// Bytes
		{"zero", 0, 2, "0.00 B"},
		{"bytes", 999, 2, "999.00 B"},
		{"negative bytes", -500, 2, "-500.00 B"},

		// KB (1000)
		{"1 KB", 1000, 2, "1.00 KB"},
		{"1.5 KB", 1500, 2, "1.50 KB"},
		{"999 KB", 999999, 2, "1000.00 KB"},

		// MB (1000^2)
		{"1 MB", 1000 * 1000, 2, "1.00 MB"},
		{"1.5 MB", 1500 * 1000, 2, "1.50 MB"},
		{"999 MB", 999999999, 2, "1000.00 MB"},

		// GB (1000^3)
		{"1 GB", 1000 * 1000 * 1000, 2, "1.00 GB"},
		{"1.5 GB", 1500 * 1000 * 1000, 2, "1.50 GB"},

		// TB (1000^4)
		{"1 TB", 1000 * 1000 * 1000 * 1000, 2, "1.00 TB"},
		{"1.5 TB", 1500 * 1000 * 1000 * 1000, 2, "1.50 TB"},

		// PB (1000^5)
		{"1 PB", 1000 * 1000 * 1000 * 1000 * 1000, 2, "1.00 PB"},
		{"1.5 PB", 1500 * 1000 * 1000 * 1000 * 1000, 2, "1.50 PB"},
		{"10 PB", 10 * 1000 * 1000 * 1000 * 1000 * 1000, 2, "10.00 PB"},

		// precision
		{"precision 0", 1500, 0, "2 KB"},
		{"precision 1", 1500, 1, "1.5 KB"},
		{"precision 3", 1500, 3, "1.500 KB"},

		// negative values
		{"negative KB", -1500, 2, "-1.50 KB"},
		{"negative MB", -1500 * 1000, 2, "-1.50 MB"},
		{"negative GB", -1500 * 1000 * 1000, 2, "-1.50 GB"},
		{"negative TB", -1500 * 1000 * 1000 * 1000, 2, "-1.50 TB"},
		{"negative PB", -1500 * 1000 * 1000 * 1000 * 1000, 2, "-1.50 PB"},
	}

	for _, tt := range tests {
		result := HumanBytes(tt.size, tt.precision)
		require.Equal(t, tt.expected, result, tt.name)
	}
}

func TestHumanBytesGenericTypes(t *testing.T) {
	// Test different integer types
	require.Equal(t, "1.00 KB", HumanBytes(int(1000), 2))
	require.Equal(t, "100.00 B", HumanBytes(int8(100), 2))
	require.Equal(t, "1.00 KB", HumanBytes(int16(1000), 2))
	require.Equal(t, "1.00 KB", HumanBytes(int32(1000), 2))
	require.Equal(t, "1.00 KB", HumanBytes(int64(1000), 2))

	require.Equal(t, "1.00 KB", HumanBytes(uint(1000), 2))
	require.Equal(t, "100.00 B", HumanBytes(uint8(100), 2))
	require.Equal(t, "1.00 KB", HumanBytes(uint16(1000), 2))
	require.Equal(t, "1.00 KB", HumanBytes(uint32(1000), 2))
	require.Equal(t, "1.00 KB", HumanBytes(uint64(1000), 2))

	// Test large uint64 value (> int64 max)
	largeUint64 := uint64(10) * 1000 * 1000 * 1000 * 1000 * 1000
	require.Equal(t, "10.00 PB", HumanBytes(largeUint64, 2))
}

func TestHumanIBytes(t *testing.T) {
	tests := []struct {
		name      string
		size      int64
		precision uint8
		expected  string
	}{
		// Bytes
		{"zero", 0, 2, "0.00 B"},
		{"bytes", 1023, 2, "1023.00 B"},
		{"negative bytes", -500, 2, "-500.00 B"},

		// KiB (1024)
		{"1 KiB", 1024, 2, "1.00 KiB"},
		{"1.5 KiB", 1536, 2, "1.50 KiB"},
		{"1023 KiB", 1024*1024 - 1, 2, "1024.00 KiB"},

		// MiB (1024^2)
		{"1 MiB", 1024 * 1024, 2, "1.00 MiB"},
		{"1.5 MiB", 1536 * 1024, 2, "1.50 MiB"},

		// GiB (1024^3)
		{"1 GiB", 1024 * 1024 * 1024, 2, "1.00 GiB"},
		{"1.5 GiB", 1536 * 1024 * 1024, 2, "1.50 GiB"},

		// TiB (1024^4)
		{"1 TiB", 1024 * 1024 * 1024 * 1024, 2, "1.00 TiB"},
		{"1.5 TiB", 1536 * 1024 * 1024 * 1024, 2, "1.50 TiB"},

		// PiB (1024^5)
		{"1 PiB", 1024 * 1024 * 1024 * 1024 * 1024, 2, "1.00 PiB"},
		{"1.5 PiB", 1536 * 1024 * 1024 * 1024 * 1024, 2, "1.50 PiB"},
		{"8 PiB", 8 * 1024 * 1024 * 1024 * 1024 * 1024, 2, "8.00 PiB"},

		// precision
		{"precision 0", 1536, 0, "2 KiB"},
		{"precision 1", 1536, 1, "1.5 KiB"},
		{"precision 3", 1536, 3, "1.500 KiB"},

		// negative values
		{"negative KiB", -1536, 2, "-1.50 KiB"},
		{"negative MiB", -1536 * 1024, 2, "-1.50 MiB"},
		{"negative GiB", -1536 * 1024 * 1024, 2, "-1.50 GiB"},
		{"negative TiB", -1536 * 1024 * 1024 * 1024, 2, "-1.50 TiB"},
		{"negative PiB", -1536 * 1024 * 1024 * 1024 * 1024, 2, "-1.50 PiB"},
	}

	for _, tt := range tests {
		result := HumanIBytes(tt.size, tt.precision)
		require.Equal(t, tt.expected, result, tt.name)
	}
}

func TestHumanIBytesGenericTypes(t *testing.T) {
	// Test different integer types
	require.Equal(t, "1.00 KiB", HumanIBytes(int(1024), 2))
	require.Equal(t, "100.00 B", HumanIBytes(int8(100), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(int16(1024), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(int32(1024), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(int64(1024), 2))

	require.Equal(t, "1.00 KiB", HumanIBytes(uint(1024), 2))
	require.Equal(t, "100.00 B", HumanIBytes(uint8(100), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(uint16(1024), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(uint32(1024), 2))
	require.Equal(t, "1.00 KiB", HumanIBytes(uint64(1024), 2))

	// Test large uint64 value (> int64 max)
	largeUint64 := uint64(8) * 1024 * 1024 * 1024 * 1024 * 1024
	require.Equal(t, "8.00 PiB", HumanIBytes(largeUint64, 2))
}
