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
	"testing"
	"time"
)

func TestNewTs(t *testing.T) {
	tests := []struct {
		name     string
		timeUnix int64
		expected Ts
	}{
		{
			name:     "zero timestamp",
			timeUnix: 0,
			expected: Ts(0),
		},
		{
			name:     "positive timestamp",
			timeUnix: 1640995200, // 2022-01-01 00:00:00 UTC
			expected: Ts(1640995200 << 32),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewTs(tt.timeUnix)
			if result != tt.expected {
				t.Errorf("NewTs(%d) = %v, want %v", tt.timeUnix, result, tt.expected)
			}
		})
	}
}

func TestTs_TimeUnix(t *testing.T) {
	tests := []struct {
		name     string
		ts       Ts
		expected int64
	}{
		{
			name:     "zero timestamp",
			ts:       Ts(0),
			expected: 0,
		},
		{
			name:     "positive timestamp",
			ts:       Ts(1640995200 << 32),
			expected: 1640995200,
		},
		{
			name:     "timestamp with increment",
			ts:       Ts((1640995200 << 32) | 12345),
			expected: 1640995200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts.TimeUnix()
			if result != tt.expected {
				t.Errorf("ts.TimeUnix() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestTs_Compare(t *testing.T) {
	tests := []struct {
		name     string
		ts       Ts
		target   Ts
		expected int
	}{
		{
			name:     "equal timestamps",
			ts:       Ts(1640995200 << 32),
			target:   Ts(1640995200 << 32),
			expected: 0,
		},
		{
			name:     "ts less than target",
			ts:       Ts(1640995200 << 32),
			target:   Ts(1640995260 << 32),
			expected: -1,
		},
		{
			name:     "ts greater than target",
			ts:       Ts(1640995260 << 32),
			target:   Ts(1640995200 << 32),
			expected: 1,
		},
		{
			name:     "same time different increment",
			ts:       Ts((1640995200 << 32) | 100),
			target:   Ts((1640995200 << 32) | 200),
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts.Compare(tt.target)
			if result != tt.expected {
				t.Errorf("ts.Compare(%v) = %d, want %d", tt.target, result, tt.expected)
			}
		})
	}
}

func TestTs_Increment(t *testing.T) {
	tests := []struct {
		name     string
		ts       Ts
		expected uint32
	}{
		{
			name:     "zero increment",
			ts:       Ts(1640995200 << 32),
			expected: 0,
		},
		{
			name:     "positive increment",
			ts:       Ts((1640995200 << 32) | 12345),
			expected: 12345,
		},
		{
			name:     "max increment",
			ts:       Ts((1640995200 << 32) | 0xFFFFFFFF),
			expected: 0xFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts.Increment()
			if result != tt.expected {
				t.Errorf("ts.Increment() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestTs_Add(t *testing.T) {
	baseTime := int64(1640995200) // 2022-01-01 00:00:00 UTC
	baseTs := Ts(baseTime << 32)

	tests := []struct {
		name     string
		ts       Ts
		duration time.Duration
		expected Ts
	}{
		{
			name:     "add zero duration",
			ts:       baseTs,
			duration: 0,
			expected: baseTs,
		},
		{
			name:     "add positive duration",
			ts:       baseTs,
			duration: time.Hour,
			expected: Ts((baseTime + 3600) << 32),
		},
		{
			name:     "add negative duration",
			ts:       baseTs,
			duration: -time.Hour,
			expected: Ts((baseTime - 3600) << 32),
		},
		{
			name:     "add one second",
			ts:       baseTs,
			duration: time.Second,
			expected: Ts((baseTime + 1) << 32),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts.Add(tt.duration)
			if result != tt.expected {
				t.Errorf("ts.Add(%v) = %v, want %v", tt.duration, result, tt.expected)
			}
		})
	}
}
