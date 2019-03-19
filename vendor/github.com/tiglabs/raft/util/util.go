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
	"time"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

const time_format = "2006-01-02 15:04:05.000"

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func FormatDate(t time.Time) string {
	return t.Format(time_format)
}

func FormatTimestamp(t int64) string {
	if t <= 0 {
		return ""
	}
	return time.Unix(0, t).Format(time_format)
}
