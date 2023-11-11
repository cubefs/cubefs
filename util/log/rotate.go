// Copyright 2018 The CubeFS Authors.
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

package log

const (
	// DefaultRotateSize Specifies at what size to rotate the output log at
	// Units: byte
	DefaultRotateSize    = 1 * 1024 * 1024 * 1024
	DefaultMinRotateSize = 200 * 1024 * 1024
	// DefaultHeadRoom The tolerance for the log space limit (in megabytes)
	DefaultHeadRoom = 50 * 1024
	// DefaultHeadRatio The disk reserve space ratio
	DefaultHeadRatio         = 0.2
	DefaultLogLeftSpaceLimit = 5 * 1024
)

// LogRotate A log can be rotated by the size or time.
type LogRotate struct {
	rotateSize int64 // the size of the rotated log
	headRoom   int64 // capacity reserved for writing the next log on the disk
}

// NewLogRotate returns a new LogRotate instance.
func NewLogRotate() *LogRotate {
	return &LogRotate{}
}

// SetRotateSizeMb sets the rotate size in terms of MB.
func (r *LogRotate) SetRotateSizeMb(size int64) {
	r.rotateSize = size
}

// SetHeadRoomMb sets the headroom in terms of MB.
func (r *LogRotate) SetHeadRoomMb(size int64) {
	r.headRoom = size
}
