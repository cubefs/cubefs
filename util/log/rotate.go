// Copyright 2018 The Cubefs Authors.
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
	// DefaultRollingSize Specifies at what size to roll the output log at
	// Units: byte
	DefaultRollingSize    = 5 * 1024 * 1024 * 1024
	DefaultMinRollingSize = 200 * 1024 * 1024
	// DefaultHeadRoom The tolerance for the log space limit (in megabytes)
	DefaultHeadRoom = 50 * 1024
	// DefaultHeadRatio The disk reserve space ratio
	DefaultHeadRatio = 0.2
)

// A log can be rotated by the size or time.
type LogRotate struct {
	rollingSize int64 // the size of the rotated log // TODO we should either call rotate or rolling, but not both.
	headRoom    int64 // capacity reserved for writing the next log on the disk
}

// NewLogRotate returns a new LogRotate instance.
func NewLogRotate() *LogRotate {
	return &LogRotate{
		rollingSize: DefaultRollingSize,
		headRoom:    DefaultHeadRoom,
	}
}

// SetRollingSizeMb sets the rolling size in terms of MB.
func (r *LogRotate) SetRollingSizeMb(size int64) {
	r.rollingSize = size
}

// SetHeadRoomMb sets the headroom in terms of MB.
func (r *LogRotate) SetHeadRoomMb(size int64) {
	r.headRoom = size
}
