// Copyright 2018 The Containerfs Authors.
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
	// Units: MB
	DefaultRollingSize = 10 * 1024
	// DefaultHeadRoom The tolerance for the log space limit (in megabytes)
	DefaultHeadRoom = 50 * 1024
	// DefaultHeadRatio The disk reserve space ratio
	DefaultHeadRatio = 0.2
)

type LogRotate struct {
	rollingSize int64
	headRoom    int64
}

func NewLogRotate() *LogRotate {
	return &LogRotate{
		rollingSize: DefaultRollingSize,
		headRoom:    DefaultHeadRoom,
	}
}

func (r *LogRotate) SetRollingSizeMb(size int64) {
	r.rollingSize = size
}

func (r *LogRotate) SetHeadRoomMb(size int64) {
	r.headRoom = size
}
