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

package base

// volume task type
type VolumeTaskType uint8

const (
	VolumeTaskTypeLock VolumeTaskType = VolumeTaskType(iota + 1)
	VolumeTaskTypeUnlock
)

func (t VolumeTaskType) String() string {
	switch t {
	case VolumeTaskTypeLock:
		return "lock"
	case VolumeTaskTypeUnlock:
		return "unlock"
	}
	return "unknown"
}
