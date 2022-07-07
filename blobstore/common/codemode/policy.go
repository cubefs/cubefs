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

package codemode

// Policy will be used to adjust code mode's upload range or code mode's volume ratio and so on
type Policy struct {
	ModeName CodeModeName `json:"mode_name"`
	// min size of object, access use this to put object
	MinSize int64 `json:"min_size"`
	// max size of object. access use this to put object
	MaxSize int64 `json:"max_size"`
	// code mode's cluster space size ratio, clusterMgr should use this to create specified num of volume
	SizeRatio float64 `json:"size_ratio"`
	// enable means this kind of code mode enable or not
	// access/allocator will ignore this kind of code mode's allocation when enable is false
	// clustermgr will ignore this kind of code mode's creation when enable is false
	Enable bool `json:"enable"`
}
