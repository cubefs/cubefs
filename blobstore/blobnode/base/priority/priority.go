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

package priority

import (
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
)

type Priority int

const (
	level0   Priority = iota // high, need low latency
	level1                   // normal
	level2                   // low
	level3                   // least
	levelNum                 // numbers of priority
)

var levelNames = [...]string{
	"level0",
	"level1",
	"level2",
	"level3",
}

var _ = levelNames[levelNum-1]

var levelPriorityTable = map[blobnode.IOType]Priority{
	blobnode.NormalIO:   level0,
	blobnode.RepairIO:   level1,
	blobnode.DeleteIO:   level1,
	blobnode.MigrateIO:  level2,
	blobnode.CompactIO:  level2,
	blobnode.InternalIO: level3,
	blobnode.InspectIO:  level3,
}

func (pri Priority) String() string {
	return levelNames[pri]
}

func IsValidPriName(name string) bool {
	for _, n := range levelNames {
		if name == n {
			return true
		}
	}
	return false
}

func GetPriority(ioType blobnode.IOType) Priority {
	if pri, ok := levelPriorityTable[ioType]; ok {
		return pri
	}
	return level3 // lowest
}

func GetLevels() []string {
	return levelNames[:]
}
