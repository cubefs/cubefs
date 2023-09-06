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
	normal     Priority = iota // high, need low latency
	background                 // low
	levelNum                   // numbers of priority
)

var levelNames = [...]string{
	"normal",
	"background",
}

var _ = levelNames[levelNum-1]

var levelPriorityTable = map[blobnode.IOType]Priority{
	blobnode.NormalIO:     normal,
	blobnode.BackgroundIO: background,
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
	return background
}

func GetLevels() []string {
	return levelNames[:]
}
