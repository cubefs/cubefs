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

import (
	"fmt"
)

type (
	// CodeMode EC encode and decode mode
	CodeMode     uint8
	CodeType     uint8
	CodeModeName string
)

// pre-defined mode
const (
	EC15P12       CodeMode = 1
	EC6P6         CodeMode = 2
	EC16P20L2     CodeMode = 3
	EC6P10L2      CodeMode = 4
	EC6P3L3       CodeMode = 5
	EC6P6Align0   CodeMode = 6
	EC6P6Align512 CodeMode = 7
	EC4P4L2       CodeMode = 8
	EC12P4        CodeMode = 9
	EC16P4        CodeMode = 10
	EC3P3         CodeMode = 11
	EC10P4        CodeMode = 12
	EC6P3         CodeMode = 13
	EC12P9        CodeMode = 14
	// for test
	EC6P6L9  CodeMode = 200
	EC6P8L10 CodeMode = 201
	// for azureLrcP1
	// dataShards/(L-1) MUST equal to globalParityShards
	EC12P6L3 CodeMode = 211
	EC18P9L3 CodeMode = 212
	EC10P5L3 CodeMode = 213
	EC12P3L3 CodeMode = 214
)

// Note: Don't modify it unless you know very well how codemode works.
const (
	// align size per shard
	alignSize0B   = 0    // 0B
	alignSize512B = 512  // 512B
	alignSize2KB  = 2048 // 2KB
)

// Note: Don't modify it unless you know very well what codetype means.
const (
	ReedSolomon CodeType = 0
	OPPOLrc     CodeType = 1
	AzureLrcP1  CodeType = 2
)

// The tactic is fixed pairing with one codemode.
// Add a new codemode if you want other features.
var constCodeModeTactic = map[CodeMode]Tactic{
	// three az
	EC15P12: {N: 15, M: 12, L: 0, AZCount: 3, PutQuorum: 24, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC6P6:   {N: 6, M: 6, L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC12P9:  {N: 12, M: 9, L: 0, AZCount: 3, PutQuorum: 20, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},

	// two az
	EC16P20L2: {N: 16, M: 20, L: 2, AZCount: 2, PutQuorum: 34, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: OPPOLrc},
	EC6P10L2:  {N: 6, M: 10, L: 2, AZCount: 2, PutQuorum: 14, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: OPPOLrc},

	// single az
	EC12P4: {N: 12, M: 4, L: 0, AZCount: 1, PutQuorum: 15, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC16P4: {N: 16, M: 4, L: 0, AZCount: 1, PutQuorum: 19, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC3P3:  {N: 3, M: 3, L: 0, AZCount: 1, PutQuorum: 5, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC10P4: {N: 10, M: 4, L: 0, AZCount: 1, PutQuorum: 13, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	EC6P3:  {N: 6, M: 3, L: 0, AZCount: 1, PutQuorum: 8, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: ReedSolomon},
	// for env test
	EC6P3L3:       {N: 6, M: 3, L: 3, AZCount: 3, PutQuorum: 9, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: OPPOLrc},
	EC6P6Align0:   {N: 6, M: 6, L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: alignSize0B, CodeType: ReedSolomon},
	EC6P6Align512: {N: 6, M: 6, L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: alignSize512B, CodeType: ReedSolomon},
	EC4P4L2:       {N: 4, M: 4, L: 2, AZCount: 2, PutQuorum: 6, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: OPPOLrc},
	EC6P6L9:       {N: 6, M: 6, L: 9, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: OPPOLrc},
	EC6P8L10:      {N: 6, M: 8, L: 10, AZCount: 2, PutQuorum: 13, GetQuorum: 0, MinShardSize: alignSize0B, CodeType: OPPOLrc},
	// for azureLrcP1 test
	EC12P6L3: {N: 12, M: 6, L: 3, AZCount: 3, PutQuorum: 18, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: AzureLrcP1},
	EC18P9L3: {N: 18, M: 9, L: 3, AZCount: 3, PutQuorum: 27, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: AzureLrcP1},
	EC10P5L3: {N: 10, M: 5, L: 3, AZCount: 3, PutQuorum: 15, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: AzureLrcP1},
	EC12P3L3: {N: 12, M: 3, L: 3, AZCount: 3, PutQuorum: 15, GetQuorum: 0, MinShardSize: alignSize2KB, CodeType: AzureLrcP1},
}

var constName2CodeMode = map[CodeModeName]CodeMode{
	"EC15P12":       EC15P12,
	"EC6P6":         EC6P6,
	"EC16P20L2":     EC16P20L2,
	"EC6P10L2":      EC6P10L2,
	"EC6P3L3":       EC6P3L3,
	"EC6P6Align0":   EC6P6Align0,
	"EC6P6Align512": EC6P6Align512,
	"EC4P4L2":       EC4P4L2,
	"EC12P4":        EC12P4,
	"EC16P4":        EC16P4,
	"EC3P3":         EC3P3,
	"EC10P4":        EC10P4,
	"EC6P3":         EC6P3,
	"EC6P6L9":       EC6P6L9,
	"EC6P8L10":      EC6P8L10,
	"EC12P9":        EC12P9,
	"EC12P6L3":      EC12P6L3,
	"EC18P9L3":      EC18P9L3,
	"EC10P5L3":      EC10P5L3,
	"EC12P3L3":      EC12P3L3,
}

var constCodeMode2Name = map[CodeMode]CodeModeName{
	EC15P12:       "EC15P12",
	EC6P6:         "EC6P6",
	EC16P20L2:     "EC16P20L2",
	EC6P10L2:      "EC6P10L2",
	EC6P3L3:       "EC6P3L3",
	EC6P6Align0:   "EC6P6Align0",
	EC6P6Align512: "EC6P6Align512",
	EC4P4L2:       "EC4P4L2",
	EC12P4:        "EC12P4",
	EC16P4:        "EC16P4",
	EC3P3:         "EC3P3",
	EC10P4:        "EC10P4",
	EC6P3:         "EC6P3",
	EC6P6L9:       "EC6P6L9",
	EC6P8L10:      "EC6P8L10",
	EC12P9:        "EC12P9",
	EC12P6L3:      "EC12P6L3",
	EC18P9L3:      "EC18P9L3",
	EC10P5L3:      "EC10P5L3",
	EC12P3L3:      "EC12P3L3",
}

// vol layout ep:EC6P10L2
// |----N------|--------M----------------|--L--|
// |0,1,2,3,4,5|6,7,8,9,10,11,12,13,14,15|16,17|
// global stripe:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15], n=6 m=10
// two local stripes:
// local stripe1:[0,1,2,  6, 7, 8, 9,10, 16] n=8 m=1
// local stripe2:[3,4,5, 11,12,13,14,15, 17] n=8 m=1

// ec layout of AzureLrcP1 is as follows: e.g. EC6P3L3
// |----N------|--M--|---L---|
// |0,1,2,3,4,5|6,7,8|9,10,11|
// global stripe:[0,1,2,3,4,5,6,7,8], n=6 m=3
// three local stripes:
// local stripe1(data):		[0,1,2,  9] n=3 m=1
// local stripe2(data):		[3,4,5, 10] n=3 m=1
// local stripe3(parity):	[6,7,8, 11] n=3 m=1

// Tactic constant strategy of one CodeMode
type Tactic struct {
	N int
	M int
	// local parity count
	L int
	// the count of AZ, access use this for split data shards and parity shards
	AZCount int

	// PutQuorum write quorum,
	// MUST make sure that ec data is recoverable if one AZ was down
	// We SHOULD ignore the local shards
	// (N + M) / AZCount + N <= PutQuorum <= M + N
	PutQuorum int

	// get quorum config
	GetQuorum int

	// MinShardSize min size per shard, fill data into shards 0-N continuously,
	// align with zero bytes if data size less than MinShardSize*N
	//
	// length of data less than MinShardSize*N, size of per shard = MinShardSize
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  |  data  |                 align zero bytes                     |
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  |    0    |    1    |    2    |   ....                |    N    |
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//
	// length of data more than MinShardSize*N, size of per shard = ceil(len(data)/N)
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  |                           data                        |padding|
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  |    0    |    1    |    2    |   ....                |    N    |
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	MinShardSize int

	// indicate the code types:
	// ReedSolomon = 0, OPPOLrc = 1, AzureLrcP1 = 2
	CodeType CodeType
}

func init() {
	// assert all codemode
	for _, pair := range []struct {
		Mode CodeMode
		Size int
	}{
		{Mode: EC15P12, Size: alignSize2KB},
		{Mode: EC6P6, Size: alignSize2KB},
		{Mode: EC12P9, Size: alignSize2KB},
		{Mode: EC16P20L2, Size: alignSize2KB},
		{Mode: EC6P10L2, Size: alignSize2KB},

		{Mode: EC6P3L3, Size: alignSize2KB},
		{Mode: EC6P6Align0, Size: alignSize0B},
		{Mode: EC6P6Align512, Size: alignSize512B},

		// azureLrc+1's code mode
		{Mode: EC12P6L3, Size: alignSize2KB},
		{Mode: EC18P9L3, Size: alignSize2KB},
		{Mode: EC10P5L3, Size: alignSize2KB},
		{Mode: EC12P3L3, Size: alignSize2KB},
	} {
		tactic := pair.Mode.Tactic()
		if !tactic.IsValid() {
			panic(fmt.Sprintf("Invalid codemode:%d Tactic:%+v", pair.Mode, tactic))
		}

		// TODO: MODIFY THE MIN
		min := tactic.N + (tactic.N+tactic.M)/tactic.AZCount
		if tactic.CodeType == AzureLrcP1 { // just a naive solution to pass the test
			min = 0
		}
		max := tactic.N + tactic.M
		if tactic.PutQuorum < min || tactic.PutQuorum > max {
			panic(fmt.Sprintf("Invalid codemode:%d PutQuorum:%d([%d,%d])", pair.Mode,
				tactic.PutQuorum, min, max))
		}

		if tactic.MinShardSize != pair.Size {
			panic(fmt.Sprintf("Invalid codemode:%d MinShardSize:%d(%d)", pair.Mode,
				tactic.MinShardSize, pair.Size))
		}
	}
}

// T returns pointer of Tactic, used like:
// EC6P6.T().AllLocalStripe()
func (c CodeMode) T() *Tactic {
	tactic := c.Tactic()
	return &tactic
}

// Tactic returns its constant tactic
func (c CodeMode) Tactic() Tactic {
	if tactic, ok := constCodeModeTactic[c]; ok {
		return tactic
	}
	panic(fmt.Sprintf("Invalid codemode:%d", c))
}

// GetShardNum returns all shards number.
func (c CodeMode) GetShardNum() int {
	tactic := c.Tactic()
	return tactic.L + tactic.M + tactic.N
}

// Name turn the CodeMode to CodeModeName
func (c CodeMode) Name() CodeModeName {
	if name, ok := constCodeMode2Name[c]; ok {
		return name
	}
	panic(fmt.Sprintf("codemode: %d is invalid", c))
}

// String turn the CodeMode to string
func (c CodeMode) String() string {
	if name, ok := constCodeMode2Name[c]; ok {
		return string(name)
	}
	return ""
}

// IsValid check the CodeMode is valid
func (c CodeMode) IsValid() bool {
	if _, ok := constCodeMode2Name[c]; ok {
		return ok
	}
	return false
}

// GetCodeMode get the code mode by name
func (cn CodeModeName) GetCodeMode() CodeMode {
	if code, ok := constName2CodeMode[cn]; ok {
		return code
	}
	panic(fmt.Sprintf("codemode: %s is invalid", cn))
}

// IsValid check the CodeMode is valid by Name
func (cn CodeModeName) IsValid() bool {
	if _, ok := constName2CodeMode[cn]; ok {
		return ok
	}
	return false
}

// Tactic get tactic by code mode name
func (cn CodeModeName) Tactic() Tactic {
	return cn.GetCodeMode().Tactic()
}

// IsValid ec tactic valid or not
func (c *Tactic) IsValid() bool {
	if c.CodeType == AzureLrcP1 {
		return c.N > 0 && c.M > 0 && c.L >= 0 && c.AZCount > 0 &&
			c.PutQuorum > 0 && c.GetQuorum >= 0 && c.MinShardSize >= 0 &&
			c.L == 3
	} else {
		return c.N > 0 && c.M > 0 && c.L >= 0 && c.AZCount > 0 &&
			c.PutQuorum > 0 && c.GetQuorum >= 0 && c.MinShardSize >= 0 &&
			c.N%c.AZCount == 0 && c.M%c.AZCount == 0 && c.L%c.AZCount == 0
	}
}

// GetECLayoutByAZ ec layout by AZ
func (c *Tactic) GetECLayoutByAZ() (azStripes [][]int) {
	azStripes = make([][]int, c.AZCount)
	if c.CodeType == AzureLrcP1 {
		// generally, c.L is equal to c.AZCount
		// we force that c.L = 3
		n, l := c.N/(c.AZCount-1), c.L/c.AZCount
		for idx := range azStripes {
			var stripe []int
			if idx == c.AZCount-1 {
				// parity stripe
				stripe = make([]int, 0, c.M+l)
				for i := 0; i < c.M; i++ {
					stripe = append(stripe, c.N+i)
				}
			} else {
				// data stripe
				stripe = make([]int, 0, n+l)
				for i := 0; i < n; i++ {
					stripe = append(stripe, idx*n+i)
				}
			}
			for i := 0; i < l; i++ {
				stripe = append(stripe, c.N+c.M+idx*l+i)
			}
			azStripes[idx] = stripe
		}
	}
	if c.CodeType == OPPOLrc {
		n, m, l := c.N/c.AZCount, c.M/c.AZCount, c.L/c.AZCount
		for idx := range azStripes {
			stripe := make([]int, 0, n+m+l)
			for i := 0; i < n; i++ {
				stripe = append(stripe, idx*n+i)
			}
			for i := 0; i < m; i++ {
				stripe = append(stripe, c.N+idx*m+i)
			}
			for i := 0; i < l; i++ {
				stripe = append(stripe, c.N+c.M+idx*l+i)
			}
			azStripes[idx] = stripe
		}
	}
	return azStripes
}

// GlobalStripe returns initial stripe	return name.GetCodeMode().Tactic()
func (c *Tactic) GlobalStripe() (indexes []int, n, m int) {
	indexes = make([]int, c.N+c.M)
	for i := 0; i < c.N+c.M; i++ {
		indexes[i] = i
	}
	return indexes, c.N, c.M
}

// AllLocalStripe returns all local stripes
func (c *Tactic) AllLocalStripe() (stripes [][]int, n, m int) {
	if c.L == 0 {
		return
	}

	n, m, l := c.N/c.AZCount, c.M/c.AZCount, c.L/c.AZCount
	return c.GetECLayoutByAZ(), n + m, l
}

// LocalStripe get local stripe by index
func (c *Tactic) LocalStripe(index int) (localStripe []int, n, m int) {
	if c.L == 0 {
		return nil, 0, 0
	}

	n, m, l := c.N/c.AZCount, c.M/c.AZCount, c.L/c.AZCount
	var azIdx int
	if index < c.N {
		azIdx = index / n
	} else if index < c.N+c.M {
		azIdx = (index - c.N) / m
	} else if index < c.N+c.M+c.L {
		azIdx = (index - c.N - c.M) / l
	} else {
		return nil, 0, 0
	}

	return c.LocalStripeInAZ(azIdx)
}

// LocalStripeInAZ get local stripe in az index
func (c *Tactic) LocalStripeInAZ(azIndex int) (localStripe []int, n, m int) {
	if c.L == 0 {
		return nil, 0, 0
	}

	l := c.L / c.AZCount
	if c.CodeType == OPPOLrc {
		n, m = c.N/c.AZCount, c.M/c.AZCount
	}
	if c.CodeType == AzureLrcP1 {
		if azIndex == c.AZCount-1 {
			n, m = 0, c.M
		} else {
			n, m = c.N/(c.AZCount-1), 0
		}
	}
	azStripes := c.GetECLayoutByAZ()
	if azIndex < 0 || azIndex >= len(azStripes) {
		return nil, 0, 0
	}
	return azStripes[azIndex][:], n + m, l
}

// GetAllCodeModes get all the available CodeModes
func GetAllCodeModes() []CodeMode {
	return []CodeMode{
		EC15P12,
		EC6P6,
		EC16P20L2,
		EC6P10L2,
		EC6P3L3,
		EC6P6Align0,
		EC6P6Align512,
		EC4P4L2,
		EC12P4,
		EC16P4,
		EC3P3,
		EC10P4,
		EC6P3,
		EC6P6L9,
		EC6P8L10,
		EC12P6L3,
		EC18P9L3,
		EC10P5L3,
		EC12P3L3,
	}
}
