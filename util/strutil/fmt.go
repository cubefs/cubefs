// Copyright 2024 The CubeFS Authors.
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

package strutil

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util"
)

var suffixs = map[string]uint64{
	"kb": util.KB,
	"mb": util.MB,
	"gb": util.GB,
	"tb": util.TB,
	"pb": util.PB,
}

func ParseSize(sizeStr string) (size uint64, err error) {
	sizeStr = strings.ToLower(sizeStr)
	base := uint64(1)
	for suffix, v := range suffixs {
		if strings.HasSuffix(sizeStr, suffix) {
			base = v
			sizeStr = strings.TrimSuffix(sizeStr, suffix)
			break
		}
	}
	size, err = strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return
	}
	size *= base
	return
}

type SuffixPair struct {
	Size   uint64
	Suffix string
}

var suffixTable = []SuffixPair{
	{
		Size:   1,
		Suffix: "",
	},
	{
		Size:   util.KB,
		Suffix: "KB",
	},
	{
		Size:   util.MB,
		Suffix: "MB",
	},
	{
		Size:   util.GB,
		Suffix: "GB",
	},
	{
		Size:   util.TB,
		Suffix: "TB",
	},
	{
		Size:   util.PB,
		Suffix: "PB",
	},
}

func FormatSize(size uint64) (sizeStr string) {
	var pair SuffixPair
	for _, suffixPair := range suffixTable {
		if size >= suffixPair.Size {
			pair = suffixPair
			continue
		}
		break
	}
	size /= pair.Size
	sizeStr = fmt.Sprintf("%v%v", size, pair.Suffix)
	return
}
