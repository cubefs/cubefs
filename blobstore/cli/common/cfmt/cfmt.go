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

// Package cfmt provides fmt string for all struct in blobstore
//
// function *F make the pointer struct value to []string
// function *Join []string into string with profix each line, sep is '\n'
//
package cfmt

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
)

func joinWithPrefix(prefix string, vals []string) string {
	if len(prefix) > 0 {
		for idx := range vals {
			vals[idx] = prefix + vals[idx]
		}
	}
	return strings.Join(vals, "\n")
}

func humanIBytes(size interface{}) string {
	str := fmt.Sprintf("%v", size)
	s, _ := strconv.Atoi(str)
	return fmt.Sprintf("%s (%s)", str, humanize.IBytes(uint64(s)))
}
