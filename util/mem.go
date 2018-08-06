// Copyright 2018 The ChuBao Authors.
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

package util

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

const MEMINFO = "/proc/meminfo"

func GetMemInfo() (total, used uint64, err error) {
	fp, err := os.Open(MEMINFO)
	if err != nil {
		return
	}
	defer fp.Close()
	var (
		val    uint64
		free   uint64
		buffer uint64
		cached uint64
	)
	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		key := fields[0]
		value := strings.TrimSpace(fields[1])
		value = strings.Replace(value, " kB", "", -1)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
		switch key {
		case "MemTotal":
			total = val * KB
		case "MemFree":
			free = val * KB
		case "Buffers":
			buffer = val * KB
		case "Cached":
			cached = val * KB
		}
	}
	used = total - free - buffer - cached
	return
}
