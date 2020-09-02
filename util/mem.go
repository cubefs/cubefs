// Copyright 2018 The Chubao Authors.
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
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	ioutil "io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	MEMINFO = "/proc/meminfo"
	PRO_MEM = "/proc/%d/status"
)

// GetMemInfo returns the memory information.
func GetMemInfo() (total, used uint64, err error) {
	fp, err := os.Open(MEMINFO)
	if err != nil {
		return
	}
	// TODO Unhandled errors
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

func GetProcessMemory(pid int) (used uint64, err error) {
	proFileName := fmt.Sprintf(PRO_MEM, pid)
	fp, err := os.Open(proFileName)
	if err != nil {
		return
	}
	defer fp.Close()
	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		fields := strings.Split(line, ":")
		key := fields[0]
		if key != "VmRSS" {
			continue
		}
		value := strings.TrimSpace(fields[1])
		value = strings.Replace(value, " kB", "", -1)
		used, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
		used = used * KB
		break
	}
	return
}

func GetDiskInfo(path string) (all uint64, used uint64, err error) {
	fs := syscall.Statfs_t{}
	if err = syscall.Statfs(path, &fs); err != nil {
		return
	}
	all = fs.Blocks * uint64(fs.Bsize)
	used = all - fs.Bfree*uint64(fs.Bsize)
	return
}

type diskScore struct {
	path         string
	freeMemory   uint64
	partitionNum int
	score        float64
}

// score = 1/(1+exp(1-free/allfree)) + 1/(1+exp(num/allnum))
func (ds *diskScore) computeScore(memory uint64, num int) {
	ds.score = 1/(1+math.Exp(1-float64(ds.freeMemory)/float64(memory))) + 1/(1+math.Exp(float64(ds.partitionNum)/float64(num)))
}

// select best dir by dirs , The reference parameters are the number of space remaining and partitions
func SelectDisk(dirs []string) (string, error) {

	result := make([]*diskScore, 0, len(dirs))

	var (
		sumMemory uint64
		sumCount  int
	)

	for _, path := range dirs {
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(path, &fs); err != nil {
			log.LogErrorf("statfs dir:[%s] has err:[%s]", path, err.Error())
			continue
		}
		freeMemory := fs.Bfree * uint64(fs.Bsize)

		dirs, _ := ioutil.ReadDir(path) //this onlu total all count in dir, so best to ensure the uniqueness of the directory

		if freeMemory < GB {
			log.LogWarnf("dir:[%s] not enough space:[%d] of disk so skip", path, freeMemory)
			continue
		}

		result = append(result, &diskScore{
			path:         path,
			freeMemory:   freeMemory,
			partitionNum: len(dirs),
		})

		sumMemory += freeMemory
		sumCount += len(dirs)
	}

	if len(result) == 0 {
		return "", fmt.Errorf("select disk got 0 result")
	}

	var max *diskScore

	for _, ds := range result {
		ds.computeScore(sumMemory, sumCount)
		if max == nil {
			max = ds
		}

		if max.score < ds.score {
			max = ds
		}
	}

	if max == nil {
		panic("impossibility")
	}

	return max.path, nil
}
