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

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/errors"
)

func IsEIO(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, syscall.EIO.Error()) || strings.Contains(errMsg, syscall.EROFS.Error())
}

func IsFileExists(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func IsEmptyDisk(filename string) (bool, error) {
	fis, err := os.ReadDir(filename)
	if err != nil {
		return false, err
	}

	if len(fis) == 0 {
		return true, nil
	}

	sysInitDir := map[string]bool{
		"lost+found": true,
	}

	for _, fi := range fis {
		if !sysInitDir[fi.Name()] {
			return false, nil
		}
	}

	return true, nil
}

// parse rangeStr to => [start, end]
func ParseHttpRangeStr(rangeBytesStr string) (start int64, end int64, err error) {
	pos := strings.Index(rangeBytesStr, "=")
	if pos == -1 {
		return start, end, errors.ErrRequestedRangeNotSatisfiable
	}

	rangeBytesStr = rangeBytesStr[pos+1:]
	pos = strings.Index(rangeBytesStr, "-")
	if pos == -1 {
		return start, end, errors.ErrRequestedRangeNotSatisfiable
	}

	startStr := strings.Trim(rangeBytesStr[:pos], " \t")
	endStr := strings.Trim(rangeBytesStr[pos+1:], " \t")

	if startStr == "" { // -val
		start = -1
	} else {
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return start, end, errors.ErrRequestedRangeNotSatisfiable
		}
	}

	if endStr == "" { // val-
		end = -1
	} else {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < 0 {
			return start, end, errors.ErrRequestedRangeNotSatisfiable
		}
	}

	return start, end, nil
}

// convert [start, end] => [from , to )
func FixHttpRange(start, end int64, size int64) (from, to int64, err error) {
	from, to = start, end

	if start < 0 { // -val
		from, to = to, size
		from = to - from
		if from < 0 {
			from = 0
		}
	} else {
		if to < 0 { // val-
			to = size
		} else {
			to = to + 1
		}
	}

	if !(from < to && to <= size) {
		return from, to, errors.ErrInvalidParam
	}

	return from, to, nil
}

func GenMetric(stat interface{}) (map[string]float64, error) {
	m := make(map[string]float64)
	data, err := json.Marshal(stat)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func BackgroudReqID(prefix string) string {
	return prefix + time.Now().Format("2006-01-02 15:04:05")
}
