// Copyright 2020 The CubeFS Authors.
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
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	multipartIDMetaLength = 25
	multipartIDFlagLength = 2
	multipartIDDelimiter  = "x"
)

type MultipartID string

func (id MultipartID) String() string {
	return string(id)
}

func (id MultipartID) PartitionID() (pID uint64, found bool) {
	if len(id) < multipartIDMetaLength {
		return
	}
	var (
		mpStart        int
		mpEnd          int
		flag           string
		length         uint64
		appendInfo     []rune
		mpIdString     string
		delimiterIndex int
		err            error
	)
	delimiterIndex = len(id) - multipartIDMetaLength
	appendInfo = []rune(id)[delimiterIndex:]
	if string(appendInfo[0]) != multipartIDDelimiter {
		return
	}
	flag = string(appendInfo[1 : multipartIDFlagLength+1])
	length, err = strconv.ParseUint(flag, 10, 64)
	if err != nil {
		return 0, false
	}
	mpStart = 1 + multipartIDFlagLength
	mpEnd = mpStart + int(length)
	mpIdString = string(appendInfo[mpStart:mpEnd])
	pID, err = strconv.ParseUint(mpIdString, 10, 64)
	found = err == nil
	return
}

func MultipartIDFromString(src string) MultipartID {
	return MultipartID(src)
}

func CreateMultipartID(mpId uint64) MultipartID {
	var (
		mpIdLength  string
		multipartId string
	)

	// Append special char 'x' and meta partition id after generated multipart id.
	// If appended string length is less then 25, completion using random string
	tempLength := len(strconv.FormatUint(mpId, 10))

	// Meta partition id's length is fixed, if current length is not enough,
	// append '0' in the beginning of current meta partition id
	if len(strconv.Itoa(tempLength)) < multipartIDFlagLength {
		for i := 0; i < multipartIDFlagLength-len(strconv.Itoa(tempLength)); i++ {
			mpIdLength += "0"
		}
		mpIdLength += strconv.Itoa(tempLength)
	}
	appendMultipart := fmt.Sprintf("%s%d", mpIdLength, mpId)
	nextId := strings.ReplaceAll(uuid.New().String(), "-", "")
	if len(appendMultipart) < multipartIDMetaLength-1 {
		l := multipartIDMetaLength - 1 - len(appendMultipart)
		t := strings.ReplaceAll(uuid.New().String(), "-", "")
		r := string([]rune(t)[:l])
		multipartId = fmt.Sprintf("%s%s%s%s", nextId, multipartIDDelimiter, appendMultipart, r)
	} else {
		multipartId = fmt.Sprintf("%s%s%s", nextId, multipartIDDelimiter, appendMultipart)
	}
	return MultipartID(multipartId)
}
