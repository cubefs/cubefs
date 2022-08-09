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

package objectnode

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	regexpEncodedETagValue = regexp.MustCompile("^(\\w)+(-(\\d)+)?(:(\\d)+)?$")
	regexpEncodedETagParts = [3]*regexp.Regexp{
		regexp.MustCompile("(\\w)+"),
		regexp.MustCompile("-(\\d)+"),
		regexp.MustCompile(":(\\d)+"),
	}
	staticDirectoryETagValue = ETagValue{Value: EmptyContentMD5String, PartNum: 0}
)

type ETagValue struct {
	Value   string
	PartNum int
	TS      time.Time
}

func (e ETagValue) ETag() string {
	sb := strings.Builder{}
	sb.WriteString(e.Value)
	if e.PartNum > 0 {
		sb.WriteString("-" + strconv.Itoa(e.PartNum))
	}
	return sb.String()
}

func (e ETagValue) TSUnix() int64 {
	return e.TS.Unix()
}

func (e ETagValue) String() string {
	if !e.Valid() {
		return "Invalid"
	}
	return fmt.Sprintf("%s_%d_%d", e.Value, e.PartNum, e.TS.Unix())
}

func (e ETagValue) Encode() string {
	sb := strings.Builder{}
	sb.WriteString(e.ETag())
	if ts := e.TS.Unix(); ts > 0 {
		sb.WriteString(":" + strconv.FormatInt(ts, 10))
	}
	return sb.String()
}

func (e ETagValue) Valid() bool {
	return len(e.Value) > 0 && e.PartNum >= 0 && e.TS.Unix() >= 0
}

func DirectoryETagValue() ETagValue {
	return staticDirectoryETagValue
}

func EmptyContentETagValue(ts time.Time) ETagValue {
	return ETagValue{
		Value:   EmptyContentMD5String,
		PartNum: 0,
		TS:      ts,
	}
}

func NewRandomBytesETagValue(partNum int, ts time.Time) ETagValue {
	r := rand.New(rand.NewSource(ts.Unix()))
	tmp := make([]byte, 4*1024)
	n, _ := r.Read(tmp)
	md5Hash := md5.New()
	md5Hash.Write(tmp[:n])

	value := ETagValue{
		Value:   hex.EncodeToString(md5Hash.Sum(nil)),
		PartNum: partNum,
		TS:      ts,
	}
	return value
}

func NewRandomUUIDETagValue(partNum int, ts time.Time) ETagValue {
	uUID, _ := uuid.NewRandom()
	md5Hash := md5.New()
	md5Hash.Write([]byte(uUID.String()))

	value := ETagValue{
		Value:   hex.EncodeToString(md5Hash.Sum(nil)),
		PartNum: partNum,
		TS:      ts,
	}
	return value
}

func ParseETagValue(raw string) ETagValue {
	value := ETagValue{}
	if !regexpEncodedETagValue.MatchString(raw) {
		return value
	}
	offset := 0
	valueLoc := regexpEncodedETagParts[0].FindStringIndex(raw)
	if len(valueLoc) != 2 {
		return value
	}
	value.Value = raw[valueLoc[0]:valueLoc[1]]
	offset += valueLoc[1] - valueLoc[0]
	partNumLoc := regexpEncodedETagParts[1].FindStringIndex(raw[offset:])
	if len(partNumLoc) == 2 {
		value.PartNum, _ = strconv.Atoi(raw[offset:][partNumLoc[0]+1 : partNumLoc[1]])
		offset += partNumLoc[1] - partNumLoc[0]
	} else {
		value.PartNum = 0
	}
	tsLoc := regexpEncodedETagParts[2].FindStringIndex(raw[offset:])
	if len(tsLoc) == 2 {
		unixSec, _ := strconv.ParseInt(raw[offset:][tsLoc[0]+1:tsLoc[1]], 10, 64)
		value.TS = time.Unix(unixSec, 0)
		offset += tsLoc[1] - tsLoc[0]
	} else {
		value.TS = time.Unix(0, 0)
	}
	return value
}
