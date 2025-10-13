// Copyright 2018 The CubeFS Authors.
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
	"crypto/rand"
	"math/big"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util"
)

func SubString(sourceString string, begin, end int) string {
	bytes := []byte(sourceString)
	stringLength := len(bytes)

	if begin < 0 {
		begin = 0
	}
	if end > stringLength {
		end = stringLength
	}
	return string(bytes[begin:end])
}

type RandomSeed byte

func (s RandomSeed) Runes() []rune {
	sourceBuilder := strings.Builder{}
	if s&Numeric > 0 {
		sourceBuilder.WriteString("0123456789")
	}
	if s&LowerLetter > 0 {
		sourceBuilder.WriteString("abcdefghijklmnopqrstuvwxyz")
	}
	if s&UpperLetter > 0 {
		sourceBuilder.WriteString("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	}
	return []rune(sourceBuilder.String())
}

const (
	Numeric RandomSeed = 1 << iota
	LowerLetter
	UpperLetter
)

func RandomString(length int, seed RandomSeed) string {
	runs := seed.Runes()
	result := ""
	for i := 0; i < length; i++ {
		lenInt64 := int64(len(runs))
		randNumber, _ := rand.Int(rand.Reader, big.NewInt(lenInt64))
		result += string(runs[randNumber.Uint64()])
	}
	return result
}

var (
	Any2String = util.Any2String
	String2Any = util.String2Any
)
