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
	"math/rand"
	"strings"
	"time"
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
		rand.Seed(time.Now().UnixNano())
		randNumber := rand.Intn(len(runs))
		result += string(runs[randNumber])
	}
	return result
}
func Intersect(string1, string2 []string) (inter []string) {
	m := make(map[string]int)
	for _, v := range string1 {
		m[v]++
	}

	for _, v := range string2 {
		times, ok := m[v]
		if ok && times > 0 {
			inter = append(inter, v)
			m[v]--
		}
	}
	return
}

func Projective(long, short []string) (result []string) {
	if len(short) == 0 {
		return long
	}
	if len(Intersect(long, short)) < len(short) {
		return make([]string, 0)
	}
	m := make(map[string]int)
	for _, v := range short {
		m[v]++
	}
	for _, s := range long {
		times, ok := m[s]
		if times > 0 && ok {
			m[s]--
		} else {
			result = append(result, s)
		}
	}
	return result
}

func IsStrEmpty(str string) bool {
	str = strings.ReplaceAll(str, " ", "")
	return len(str) == 0
}

func SubStringByLength(str string, length int) string {
	if len(str) <= length {
		return str
	}
	return str[:length]
}

func FormatTimestamp(timestamp int64) string {
	return time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")
}