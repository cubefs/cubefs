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
	"fmt"
	"math/big"
	"strconv"
	"strings"
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

// Any2String format any value to string.
func Any2String(value interface{}) string {
	var val string
	switch v := value.(type) {
	case string:
		val = v
	case *string:
		val = *v
	case bool:
		val = strconv.FormatBool(v)
	case *bool:
		val = strconv.FormatBool(*v)

	case int:
		val = strconv.FormatInt(int64(v), 10)
	case int8:
		val = strconv.FormatInt(int64(v), 10)
	case int16:
		val = strconv.FormatInt(int64(v), 10)
	case int32:
		val = strconv.FormatInt(int64(v), 10)
	case int64:
		val = strconv.FormatInt(int64(v), 10)
	case *int:
		val = strconv.FormatInt(int64(*v), 10)
	case *int8:
		val = strconv.FormatInt(int64(*v), 10)
	case *int16:
		val = strconv.FormatInt(int64(*v), 10)
	case *int32:
		val = strconv.FormatInt(int64(*v), 10)
	case *int64:
		val = strconv.FormatInt(int64(*v), 10)

	case uint:
		val = strconv.FormatUint(uint64(v), 10)
	case uint8:
		val = strconv.FormatUint(uint64(v), 10)
	case uint16:
		val = strconv.FormatUint(uint64(v), 10)
	case uint32:
		val = strconv.FormatUint(uint64(v), 10)
	case uint64:
		val = strconv.FormatUint(uint64(v), 10)
	case *uint:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint8:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint16:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint32:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint64:
		val = strconv.FormatUint(uint64(*v), 10)

	case float32:
		val = strconv.FormatFloat(float64(v), 'f', 6, 64)
	case float64:
		val = strconv.FormatFloat(float64(v), 'f', 6, 64)
	case *float32:
		val = strconv.FormatFloat(float64(*v), 'f', 6, 64)
	case *float64:
		val = strconv.FormatFloat(float64(*v), 'f', 6, 64)
	case complex64:
		val = strconv.FormatComplex(complex128(v), 'f', 6, 64)
	case complex128:
		val = strconv.FormatComplex(complex128(v), 'f', 6, 64)
	case *complex64:
		val = strconv.FormatComplex(complex128(*v), 'f', 6, 64)
	case *complex128:
		val = strconv.FormatComplex(complex128(*v), 'f', 6, 64)

	default:
		val = fmt.Sprintf("%v", value)
	}
	return val
}

// Any2String parse string to pointer of value.
func String2Any(str string, pvalue interface{}) error {
	var val interface{}
	var err error

	switch v := pvalue.(type) {
	case *string:
		val = str
	case *bool:
		val, err = strconv.ParseBool(str)

	case *int:
		val, err = strconv.ParseInt(str, 10, 0)
	case *int8:
		val, err = strconv.ParseInt(str, 10, 8)
	case *int16:
		val, err = strconv.ParseInt(str, 10, 16)
	case *int32:
		val, err = strconv.ParseInt(str, 10, 32)
	case *int64:
		val, err = strconv.ParseInt(str, 10, 64)

	case *uint:
		val, err = strconv.ParseUint(str, 10, 0)
	case *uint8:
		val, err = strconv.ParseUint(str, 10, 8)
	case *uint16:
		val, err = strconv.ParseUint(str, 10, 16)
	case *uint32:
		val, err = strconv.ParseUint(str, 10, 32)
	case *uint64:
		val, err = strconv.ParseUint(str, 10, 64)

	case *float32:
		val, err = strconv.ParseFloat(str, 32)
	case *float64:
		val, err = strconv.ParseFloat(str, 64)
	case *complex64:
		val, err = strconv.ParseComplex(str, 64)
	case *complex128:
		val, err = strconv.ParseComplex(str, 128)

	default:
		return fmt.Errorf("unknown type %v of %s %v", v, str, pvalue)
	}
	if err != nil {
		return err
	}

	switch v := pvalue.(type) {
	case *string:
		*v = val.(string)
	case *bool:
		*v = val.(bool)

	case *int:
		*v = int(val.(int64))
	case *int8:
		*v = int8(val.(int64))
	case *int16:
		*v = int16(val.(int64))
	case *int32:
		*v = int32(val.(int64))
	case *int64:
		*v = int64(val.(int64))

	case *uint:
		*v = uint(val.(uint64))
	case *uint8:
		*v = uint8(val.(uint64))
	case *uint16:
		*v = uint16(val.(uint64))
	case *uint32:
		*v = uint32(val.(uint64))
	case *uint64:
		*v = uint64(val.(uint64))

	case *float32:
		*v = float32(val.(float64))
	case *float64:
		*v = float64(val.(float64))
	case *complex64:
		*v = complex64(val.(complex128))
	case *complex128:
		*v = complex128(val.(complex128))
	}
	return nil
}
