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

package defaulter

// Set basic type's value to default.
// Epsilon of float is 1e-9.

import (
	"fmt"
	"math"
	"reflect"
)

// Empty sets string value to default if it's empty.
func Empty(valPointer *string, defaultVal string) {
	if *valPointer == "" {
		*valPointer = defaultVal
	}
}

// Equal sets basic value to default if it equal zero.
func Equal(valPointer interface{}, defaultVal interface{}) {
	setDefault(valPointer, defaultVal, equalZero)
}

// Less sets basic value to default if it is less than zero.
func Less(valPointer interface{}, defaultVal interface{}) {
	setDefault(valPointer, defaultVal, lessZero)
}

// LessOrEqual sets basic value to default if it is not greater than zero.
func LessOrEqual(valPointer interface{}, defaultVal interface{}) {
	setDefault(valPointer, defaultVal, lessOrEqualZero)
}

func setDefault(valPointer interface{}, defaultVal interface{},
	cmp func(reflect.Value, reflect.Kind) bool) {
	typ := reflect.TypeOf(valPointer)
	if typ.Kind() != reflect.Ptr {
		panic(typ.Name() + " must be pointer")
	}
	typ = typ.Elem()
	val := reflect.ValueOf(valPointer).Elem()

	dTyp, dVal := parseDefault(defaultVal)
	if typ.Kind() != dTyp.Kind() {
		panic(fmt.Sprintf("not the same type %s != %s", typ.Kind().String(), dTyp.Kind().String()))
	}

	if cmp(val, typ.Kind()) {
		val.Set(dVal)
	}
}

func parseDefault(defaultVal interface{}) (reflect.Type, reflect.Value) {
	typ, val := reflect.TypeOf(defaultVal), reflect.ValueOf(defaultVal)
	if typ.Kind() == reflect.Ptr {
		typ, val = typ.Elem(), val.Elem()
	}
	return typ, val
}

func equalZero(val reflect.Value, typ reflect.Kind) bool {
	switch typ {
	case reflect.Bool:
		return !val.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return val.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return math.Float64bits(val.Float()) == 0
	default:
		panic("unsupported type " + typ.String())
	}
}

func lessZero(val reflect.Value, typ reflect.Kind) bool {
	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() < 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return false
	case reflect.Float32, reflect.Float64:
		return val.Float() < -1e-9
	default:
		panic("unsupported type " + typ.String())
	}
}

func lessOrEqualZero(val reflect.Value, typ reflect.Kind) bool {
	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() <= 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return val.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return val.Float() < 1e-9
	default:
		panic("unsupported type " + typ.String())
	}
}
