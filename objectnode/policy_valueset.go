// Copyright 2023 The CubeFS Authors.
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
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// is enum type of string, int or bool.
type Value struct {
	t reflect.Kind
	s string
	i int
	b bool
}

// gets stored bool value.
func (v Value) GetBool() (bool, error) {
	var err error

	if v.t != reflect.Bool {
		err = fmt.Errorf("not a bool Value")
	}

	return v.b, err
}

// gets stored int value.
func (v Value) GetInt() (int, error) {
	var err error

	if v.t != reflect.Int {
		err = fmt.Errorf("not a int Value")
	}

	return v.i, err
}

//  gets stored string value.
func (v Value) GetString() (string, error) {
	var err error

	if v.t != reflect.String {
		err = fmt.Errorf("not a string Value")
	}

	return v.s, err
}

// gets enum type.
func (v Value) GetType() reflect.Kind {
	return v.t
}

// encodes Value to JSON data.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.t {
	case reflect.String:
		return json.Marshal(v.s)
	case reflect.Int:
		return json.Marshal(v.i)
	case reflect.Bool:
		return json.Marshal(v.b)
	}

	return nil, fmt.Errorf("unknown value kind %v", v.t)
}

//  stores bool value.
func (v *Value) StoreBool(b bool) {
	*v = Value{t: reflect.Bool, b: b}
}

//  stores int value.
func (v *Value) StoreInt(i int) {
	*v = Value{t: reflect.Int, i: i}
}

// stores string value.
func (v *Value) StoreString(s string) {
	*v = Value{t: reflect.String, s: s}
}

// returns string representation of value.
func (v Value) String() string {
	switch v.t {
	case reflect.String:
		return v.s
	case reflect.Int:
		return strconv.Itoa(v.i)
	case reflect.Bool:
		return strconv.FormatBool(v.b)
	}

	return ""
}

// decodes JSON data.
func (v *Value) UnmarshalJSON(data []byte) error {
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		v.StoreBool(b)
		return nil
	}

	var i int
	if err := json.Unmarshal(data, &i); err == nil {
		v.StoreInt(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v.StoreString(s)
		return nil
	}

	return fmt.Errorf(unknownJsonData, data)
}

// returns new bool value.
func NewBoolValue(b bool) Value {
	value := &Value{}
	value.StoreBool(b)
	return *value
}

// returns new int value.
func NewIntValue(i int) Value {
	value := &Value{}
	value.StoreInt(i)
	return *value
}

// returns new string value.
func NewStringValue(s string) Value {
	value := &Value{}
	value.StoreString(s)
	return *value
}

//  unique list of values.
type ValueSet map[Value]struct{}

// adds given value to value set.
func (set ValueSet) Add(value Value) {
	set[value] = struct{}{}
}

//  encodes ValueSet to JSON data.
func (set ValueSet) MarshalJSON() ([]byte, error) {
	values := []Value{}
	for k := range set {
		values = append(values, k)
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("invalid value set %v", set)
	}

	return json.Marshal(values)
}

// decodes JSON data.
func (set *ValueSet) UnmarshalJSON(data []byte) error {
	var v Value
	if err := json.Unmarshal(data, &v); err == nil {
		*set = make(ValueSet)
		set.Add(v)
		return nil
	}

	var values []Value
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}

	if len(values) < 1 {
		return fmt.Errorf(invalidConditionValue)
	}

	*set = make(ValueSet)
	for _, v = range values {
		if _, found := (*set)[v]; found {
			return fmt.Errorf(duplicateConditionValue, v)
		}

		set.Add(v)
	}

	return nil
}

//  returns new value set containing given values.
func NewValueSet(values ...Value) ValueSet {
	set := make(ValueSet)

	for _, value := range values {
		set.Add(value)
	}

	return set
}
