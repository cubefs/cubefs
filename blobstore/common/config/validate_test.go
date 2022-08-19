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

package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotZero(t *testing.T) {
	{
		require.True(t, testisZero(nil))
	}

	{
		var i int
		require.True(t, testisZero(i))
		i = 1
		require.False(t, testisZero(i))
	}

	{
		var i uint
		require.True(t, testisZero(i))
		i = 1
		require.False(t, testisZero(i))
	}

	{
		var f float64
		require.True(t, testisZero(f))
		f = 1.2
		require.False(t, testisZero(f))
	}

	{
		var str string
		require.True(t, testisZero(str))
		str = "str"
		require.False(t, testisZero(str))
	}

	{
		var sli []string
		sli1 := make([]string, 0)
		sli2 := []string{""}
		require.True(t, testisZero(sli))
		require.True(t, testisZero(sli1))
		require.False(t, testisZero(sli2))
	}

	{
		var arr [2]int
		require.True(t, testisZero(arr))
		arr = [2]int{0, 1}
		require.False(t, testisZero(arr))
	}

	{
		var m1 map[string]struct{}
		m2 := make(map[string]string)
		require.True(t, testisZero(m1))
		require.True(t, testisZero(m2))
		m2["key"] = "value"
		require.False(t, testisZero(m2))
	}

	{
		var v chan int
		require.True(t, testisZero(v))
		v = make(chan int)
		require.False(t, testisZero(v))
	}

	type School struct {
		Address string
	}
	type StructA struct {
		Name    []string
		Age     int
		School1 School
		School2 *School
	}

	{
		var struct1 *StructA
		require.True(t, testisZero(struct1))
		var struct2 StructA
		require.True(t, testisZero(struct2))

		struct2 = StructA{Age: 1}
		require.False(t, testisZero(struct2))
		struct2 = StructA{Name: []string{"name1"}}
		require.False(t, testisZero(struct2))

		struct2 = StructA{School2: &School{}}
		require.True(t, testisZero(struct2))
		struct2 = StructA{School2: &School{Address: "address1"}}
		require.False(t, testisZero(struct2))
	}
}

func testisZero(v interface{}) bool {
	rv := reflect.ValueOf(v)
	return isZero(rv)
}

func TestRequiredWithParent(t *testing.T) {
	type School struct {
		Addr string `json:"addr" validate:"required_with_parent"`
	}
	type Student struct {
		Name   string `json:"name"`
		School School `json:"school"`
	}
	stu1Json := `{"school" : {"addr" : "university"}, "name" : "jackma"}`
	stu2Json := `{"name" : "jackma"}`

	var stu1, stu2 Student
	err := LoadData(&stu1, []byte(stu1Json))
	require.Nil(t, err)

	err = LoadData(&stu2, []byte(stu2Json))
	require.Nil(t, err)
}
