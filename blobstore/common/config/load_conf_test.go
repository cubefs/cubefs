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
	"testing"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/stretchr/testify/require"
)

const (
	srcconf = "conf_test_file.conf"
)

func TestLoad(t *testing.T) {
	oldVal := confName
	defer func() {
		confName = oldVal
	}()
	confName = nil
	Init("f", "appname", "notexist.conf")
	err := Load(confName)
	require.NotNil(t, err)
}

func TestLoadFile(t *testing.T) {
	var conf interface{}
	err := LoadFile(&conf, srcconf)
	require.Nil(t, err)

	err = LoadFile(&conf, "notexist.conf")
	require.NotNil(t, err)
}

func TestSafeLoadData(t *testing.T) {
	var conf interface{}
	{
		data := `{"field" : "value", }` // bad json
		err := SafeLoadData(&conf, []byte(data))
		require.NotNil(t, err)
	}
	{
		data := `{"field" : "value"}` // unknown field
		err := SafeLoadData(&conf, []byte(data))
		require.Nil(t, err)
	}
}

func TestLoadData(t *testing.T) {
	var conf interface{}
	data := `{"field" : "value", }` // bad json
	err := LoadData(&conf, []byte(data))
	require.NotNil(t, err)

	type Stu struct {
		Name string `json:"name" validate:"required"`
		Age  int    `json:"age"`
	}

	var stu Stu
	stuStr := `{"age" : 20}`
	err = LoadData(&stu, []byte(stuStr))
	require.NotNil(t, err)

	stuStr = `{"age" : 20, "name" : "slax", "unknown" : "field"}`
	err = LoadData(&stu, []byte(stuStr))
	require.NotNil(t, err)

	stuStr = `{"age" : 20, "name" : "slax"}`
	err = LoadData(&stu, []byte(stuStr))
	require.Nil(t, err)
}
