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

package redis_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/redis"
)

const (
	testkey = "redis/test/key"
)

var (
	mr  *miniredis.Miniredis
	cli *redis.ClusterClient

	errJSONGet = errors.New("json get errror")
	errJSONSet = errors.New("json set errror")
)

func init() {
	mr, _ = miniredis.Run()
	cli = redis.NewClusterClient(&redis.ClusterConfig{
		Addrs: []string{mr.Addr()},
	})
}

func TestRedis(t *testing.T) {
	type Foo struct {
		Foo string `json:"foo"`
	}
	val := Foo{
		Foo: "foo",
	}
	defer cli.Del(context.TODO(), testkey)

	err := cli.Get(context.TODO(), testkey, nil)
	require.ErrorIs(t, redis.ErrNotFound, err)
	err = cli.Set(context.TODO(), testkey, val, 0)
	require.NoError(t, err)

	var v Foo
	err = cli.Get(context.TODO(), testkey, &v)
	require.NoError(t, err)
	require.Equal(t, val, v)
}

type valJSON struct {
	Key   []string            `json:"key"`
	Value map[string]struct{} `json:"value"`
}

func (val *valJSON) Encode() ([]byte, error) {
	return json.Marshal(val)
}

func (val *valJSON) Decode(data []byte) error {
	return json.Unmarshal(data, val)
}

func TestCodecJOSN(t *testing.T) {
	v := make(map[string]struct{}, 2)
	v["key1"] = struct{}{}
	var valueIn interface{} = &valJSON{
		Key:   []string{"key1", "key2"},
		Value: v,
	}

	val, ok := valueIn.(redis.CodeC)
	require.True(t, ok)
	data, err := val.Encode()
	require.Nil(t, err)

	var valueOut interface{} = new(valJSON)
	val, ok = valueOut.(redis.CodeC)
	require.True(t, ok)
	err = val.Decode(data)
	require.Nil(t, err)

	require.Equal(t, valueIn, valueOut)
}

func TestGetSetJOSN(t *testing.T) {
	v := make(map[string]struct{}, 2)
	v["key1"] = struct{}{}
	valueIn := &valJSON{
		Key:   []string{"key1", "key2"},
		Value: v,
	}
	defer cli.Del(context.TODO(), testkey)

	err := cli.Set(context.TODO(), testkey, valueIn, 0)
	require.NoError(t, err)

	var valueOut valJSON
	err = cli.Get(context.TODO(), testkey, &valueOut)
	require.NoError(t, err)
	require.Equal(t, *valueIn, valueOut)
}

type valBinary struct {
	Key   uint32
	Value uint64
}

func (val *valBinary) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (val *valBinary) Decode(data []byte) error {
	reader := bytes.NewReader(data)
	err := binary.Read(reader, binary.BigEndian, val)
	if err != nil {
		return err
	}
	return nil
}

func TestCodecBinary(t *testing.T) {
	var valueIn interface{} = &valBinary{uint32(100), uint64(200)}

	val, ok := valueIn.(redis.CodeC)
	require.True(t, ok)
	data, err := val.Encode()
	require.Nil(t, err)

	var valueOut interface{} = &valBinary{}
	val, ok = valueOut.(redis.CodeC)
	require.True(t, ok)
	err = val.Decode(data)
	require.Nil(t, err)

	require.Equal(t, valueIn, valueOut)
}

func TestGetSetBinary(t *testing.T) {
	valueIn := &valBinary{uint32(100), uint64(200)}
	defer cli.Del(context.TODO(), testkey)

	err := cli.Set(context.TODO(), testkey, valueIn, 0)
	require.NoError(t, err)

	var valueOut valBinary
	err = cli.Get(context.TODO(), testkey, &valueOut)
	require.NoError(t, err)
	require.Equal(t, *valueIn, valueOut)
}

type valJSONSet struct {
	Key   []string            `json:"key"`
	Value map[string]struct{} `json:"value"`
}

func (val *valJSONSet) Encode() ([]byte, error) {
	return nil, errJSONSet
}

func (val *valJSONSet) Decode(data []byte) error {
	return json.Unmarshal(data, val)
}

func TestJOSNErrorSet(t *testing.T) {
	valueIn := &valJSONSet{
		Key: []string{"key1", "key2"},
	}
	defer cli.Del(context.TODO(), testkey)

	err := cli.Set(context.TODO(), testkey, valueIn, 0)
	require.ErrorIs(t, redis.ErrEncodeFailed, err)

	var valueOut valJSONSet
	err = cli.Get(context.TODO(), testkey, &valueOut)
	require.ErrorIs(t, redis.ErrNotFound, err)
}

type valJSONGet struct {
	Key   []string            `json:"key"`
	Value map[string]struct{} `json:"value"`
}

func (val *valJSONGet) Encode() ([]byte, error) {
	return json.Marshal(val)
}

func (val *valJSONGet) Decode(data []byte) error {
	return errJSONGet
}

func TestJOSNErrorGet(t *testing.T) {
	valueIn := &valJSONGet{
		Key: []string{"key1", "key2"},
	}
	defer cli.Del(context.TODO(), testkey)

	err := cli.Set(context.TODO(), testkey, valueIn, 0)
	require.NoError(t, err)

	var valueOut valJSONGet
	err = cli.Get(context.TODO(), testkey, &valueOut)
	require.ErrorIs(t, redis.ErrDecodeFailed, err)
}
