// Copyright 2023 The Cuber Authors.
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

package kvstore

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:generate mockgen -source=./kvstore.go -destination=./kvstore_mock.go -package=kvstore

func TestNewEngine(t *testing.T) {
	ctx := context.TODO()
	path, err := genTmpPath()
	require.NoError(t, err)
	defer os.RemoveAll(path)
	opt := new(Option)
	opt.CreateIfMissing = true
	eg, err := NewKVStore(ctx, path, RocksdbLsmKVType, opt)
	require.NoError(t, err)
	eg.Close()
}

func TestNewWriteBufferManager(t *testing.T) {
	mgr1 := NewWriteBufferManager(context.TODO(), RocksdbLsmKVType, 1<<10)
	mgr1.Close()

	mgr2 := NewWriteBufferManager(context.TODO(), "", 1<<10)
	require.Equal(t, nil, mgr2)
}

func TestNewCache(t *testing.T) {
	cache1 := NewCache(context.TODO(), RocksdbLsmKVType, 1<<10)
	cache1.Close()

	cache2 := NewCache(context.TODO(), "", 1<<10)
	require.Equal(t, nil, cache2)
}

func TestNewEnv(t *testing.T) {
	ctx := context.TODO()
	NewEnv(ctx, RocksdbLsmKVType)
}

func TestNewSstFileManager(t *testing.T) {
	ctx := context.TODO()
	env := NewEnv(ctx, RocksdbLsmKVType)
	NewSstFileManager(ctx, RocksdbLsmKVType, env)
}
