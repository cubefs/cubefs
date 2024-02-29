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

package raftstore_db_test

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/stretchr/testify/require"
)

func TestClearRocksdb(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	db, err := raftstore_db.NewRocksDBStore(tempDir, 0, 0)
	require.NoError(t, err)

	_, err = db.Put("Hello", []byte("World"), true)
	require.NoError(t, err)

	err = db.Clear()
	require.NoError(t, err)

	val, err := db.Get("Hello")
	require.NoError(t, err)
	require.Nil(t, val)
}
