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

package wal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileCache(t *testing.T) {
	openFunc := func(name logName) (*logFile, error) {
		if name.sequence == 2 {
			return nil, errors.New("file not exist")
		}
		return &logFile{
			name: name,
		}, nil
	}

	cache := newLogFileCache(4, openFunc)
	_, err := cache.Get(logName{1, 1000})
	require.Nil(t, err)
	_, err = cache.Get(logName{2, 1000})
	require.NotNil(t, err)

	for i := 3; i < 10; i++ {
		_, err = cache.Get(logName{uint64(i), 1000})
		require.Nil(t, err)
	}
	cache.Delete(logName{1, 1000})
	cache.Delete(logName{9, 1000})
	cache.Close()
}
