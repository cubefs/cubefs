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

package util_test

import (
	"sync"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

var stringsForSetTest = []string{
	"Hello",
	"World",
	"1234",
}

func TestSet(t *testing.T) {
	s := util.NewSet()
	if s.Len() != 0 {
		t.Errorf("set should be empty")
		return
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(stringsForSetTest))
	for _, v := range stringsForSetTest {
		copyV := v
		go func() {
			s.Add(copyV)
			wg.Done()
		}()
	}
	wg.Wait()
	for _, v := range stringsForSetTest {
		require.Equal(t, s.Has(v), true)
	}
	s.Clear()
	require.Equal(t, s.Len(), 0)
	s.Add(stringsForSetTest[0])
	s.Remove(stringsForSetTest[0])
	require.NotEqual(t, s.Has(stringsForSetTest[0]), true)
}
