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

package metanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testItem struct {
	data int
}

func (t *testItem) Less(than BtreeItem) bool {
	item, ok := than.(*testItem)
	return ok && (t.data < item.data)
}
func (t *testItem) Copy() BtreeItem {
	newItem := *t
	return &newItem
}

func TestBtree(t *testing.T) {
	bt := NewBtree()
	key1 := &testItem{data: 2}
	key2 := &testItem{data: 3}
	key3 := &testItem{data: 4}
	key4 := &testItem{data: 5}
	key5 := &testItem{data: 6}
	bt.ReplaceOrInsert(key1, true)
	bt.ReplaceOrInsert(key2, true)
	bt.ReplaceOrInsert(key3, true)
	bt.ReplaceOrInsert(key4, true)
	bt.ReplaceOrInsert(key5, true)

	item := bt.Get(key1)
	require.Equal(t, key1, item)
	item = bt.Delete(key2)
	require.Equal(t, key2, item)
	item = bt.Get(key2)
	require.Nil(t, item)
}
