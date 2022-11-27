// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/google/btree"
)

type cacheItem proto.Entry

func (c *cacheItem) Less(than btree.Item) bool {
	return c.Index < than.(*cacheItem).Index
}

// cache中只保持最新的(index较大的)若干条日志
type entryCache struct {
	capacity int
	ents     *btree.BTree
	key      *cacheItem
}

func newEntryCache(capacity int) *entryCache {
	return &entryCache{
		capacity: capacity,
		ents:     btree.New(4),
		key:      new(cacheItem),
	}
}

func (c *entryCache) Get(index uint64) *proto.Entry {
	c.key.Index = index
	ent := c.ents.Get(c.key)
	if ent != nil {
		return (*proto.Entry)(ent.(*cacheItem))
	} else {
		return nil
	}
}

func (c *entryCache) Append(ent *proto.Entry) {
	// 截断冲突的
	for c.ents.Len() > 0 && c.ents.Max().(*cacheItem).Index >= ent.Index {
		c.ents.DeleteMax()
	}

	c.ents.ReplaceOrInsert((*cacheItem)(ent))

	// keep capacity
	for c.ents.Len() > c.capacity {
		c.ents.DeleteMin()
	}
}
