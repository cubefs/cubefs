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

package meta

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestCachePut01(t *testing.T) {
	qc := NewQuotaCache(DefaultQuotaExpiration, MaxQuotaCache)
	var qinfo QuotaCacheInfo
	var quotaId uint32 = 1
	var inode uint64 = 2
	var inode2 uint64 = 3
	qinfo.inode = inode
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos[quotaId] = &proto.MetaQuotaInfo{
		RootInode: false,
	}
	qc.Put(inode, &qinfo)
	result := qc.Get(inode2)
	assert.True(t, result == nil)
	result = qc.Get(inode)
	assert.True(t, result != nil)
	assert.True(t, result.inode == inode)
	quotaInfo, ok := result.quotaInfos[quotaId]
	assert.True(t, ok == true)
	assert.True(t, quotaInfo.RootInode == false)
}

func TestCachePut02(t *testing.T) {
	qc := NewQuotaCache(DefaultQuotaExpiration, MaxQuotaCache)
	var qinfo QuotaCacheInfo
	var quotaId uint32 = 1
	var quotaId2 uint32 = 2
	var inode uint64 = 2
	qinfo.inode = inode
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos[quotaId] = &proto.MetaQuotaInfo{
		RootInode: false,
	}

	qc.Put(inode, &qinfo)

	var qinfo2 QuotaCacheInfo
	qinfo2.inode = inode
	qinfo2.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo2.quotaInfos[quotaId2] = &proto.MetaQuotaInfo{
		RootInode: true,
	}
	qc.Put(inode, &qinfo2)

	result := qc.Get(inode)
	_, ok := result.quotaInfos[quotaId]
	assert.True(t, ok == false)
	quotaInfo2, ok := result.quotaInfos[quotaId2]
	assert.True(t, ok == true)
	assert.True(t, quotaInfo2.RootInode == true)
}

func TestCachePut03(t *testing.T) {
	quotaCache := 11
	qc := NewQuotaCache(DefaultQuotaExpiration, quotaCache)

	var base uint64 = 2
	for i := 0; i < quotaCache+1; i++ {
		var qinfo QuotaCacheInfo
		inode := base + uint64(i)
		qinfo.inode = inode
		qc.Put(inode, &qinfo)
	}

	result := qc.Get(base)
	assert.True(t, result == nil)
	result = qc.Get(base + 1)
	assert.True(t, result == nil)
	result = qc.Get(base + uint64(quotaCache))
	assert.True(t, result != nil)
}

func TestCacheGet(t *testing.T) {
	expired := 2 * time.Second
	qc := NewQuotaCache(expired, MaxQuotaCache)
	var qinfo QuotaCacheInfo
	var quotaId uint32 = 1
	var inode uint64 = 2
	qinfo.inode = inode
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos[quotaId] = &proto.MetaQuotaInfo{
		RootInode: false,
	}

	qc.Put(inode, &qinfo)
	result := qc.Get(inode)
	assert.True(t, result != nil)
	time.Sleep(expired)
	result = qc.Get(inode)
	assert.True(t, result == nil)
}

func TestCacheDelete(t *testing.T) {
	qc := NewQuotaCache(DefaultQuotaExpiration, MaxQuotaCache)
	var qinfo QuotaCacheInfo
	var quotaId uint32 = 1
	var inode uint64 = 2
	qinfo.inode = inode
	qinfo.quotaInfos = make(map[uint32]*proto.MetaQuotaInfo)
	qinfo.quotaInfos[quotaId] = &proto.MetaQuotaInfo{
		RootInode: false,
	}

	qc.Put(inode, &qinfo)
	qc.Delete(inode)
	result := qc.Get(inode)
	assert.True(t, result == nil)
}
