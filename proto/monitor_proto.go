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

package proto

const (
	ActionRead = iota
	ActionRepairRead
	ActionAppendWrite
	ActionOverWrite
	ActionRepairWrite
	ActionMarkDelete
	ActionBatchMarkDelete
	ActionFlushDelete
	ActionDiskIOCreate
	ActionDiskIOWrite
	ActionDiskIORead
	ActionDiskIORemove
	ActionDiskIOPunch
	ActionDiskIOSync
)

var ActionDataMap = map[int]string{
	ActionRead:            "read",
	ActionRepairRead:      "repairRead",
	ActionAppendWrite:     "appendWrite",
	ActionOverWrite:       "overWrite",
	ActionRepairWrite:     "repairWrite",
	ActionMarkDelete:      "markDelete",
	ActionBatchMarkDelete: "batchMarkDelete",
	ActionFlushDelete:     "flushDelete",
	ActionDiskIOCreate:    "diskIOCreate",
	ActionDiskIOWrite:     "diskIOWrite",
	ActionDiskIORead:      "diskIORead",
	ActionDiskIORemove:    "diskIORemove",
	ActionDiskIOPunch:     "diskIOPunch",
	ActionDiskIOSync:      "diskIOSync",
}

const (
	ActionMetaCreateInode = iota
	ActionMetaEvictInode
	ActionMetaCreateDentry
	ActionMetaDeleteDentry
	ActionMetaLookup
	ActionMetaReadDir
	ActionMetaInodeGet
	ActionMetaBatchInodeGet
	ActionMetaExtentsAdd
	ActionMetaExtentsList
	ActionMetaTruncate
	ActionMetaExtentsInsert
	ActionMetaOpCreateInode
	ActionMetaOpEvictInode
	ActionMetaOpCreateDentry
	ActionMetaOpDeleteDentry
	ActionMetaOpLookup
	ActionMetaOpReadDir
	ActionMetaOpInodeGet
	ActionMetaOpBatchInodeGet
	ActionMetaOpExtentsAdd
	ActionMetaOpExtentsList
	ActionMetaOpTruncate
	ActionMetaOpExtentsInsert
)

var ActionMetaMap = map[int]string{
	ActionMetaCreateInode:     "createInode",
	ActionMetaEvictInode:      "evictInode",
	ActionMetaCreateDentry:    "createDentry",
	ActionMetaDeleteDentry:    "deleteDentry",
	ActionMetaLookup:          "lookup",
	ActionMetaReadDir:         "readDir",
	ActionMetaInodeGet:        "inodeGet",
	ActionMetaBatchInodeGet:   "batchInodeGet",
	ActionMetaExtentsAdd:      "addExtents",
	ActionMetaExtentsList:     "listExtents",
	ActionMetaTruncate:        "truncate",
	ActionMetaExtentsInsert:   "insertExtent",
	ActionMetaOpCreateInode:   "opCreateInode",
	ActionMetaOpEvictInode:    "opEvictInode",
	ActionMetaOpCreateDentry:  "opCreateDentry",
	ActionMetaOpDeleteDentry:  "opDeleteDentry",
	ActionMetaOpLookup:        "opLookup",
	ActionMetaOpReadDir:       "opReadDir",
	ActionMetaOpInodeGet:      "opInodeGet",
	ActionMetaOpBatchInodeGet: "opBatchInodeGet",
	ActionMetaOpExtentsAdd:    "opAddExtents",
	ActionMetaOpExtentsList:   "opListExtents",
	ActionMetaOpTruncate:      "opTruncate",
	ActionMetaOpExtentsInsert: "opInsertExtent",
}

const (
	ActionCacheRead = iota
	ActionCachePrepare
	ActionCacheEvict
	ActionCacheHit
	ActionCacheMiss
)

var ActionFlashMap = map[int]string{
	ActionCacheRead:    "read",
	ActionCachePrepare: "prepare",
	ActionCacheEvict:   "evict",
	ActionCacheHit:     "hit",
	ActionCacheMiss:    "miss",
}
