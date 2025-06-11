package flashgroupmanager

import (
	"github.com/cubefs/cubefs/util"
)

const (
	LRUCacheSize    = 3 << 30
	WriteBufferSize = 4 * util.MB
)

const (
	idKey   = "id"
	addrKey = "addr"
)

const (
	keySeparator     = "#"
	clusterAcronym   = "c"
	maxCommonIDKey   = keySeparator + "max_common_id"
	clusterPrefix    = keySeparator + clusterAcronym + keySeparator
	flashNodePrefix  = keySeparator + "fn" + keySeparator
	flashGroupPrefix = keySeparator + "fg" + keySeparator
)

const (
	opSyncAllocCommonID    uint32 = 0x0C
	opSyncAddFlashNode     uint32 = 0x6A
	opSyncDeleteFlashNode  uint32 = 0x6B
	opSyncUpdateFlashNode  uint32 = 0x6C
	opSyncAddFlashGroup    uint32 = 0x6D
	opSyncDeleteFlashGroup uint32 = 0x6E
	opSyncUpdateFlashGroup uint32 = 0x6F
	opSyncPutCluster       uint32 = 0x0D
)
