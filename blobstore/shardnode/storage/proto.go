package storage

import (
	"encoding/binary"

	storageproto "github.com/cubefs/cubefs/blobstore/shardnode/storage/proto"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	dataCF = "data"
)

var (
	// top level prefix
	shardDataPrefix = []byte{'d'}
	shardInfoPrefix = []byte{'s'}

	// shard's internal suffix
	itemSuffix = []byte{'a'}
	maxSuffix  = []byte{'z'}
)

type Timestamp struct{}

// proto for storage encoding/decoding and function return value

type (
	item = storageproto.Item

	shardInfo     = clustermgr.Shard
	shardUnitInfo = clustermgr.ShardUnitInfo
)

// todo: merge these encode and decode function into shard?

func shardDataPrefixSize() int {
	return len(shardDataPrefix) + 4
}

func shardInfoPrefixSize() int {
	return len(shardInfoPrefix) + 4
}

func shardItemPrefixSize() int {
	return shardDataPrefixSize() + len(itemSuffix)
}

func shardMaxPrefixSize() int {
	return shardDataPrefixSize() + len(maxSuffix)
}

func encodeShardInfoListPrefix(raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	copy(raw, shardInfoPrefix)
}

func encodeShardInfoPrefix(shardID proto.ShardID, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	prefixSize := len(shardInfoPrefix)
	copy(raw, shardInfoPrefix)
	binary.BigEndian.PutUint32(raw[prefixSize:], uint32(shardID))
}

func encodeShardDataPrefix(shardID proto.ShardID, raw []byte) {
	copy(raw, shardDataPrefix)
	binary.BigEndian.PutUint32(raw[len(shardDataPrefix):], uint32(shardID))
}

func encodeShardItemPrefix(shardID proto.ShardID, raw []byte) {
	shardPrefixSize := shardDataPrefixSize()
	encodeShardDataPrefix(shardID, raw)
	copy(raw[shardPrefixSize:], itemSuffix)
}

func encodeShardDataMaxPrefix(shardID proto.ShardID, raw []byte) {
	shardPrefixSize := shardDataPrefixSize()
	encodeShardDataPrefix(shardID, raw)
	copy(raw[shardPrefixSize:], maxSuffix)
}
