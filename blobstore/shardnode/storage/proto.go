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
	return len(shardDataPrefix) + 8
}

func shardInfoPrefixSize() int {
	return len(shardInfoPrefix) + 8
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

func encodeShardInfoPrefix(suid proto.Suid, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	prefixSize := len(shardInfoPrefix)
	copy(raw, shardInfoPrefix)
	binary.BigEndian.PutUint64(raw[prefixSize:], uint64(suid))
}

func decodeShardInfoPrefix(raw []byte) proto.Suid {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	prefixSize := len(shardInfoPrefix)
	return proto.Suid(binary.BigEndian.Uint64(raw[prefixSize:]))
}

func encodeShardDataPrefix(suid proto.Suid, raw []byte) {
	copy(raw, shardDataPrefix)
	binary.BigEndian.PutUint64(raw[len(shardDataPrefix):], uint64(suid))
}

func encodeShardItemPrefix(suid proto.Suid, raw []byte) {
	shardPrefixSize := shardDataPrefixSize()
	encodeShardDataPrefix(suid, raw)
	copy(raw[shardPrefixSize:], itemSuffix)
}

func encodeShardDataMaxPrefix(suid proto.Suid, raw []byte) {
	shardPrefixSize := shardDataPrefixSize()
	encodeShardDataPrefix(suid, raw)
	copy(raw[shardPrefixSize:], maxSuffix)
}
