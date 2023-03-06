package main

/*
 * dump meta data from rocksdb
 */

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/tecbot/gorocksdb"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	// _diskSpacePrefix       = "disk"
	_chunkSpacePrefix      = "chunks"
	_chunkShardSpacePrefix = "shards"
	_vuidSpacePrefix       = "vuids"

	_diskmetaKey = "diskinfo"
)

var (
	shardSpacePrefixLen = len(_chunkShardSpacePrefix)
	shardChunkCommonLen = shardSpacePrefixLen + bnapi.ChunkIdLength
	shardKeyLen         = shardChunkCommonLen + 8
)

var ErrShardKeyPrefix = errors.New("shard key error prefix")

type KV struct {
	key   []byte
	value []byte
}

func printkv(key []byte, value []byte) {
	fmt.Printf("k:%s, \tv:%s\n", key, value)
}

func printshardkv(key []byte, value []byte) {
	shardKey, err := parseShardKey(key)
	if err != nil {
		log.Printf("failed parse shardkey, err:%v\n", err)
		return
	}

	var shardMeta core.ShardMeta
	if err := shardMeta.Unmarshal(value); err != nil {
		log.Printf("failed unmarshal, err:%v\n", err)
		return
	}

	fmt.Printf("\tbid:%v\n", shardKey.Bid)
}

func parseShardKey(data []byte) (key core.ShardKey, err error) {
	// pattern => shards${chunk_name}${ShardKey}

	if len(data) != shardKeyLen {
		return key, ErrShardKeyPrefix
	}

	spacePrefix := data[0:shardSpacePrefixLen]
	if !bytes.Equal(spacePrefix, []byte(_chunkShardSpacePrefix)) {
		return key, ErrShardKeyPrefix
	}

	copy(key.Chunk[:], data[shardSpacePrefixLen:shardChunkCommonLen])
	key.Bid = proto.BlobID(binary.BigEndian.Uint64(data[shardChunkCommonLen:shardKeyLen]))

	return key, nil
}

func ListKeySpace(db *gorocksdb.DB, prefix string, processFn func([]byte, []byte)) (ret []KV) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := db.NewIterator(ro)

	for iter.Seek([]byte(prefix)); iter.ValidForPrefix([]byte(prefix)); iter.Next() {
		k, v := iter.Key(), iter.Value()
		key, value := k.Data(), v.Data()

		k.Free()
		v.Free()

		processFn(key, value)

		kv := KV{
			key:   key,
			value: value,
		}

		ret = append(ret, kv)
	}
	return
}

func main() {
	var dbpath string
	var chunk string

	flag.StringVar(&dbpath, "dbpath", "", "rocksdb path(dir)")
	flag.StringVar(&chunk, "chunk", "", "chunk name")
	flag.Parse()

	opts := gorocksdb.NewDefaultOptions()

	db, err := gorocksdb.OpenDbForReadOnly(opts, dbpath, true)
	if err != nil {
		log.Fatalf("err:%v", err)
		return
	}
	defer db.Close()

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	// step: get disk info
	diskKey := disk.GenDiskKey(_diskmetaKey)
	diskInfo, err := db.GetBytes(ro, []byte(diskKey))
	if err != nil {
		log.Fatalf("err:%v\n", err)
		return
	}
	fmt.Printf("disk:%s\ninfo:%s\n", diskKey, diskInfo)

	// step: list bind vuid
	println("--------- vuid binds -----------")
	prefix := _vuidSpacePrefix
	ListKeySpace(db, prefix, printkv)

	// step: list chunks
	println("--------- chunk infos -----------")
	prefix = _chunkSpacePrefix
	ListKeySpace(db, prefix, printkv)

	// list chunk shards
	if chunk == "" {
		return
	}

	id, err := bnapi.DecodeChunk(chunk)
	if err != nil {
		log.Fatalf("invalid chunk:%s, err:%v\n", chunk, err)
		return
	}

	fmt.Printf("---------- chunk:%s shards --------\n", chunk)
	prefix = string(storage.GenChunkCommonKey(id))
	ListKeySpace(db, prefix, printshardkv)
}
