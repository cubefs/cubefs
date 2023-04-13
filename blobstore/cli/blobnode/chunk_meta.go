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

package blobnode

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/desertbit/grumble"
	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdChunkDump(chunkCommand *grumble.Command) {
	chunkDumpCommand := &grumble.Command{
		Name: "dump",
		Help: "dump static chunk tools",
	}
	chunkCommand.AddCommand(chunkDumpCommand)

	chunkDumpCommand.AddCommand(&grumble.Command{
		Name: "meta",
		Help: "dump shards of blobnode meta in rocksdb",
		Flags: func(f *grumble.Flags) {
			f.StringL("chunk", "", "chunk name")
			f.StringL("dbdir", "", "rocksdb path dir")
		},
		Run: chunkDumpMeta,
	})
	addCmdChunkDumpData(chunkDumpCommand)
}

const (
	_diskmetaKey = "diskinfo"
)

var (
	_chunkSpacePrefix = []byte("chunks")
	_shardSpacePrefix = []byte("shards")
	_vuidSpacePrefix  = []byte("vuids")

	shardSpacePrefixLen = len(_shardSpacePrefix)
	shardChunkCommonLen = shardSpacePrefixLen + blobnode.ChunkIdLength
	shardKeyLen         = shardChunkCommonLen + 8
)

func chunkDumpMeta(c *grumble.Context) error {
	dbdir := c.Flags.String("dbdir")
	chunk := c.Flags.String("chunk")
	if dbdir == "" {
		return errors.New("--dbdir is required")
	}
	noChunk := chunk == ""

	db, err := gorocksdb.OpenDbForReadOnly(gorocksdb.NewDefaultOptions(), dbdir, true)
	if err != nil {
		return err
	}
	defer db.Close()

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	if noChunk {
		fmt.Println("---------  disk  -----------")
		diskKey := disk.GenDiskKey(_diskmetaKey)
		diskInfo, err := db.GetBytes(ro, []byte(diskKey))
		if err != nil {
			return err
		}
		fmt.Println(string(diskInfo))

		fmt.Println("---------  vuids -----------")
		walkWithPrefix(db, _vuidSpacePrefix, printkv)

		fmt.Println("--------- chunks -----------")
		walkWithPrefix(db, _chunkSpacePrefix, printkv)
		return nil
	}

	id, err := blobnode.DecodeChunk(chunk)
	if err != nil {
		return err
	}
	fmt.Printf("---------- chunk %s shards: --------\n", chunk)
	return walkWithPrefix(db, storage.GenChunkCommonKey(id), printShard)
}

func printkv(key []byte, value []byte) error {
	fmt.Printf("key:%s val:%s\n", key, value)
	return nil
}

func printShard(key []byte, value []byte) error {
	shardKey, err := parseShardKey(key)
	if err != nil {
		return err
	}
	var shardMeta core.ShardMeta
	if err := shardMeta.Unmarshal(value); err != nil {
		return err
	}
	fmt.Printf("bid:%d version:%d offset:%d size:%d inline:%v\n", shardKey.Bid,
		shardMeta.Version, shardMeta.Offset, shardMeta.Size, shardMeta.Inline)
	return nil
}

func parseShardKey(data []byte) (key core.ShardKey, err error) {
	errShardKey := errors.New("shard key error")

	// pattern => shards${chunk_name}${ShardKey}
	if len(data) != shardKeyLen {
		return key, errShardKey
	}
	if !bytes.Equal(data[0:shardSpacePrefixLen], _shardSpacePrefix) {
		return key, errShardKey
	}

	copy(key.Chunk[:], data[shardSpacePrefixLen:shardChunkCommonLen])
	key.Bid = proto.BlobID(binary.BigEndian.Uint64(data[shardChunkCommonLen:shardKeyLen]))
	return key, nil
}

func walkWithPrefix(db *gorocksdb.DB, prefix []byte, processFn func([]byte, []byte) error) error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := db.NewIterator(ro)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		k, v := iter.Key(), iter.Value()
		key, value := k.Data(), v.Data()
		err := processFn(key, value)
		k.Free()
		v.Free()

		if err != nil {
			return err
		}
	}
	return nil
}
