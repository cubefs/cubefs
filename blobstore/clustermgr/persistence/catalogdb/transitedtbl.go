// Copyright 2024 The CubeFS Authors.
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

package catalogdb

import (
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type TransitedTable struct {
	shardTbl kvstore.KVTable
	unitTbl  kvstore.KVTable
}

func OpenTransitedTable(db kvstore.KVStore) (*TransitedTable, error) {
	if db == nil {
		return nil, errors.New("OpenTransitedTable failed: db is nil")
	}
	return &TransitedTable{
		shardTbl: db.Table(transitedShardCF),
		unitTbl:  db.Table(transitedShardUnitCF),
	}, nil
}

func (t *TransitedTable) RangeShard(f func(record *ShardInfoRecord) error) (err error) {
	iter := t.shardTbl.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		record, err := decodeShardInfoRecord(iter.Value().Data())
		if err != nil {
			return err
		}
		iter.Key().Free()
		iter.Value().Free()
		if err = f(record); err != nil {
			return err
		}
	}
	return
}

func (t *TransitedTable) PutShardsAndShardUnits(shardRecs []*ShardInfoRecord, unitsRecs []*ShardUnitInfoRecord) (err error) {
	batch := t.shardTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range unitsRecs {
		unitKey := encodeShardUnitKey(unit.SuidPrefix)
		uRec, err := encodeShardUnitInfoRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(t.unitTbl.GetCf(), unitKey, uRec)
	}
	for _, shard := range shardRecs {
		shardID := encodeShardKey(shard.ShardID)
		shardRec, err := encodeShardInfoRecord(shard)
		if err != nil {
			return err
		}
		batch.PutCF(t.shardTbl.GetCf(), shardID, shardRec)
	}

	return t.shardTbl.DoBatch(batch)
}

func (t *TransitedTable) GetShardUnit(suidPrefix proto.SuidPrefix) (ret *ShardUnitInfoRecord, err error) {
	key := encodeShardUnitKey(suidPrefix)
	shardUnit, err := t.unitTbl.Get(key)
	if err != nil {
		return nil, err
	}

	return decodeShardUnitInfoRecord(shardUnit)
}

func (t *TransitedTable) PutShardUnits(units []*ShardUnitInfoRecord) (err error) {
	batch := t.unitTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range units {
		key := encodeShardUnitKey(unit.SuidPrefix)
		value, err := encodeShardUnitInfoRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(t.unitTbl.GetCf(), key, value)
	}

	return t.unitTbl.DoBatch(batch)
}

func (t *TransitedTable) DeleteShardAndUnits(shard *ShardInfoRecord, unitsRecs []*ShardUnitInfoRecord) error {
	batch := t.shardTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range unitsRecs {
		unitKey := encodeShardUnitKey(unit.SuidPrefix)
		batch.DeleteCF(t.unitTbl.GetCf(), unitKey)
	}
	shardID := encodeShardKey(shard.ShardID)
	batch.DeleteCF(t.shardTbl.GetCf(), shardID)

	return t.shardTbl.DoBatch(batch)
}
