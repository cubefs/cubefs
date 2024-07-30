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
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	SpaceInfoVersionNormal = iota + 1
	RouteInfoVersionNormal
	ShardInfoVersionNormal
	ShardUnitInfoVersionNormal
)

var shardUintDiskIDIndex = "diskID"

type indexItem struct {
	indexName string
	indexTbl  kvstore.KVTable
}

type CatalogTable struct {
	routeTbl kvstore.KVTable
	spaceTbl kvstore.KVTable
	shardTbl kvstore.KVTable
	unitTbl  kvstore.KVTable
	indexes  map[string]indexItem
}

type SpaceInfoRecord struct {
	Version    uint8                  `json:"-"`
	SpaceID    proto.SpaceID          `json:"sid"`
	Name       string                 `json:"name"`
	Status     proto.SpaceStatus      `json:"status"`
	FieldMetas []clustermgr.FieldMeta `json:"field_metas"`
	AccessKey  string                 `json:"access_key"`
	SecretKey  string                 `json:"secret_key"`
}

type RouteInfoRecord struct {
	Version      uint8                       `json:"-"`
	RouteVersion proto.RouteVersion          `json:"route_version"`
	Type         proto.CatalogChangeItemType `json:"type"`
	ItemDetail   interface{}                 `json:"item"`
}

type ShardInfoRecord struct {
	Version      uint8              `json:"-"`
	ShardID      proto.ShardID      `json:"shard_id"`
	SuidPrefixes []proto.SuidPrefix `json:"suid_prefixes"`
	LeaderDiskID proto.DiskID       `json:"leader_disk_id"`
	Range        sharding.Range     `json:"range"`
	RouteVersion proto.RouteVersion `json:"route_version"`
}

type ShardUnitInfoRecord struct {
	Version    uint8                 `json:"-"`
	SuidPrefix proto.SuidPrefix      `json:"suid_prefix"`
	Epoch      uint32                `json:"epoch"`
	NextEpoch  uint32                `json:"next_epoch"`
	DiskID     proto.DiskID          `json:"disk_id"`
	Status     proto.ShardUnitStatus `json:"status"`
	Learner    bool                  `json:"learner"`
}

type RouteInfoShardAdd struct {
	ShardID proto.ShardID `json:"shard_id"`
}

type RouteInfoShardUpdate struct {
	SuidPrefix proto.SuidPrefix `json:"suid_prefix"`
}

func OpenCatalogTable(db kvstore.KVStore) (*CatalogTable, error) {
	if db == nil {
		return nil, errors.New("OpenCatalogTable failed: db is nil")
	}
	table := &CatalogTable{
		routeTbl: db.Table(routeCF),
		spaceTbl: db.Table(spaceCF),
		shardTbl: db.Table(shardCF),
		unitTbl:  db.Table(shardUnitCF),
		indexes: map[string]indexItem{
			shardUintDiskIDIndex: {indexName: shardUintDiskIDIndex, indexTbl: db.Table(shardUnitDiskIDIndexCF)},
		},
	}
	return table, nil
}

func (c *CatalogTable) CreateSpace(info *SpaceInfoRecord) error {
	key := encodeSpaceKey(info.SpaceID)
	value, err := encodeSpaceInfoRecord(info)
	if err != nil {
		return err
	}

	err = c.spaceTbl.Put(kvstore.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (c *CatalogTable) RangeSpaceRecord(f func(record *SpaceInfoRecord) error) (err error) {
	snap := c.spaceTbl.NewSnapshot()
	defer c.spaceTbl.ReleaseSnapshot(snap)
	iter := c.spaceTbl.NewIterator(snap)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		Record, err := decodeSpaceInfoRecord(iter.Value().Data())
		if err != nil {
			return err
		}
		iter.Key().Free()
		iter.Value().Free()
		if err = f(Record); err != nil {
			return err
		}
	}
	return
}

func (c *CatalogTable) ListSpace(count uint32, afterSpace proto.SpaceID) (ret []*SpaceInfoRecord, err error) {
	snap := c.spaceTbl.NewSnapshot()
	defer c.spaceTbl.ReleaseSnapshot(snap)
	iter := c.spaceTbl.NewIterator(snap)
	defer iter.Close()

	if afterSpace > 0 {
		key := encodeSpaceKey(afterSpace)
		iter.Seek(key)
		if iter.Valid() {
			iter.Next()
		}
	} else {
		iter.SeekToFirst()
	}

	for i := uint32(0); i < count; i++ {
		if iter.Valid() {
			if iter.Err() != nil {
				return nil, errors.Info(iter.Err(), "space table iterate failed")
			}
			record, err := decodeSpaceInfoRecord(iter.Value().Data())
			if err != nil {
				return nil, errors.Info(err, "decodeSpaceInfoRecord failed")
			}
			ret = append(ret, record)
			iter.Key().Free()
			iter.Value().Free()
			iter.Next()
		}
	}

	return
}

func (c *CatalogTable) PutShardsAndUnitsAndRouteItems(shards []*ShardInfoRecord, units []*ShardUnitInfoRecord, routes []*RouteInfoRecord) error {
	batch := c.shardTbl.NewWriteBatch()
	defer batch.Destroy()

	for i := range shards {
		shardData, err := encodeShardInfoRecord(shards[i])
		if err != nil {
			return err
		}
		batch.PutCF(c.shardTbl.GetCf(), encodeShardKey(shards[i].ShardID), shardData)
	}

	for _, unit := range units {
		unitKey := encodeShardUnitKey(unit.SuidPrefix)
		unitData, err := encodeShardUnitInfoRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(c.unitTbl.GetCf(), unitKey, unitData)

		index, ok := c.indexes[shardUintDiskIDIndex]
		if !ok {
			return errors.New("index not exist")
		}
		indexKey := ""
		indexKey += fmtIndexKey(index.indexName, unit.DiskID, unit.SuidPrefix)
		batch.PutCF(c.indexes[shardUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), unitKey)
	}

	for i := range routes {
		routeData, err := encodeRouteInfoRecord(routes[i])
		if err != nil {
			return err
		}
		batch.PutCF(c.routeTbl.GetCf(), encodeRouteKey(routes[i].RouteVersion), routeData)
	}

	return c.shardTbl.DoBatch(batch)
}

func (c *CatalogTable) UpdateUnitsAndPutShardsAndRouteItems(shards []*ShardInfoRecord, units []*ShardUnitInfoRecord, routes []*RouteInfoRecord) error {
	batch := c.shardTbl.NewWriteBatch()
	defer batch.Destroy()

	for i := range shards {
		shardData, err := encodeShardInfoRecord(shards[i])
		if err != nil {
			return err
		}
		batch.PutCF(c.shardTbl.GetCf(), encodeShardKey(shards[i].ShardID), shardData)
	}

	for _, unit := range units {
		// remove old diskID index
		unitKey := encodeShardUnitKey(unit.SuidPrefix)
		oldUnitData, err := c.unitTbl.Get(unitKey)
		if err != nil {
			return errors.Info(err, "get shardUnit from unitTbl table error")
		}
		oldUnitRecord, err := decodeShardUnitInfoRecord(oldUnitData)
		if err != nil {
			return err
		}
		index, ok := c.indexes[shardUintDiskIDIndex]
		if !ok {
			return errors.New("index not exist")
		}
		oldDiskID := oldUnitRecord.DiskID
		oldIndexKey := ""
		oldIndexKey += fmtIndexKey(index.indexName, oldDiskID, unit.SuidPrefix)
		batch.DeleteCF(c.indexes[shardUintDiskIDIndex].indexTbl.GetCf(), []byte(oldIndexKey))

		unitData, err := encodeShardUnitInfoRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(c.unitTbl.GetCf(), unitKey, unitData)
		indexKey := ""
		indexKey += fmtIndexKey(index.indexName, unit.DiskID, unit.SuidPrefix)
		batch.PutCF(c.indexes[shardUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), unitKey)
	}

	for i := range routes {
		routeData, err := encodeRouteInfoRecord(routes[i])
		if err != nil {
			return err
		}
		batch.PutCF(c.routeTbl.GetCf(), encodeRouteKey(routes[i].RouteVersion), routeData)
	}

	return c.shardTbl.DoBatch(batch)
}

func (c *CatalogTable) RangeShardRecord(f func(record *ShardInfoRecord) error) (err error) {
	snap := c.shardTbl.NewSnapshot()
	defer c.shardTbl.ReleaseSnapshot(snap)
	iter := c.shardTbl.NewIterator(snap)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		Record, err := decodeShardInfoRecord(iter.Value().Data())
		if err != nil {
			return err
		}
		iter.Key().Free()
		iter.Value().Free()
		if err = f(Record); err != nil {
			return err
		}
	}
	return
}

func (c *CatalogTable) ListShard(count uint32, afterShard proto.ShardID) (ret []proto.ShardID, err error) {
	snap := c.shardTbl.NewSnapshot()
	defer c.shardTbl.ReleaseSnapshot(snap)
	iter := c.shardTbl.NewIterator(snap)
	defer iter.Close()

	if afterShard > 0 {
		key := encodeShardKey(afterShard)
		iter.Seek(key)
		if iter.Valid() {
			iter.Next() // exclude afterShard
		}
	} else {
		iter.SeekToFirst()
	}

	for i := uint32(0); i < count; i++ {
		if iter.Valid() {
			if iter.Err() != nil {
				return nil, errors.Info(iter.Err(), "shard table iterate failed")
			}
			ret = append(ret, decodeShardKey(iter.Key().Data()))
			iter.Key().Free()
			iter.Value().Free()
			iter.Next()
		}
	}

	return
}

func (c *CatalogTable) GetShard(shardID proto.ShardID) (ret *ShardInfoRecord, err error) {
	key := encodeShardKey(shardID)
	bytes, err := c.shardTbl.Get(key)
	if err != nil {
		return nil, err
	}
	ret, err = decodeShardInfoRecord(bytes)
	return
}

func (c *CatalogTable) RangeShardUnits(f func(unitRecord *ShardUnitInfoRecord)) (err error) {
	snap := c.unitTbl.NewSnapshot()
	defer c.unitTbl.ReleaseSnapshot(snap)
	iter := c.unitTbl.NewIterator(snap)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); {
		if iter.Err() != nil {
			return errors.Info(iter.Err(), "range shard units err")
		}
		info, err := decodeShardUnitInfoRecord(iter.Value().Data())
		iter.Key().Free()
		iter.Value().Free()
		if err != nil {
			return err
		}
		f(info)
		iter.Next()
	}
	return
}

func (c *CatalogTable) GetShardUnit(suidPrefix proto.SuidPrefix) (ret *ShardUnitInfoRecord, err error) {
	key := encodeShardUnitKey(suidPrefix)
	shardUnit, err := c.unitTbl.Get(key)
	if err != nil {
		return nil, errors.Info(err, "get shardUnit from table error").Detail(err)
	}

	return decodeShardUnitInfoRecord(shardUnit)
}

func (c *CatalogTable) ListShardUnit(diskID proto.DiskID) (ret []proto.SuidPrefix, err error) {
	indexTbl := c.indexes[shardUintDiskIDIndex].indexTbl
	indexKey := fmt.Sprintf(c.indexes[shardUintDiskIDIndex].indexName+"-%d-", diskID)

	snap := indexTbl.NewSnapshot()
	defer indexTbl.ReleaseSnapshot(snap)
	iter := indexTbl.NewIterator(snap)
	defer iter.Close()

	for iter.Seek([]byte(indexKey)); iter.ValidForPrefix([]byte(indexKey)); iter.Next() {
		if iter.Err() != nil {
			return nil, errors.Info(iter.Err(), "shardUnit index table iterate failed")
		}
		value := iter.Value().Data()
		suidPrefix := decodeShardUnitKey(value)
		ret = append(ret, suidPrefix)
		iter.Key().Free()
		iter.Value().Free()
	}
	return
}

func (c *CatalogTable) GetFirstRouteItem() (*RouteInfoRecord, error) {
	iter := c.routeTbl.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()
	if iter.Valid() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		if iter.Key().Size() > 0 {
			ret, err := decodeRouteInfoRecord(iter.Value().Data())
			if err != nil {
				return nil, errors.Info(err, "decode route info db failed").Detail(err)
			}
			iter.Key().Free()
			iter.Value().Free()
			return ret, nil
		}
	}
	return nil, nil
}

func (c *CatalogTable) ListRoute() ([]*RouteInfoRecord, error) {
	iter := c.routeTbl.NewIterator(nil)
	defer iter.Close()

	ret := make([]*RouteInfoRecord, 0)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		if iter.Key().Size() > 0 {
			info, err := decodeRouteInfoRecord(iter.Value().Data())
			if err != nil {
				return nil, errors.Info(err, "decode route info db failed").Detail(err)
			}
			ret = append(ret, info)
			iter.Key().Free()
			iter.Value().Free()
		}
	}
	return ret, nil
}

func (c *CatalogTable) DeleteOldRoutes(before proto.RouteVersion) error {
	batch := c.routeTbl.NewWriteBatch()
	defer batch.Destroy()

	batch.DeleteRangeCF(c.routeTbl.GetCf(), encodeRouteKey(0), encodeRouteKey(before))
	return c.routeTbl.DoBatch(batch)
}

func encodeSpaceInfoRecord(info *SpaceInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func decodeSpaceInfoRecord(data []byte) (*SpaceInfoRecord, error) {
	version := data[0]
	if version == SpaceInfoVersionNormal {
		ret := &SpaceInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid space info version")
}

func encodeRouteInfoRecord(info *RouteInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func decodeRouteInfoRecord(data []byte) (*RouteInfoRecord, error) {
	version := data[0]
	if version == RouteInfoVersionNormal {
		ret := &RouteInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		if err != nil {
			return nil, err
		}
		ret.Version = version
		switch ret.Type {
		case proto.CatalogChangeItemAddShard:
			ret.ItemDetail = &RouteInfoShardAdd{}
			err = json.Unmarshal(data[1:], ret)
		case proto.CatalogChangeItemUpdateShard:
			ret.ItemDetail = &RouteInfoShardUpdate{}
			err = json.Unmarshal(data[1:], ret)
		default:
			panic(fmt.Sprintf("unsupported route item type: %d", ret.Type))
		}
		return ret, err
	}
	return nil, errors.New("invalid route info version")
}

func encodeShardUnitInfoRecord(info *ShardUnitInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func decodeShardUnitInfoRecord(data []byte) (*ShardUnitInfoRecord, error) {
	version := data[0]
	if version == ShardUnitInfoVersionNormal {
		ret := &ShardUnitInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid shard unit info version")
}

func encodeShardInfoRecord(info *ShardInfoRecord) ([]byte, error) {
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func decodeShardInfoRecord(data []byte) (*ShardInfoRecord, error) {
	version := data[0]
	if version == ShardInfoVersionNormal {
		ret := &ShardInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid shard info version")
}

func encodeSpaceKey(spaceID proto.SpaceID) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, uint64(spaceID))
	return ret
}

func encodeShardKey(shardID proto.ShardID) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(shardID))
	return ret
}

func decodeShardKey(b []byte) proto.ShardID {
	key := binary.BigEndian.Uint32(b)
	return proto.ShardID(key)
}

func encodeShardUnitKey(suidPrefix proto.SuidPrefix) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, uint64(suidPrefix))
	return ret
}

func decodeShardUnitKey(buf []byte) proto.SuidPrefix {
	key := binary.BigEndian.Uint64(buf)
	return proto.SuidPrefix(key)
}

func encodeRouteKey(ver proto.RouteVersion) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, uint64(ver))
	return ret
}

func fmtIndexKey(indexName string, diskID proto.DiskID, suidPrefix proto.SuidPrefix) string {
	return fmt.Sprintf("%s-%d-%d", indexName, diskID, suidPrefix)
}
