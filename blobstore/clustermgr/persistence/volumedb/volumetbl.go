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

package volumedb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var volumeUintDiskIDIndex = "diskID"

type indexItem struct {
	indexName string
	indexTbl  kvstore.KVTable
}

type VolumeTable struct {
	volTbl   kvstore.KVTable
	unitTbl  kvstore.KVTable
	tokenTbl kvstore.KVTable
	taskTbl  kvstore.KVTable
	indexes  map[string]indexItem
}

type VolumeRecord struct {
	Vid            proto.Vid
	VuidPrefixs    []proto.VuidPrefix
	CodeMode       codemode.CodeMode
	HealthScore    int
	Status         proto.VolumeStatus
	Total          uint64
	Free           uint64
	Used           uint64
	CreateByNodeID uint64
}

type VolumeTaskRecord struct {
	Vid      proto.Vid
	TaskType base.VolumeTaskType
	TaskId   string
}

type VolumeUnitRecord struct {
	VuidPrefix proto.VuidPrefix
	Epoch      uint32
	NextEpoch  uint32
	DiskID     proto.DiskID
	Free       uint64
	Total      uint64
	Used       uint64
	Compacting bool
}

type TokenRecord struct {
	Vid        proto.Vid
	TokenID    string
	ExpireTime int64
}

func OpenVolumeTable(db kvstore.KVStore) (*VolumeTable, error) {
	if db == nil {
		return nil, errors.New("OpenVolumeTable failed: db is nil")
	}
	return &VolumeTable{
		volTbl:   db.Table(volumeCF),
		unitTbl:  db.Table(volumeUnitCF),
		tokenTbl: db.Table(volumeTokenCF),
		taskTbl:  db.Table(volumeTaskCF),
		indexes: map[string]indexItem{
			volumeUintDiskIDIndex: {indexName: "diskID", indexTbl: db.Table(volumeUnitDiskIDIndexCF)},
		},
	}, nil
}

func (t *VolumeTable) PutVolumeAndTask(volRec *VolumeRecord, taskRec *VolumeTaskRecord) error {
	batch := t.volTbl.NewWriteBatch()
	defer batch.Destroy()

	if volRec == nil && taskRec == nil {
		return nil
	}
	if volRec != nil {
		valueVol, err := encodeVolumeRecord(volRec)
		if err != nil {
			return err
		}
		batch.PutCF(t.volTbl.GetCf(), EncodeVid(volRec.Vid), valueVol)
	}

	if taskRec != nil {
		value, err := encodeTaskRecord(taskRec)
		if err != nil {
			return err
		}
		batch.PutCF(t.taskTbl.GetCf(), EncodeVid(taskRec.Vid), value)
	}
	return t.volTbl.DoBatch(batch)
}

func (t *VolumeTable) DeleteTaskRecord(vid proto.Vid) error {
	return t.taskTbl.Delete(EncodeVid(vid))
}

func (t *VolumeTable) ListTaskRecords(f func(rec *VolumeTaskRecord) bool) (err error) {
	snap := t.taskTbl.NewSnapshot()
	defer t.taskTbl.ReleaseSnapshot(snap)
	iter := t.taskTbl.NewIterator(snap)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		rec, err := decodeTaskRecord(iter.Value().Data())
		iter.Key().Free()
		iter.Value().Free()
		if err != nil {
			break
		}
		if !f(rec) {
			break
		}
	}
	return err
}

func (t *VolumeTable) GetVolume(vid proto.Vid) (ret *VolumeRecord, err error) {
	key := EncodeVid(vid)
	volByte, err := t.volTbl.Get(key)
	if err != nil {
		return nil, err
	}
	ret, err = decodeVolumeRecord(volByte)
	return
}

func (t *VolumeTable) PutVolumeRecord(rec *VolumeRecord) (err error) {
	key := EncodeVid(rec.Vid)
	value, err := encodeVolumeRecord(rec)
	if err != nil {
		return err
	}
	return t.volTbl.Put(kvstore.KV{Key: key, Value: value})
}

func (v *VolumeTable) PutVolumeRecords(recs []*VolumeRecord) (err error) {
	kvs := make([]kvstore.KV, 0)
	for _, volRec := range recs {
		k := EncodeVid(volRec.Vid)
		v, err := encodeVolumeRecord(volRec)
		if err != nil {
			return err
		}
		kvs = append(kvs, kvstore.KV{Key: k, Value: v})
	}
	return v.volTbl.WriteBatch(kvs, false)
}

// putVolumes:put volumeRecords volumeUnitRecords and tokenRecords at once
func (v *VolumeTable) PutVolumes(volumeRecs []*VolumeRecord, volumeUnitsRecs [][]*VolumeUnitRecord, tokens []*TokenRecord) (err error) {
	batch := v.volTbl.NewWriteBatch()
	defer batch.Destroy()

	for i, vol := range volumeRecs {
		for _, unit := range volumeUnitsRecs[i] {
			unitKey := encodeVuidPrefix(unit.VuidPrefix)
			uRec, err := encodeVolumeUnitRecord(unit)
			if err != nil {
				return err
			}
			batch.PutCF(v.unitTbl.GetCf(), unitKey, uRec)

			indexKey := ""
			index, ok := v.indexes[volumeUintDiskIDIndex]
			if !ok {
				return errors.New("index not exist")
			}
			indexKey += fmt.Sprintf(index.indexName+"-%v-%v", unit.DiskID, unit.VuidPrefix)
			batch.PutCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), unitKey)

		}
		vid := EncodeVid(vol.Vid)
		valueVol, err := encodeVolumeRecord(vol)
		if err != nil {
			return err
		}
		batch.PutCF(v.volTbl.GetCf(), vid, valueVol)
		valueToken, err := encodeTokenRecord(tokens[i])
		if err != nil {
			return err
		}
		batch.PutCF(v.tokenTbl.GetCf(), vid, valueToken)
	}
	v.volTbl.DoBatch(batch)

	return
}

func (v *VolumeTable) PutVolumeAndToken(volumeRecs []*VolumeRecord, tokens []*TokenRecord) (err error) {
	batch := v.volTbl.NewWriteBatch()
	defer batch.Destroy()

	for i, vol := range volumeRecs {
		vidKey := EncodeVid(vol.Vid)
		valueToken, err := encodeTokenRecord(tokens[i])
		if err != nil {
			return err
		}
		batch.PutCF(v.tokenTbl.GetCf(), vidKey, valueToken)
		valueVol, err := encodeVolumeRecord(vol)
		if err != nil {
			return err
		}
		batch.PutCF(v.volTbl.GetCf(), vidKey, valueVol)
	}

	return v.volTbl.DoBatch(batch)
}

func (v *VolumeTable) PutVolumeAndVolumeUnit(volumeRecs []*VolumeRecord, volumeUnitsRecs [][]*VolumeUnitRecord) (err error) {
	batch := v.volTbl.NewWriteBatch()
	defer batch.Destroy()

	for i, vol := range volumeRecs {
		for _, unit := range volumeUnitsRecs[i] {
			unitKey := encodeVuidPrefix(unit.VuidPrefix)
			uRec, err := encodeVolumeUnitRecord(unit)
			if err != nil {
				return err
			}
			batch.PutCF(v.unitTbl.GetCf(), unitKey, uRec)

			indexKey := ""
			index, ok := v.indexes[volumeUintDiskIDIndex]
			if !ok {
				return errors.New("index not exist")
			}
			indexKey += fmt.Sprintf(index.indexName+"-%v-%v", unit.DiskID, unit.VuidPrefix)
			batch.PutCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), unitKey)

		}
		vid := EncodeVid(vol.Vid)
		valueVol, err := encodeVolumeRecord(vol)
		if err != nil {
			return err
		}
		batch.PutCF(v.volTbl.GetCf(), vid, valueVol)
	}

	return v.volTbl.DoBatch(batch)
}

func (v *VolumeTable) RangeVolumeRecord(f func(Record *VolumeRecord) error) (err error) {
	snap := v.volTbl.NewSnapshot()
	defer v.volTbl.ReleaseSnapshot(snap)
	iter := v.volTbl.NewIterator(snap)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		Record, err := decodeVolumeRecord(iter.Value().Data())
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

func (v *VolumeTable) ListVolume(count int, afterVid proto.Vid) (ret []proto.Vid, err error) {
	snap := v.volTbl.NewSnapshot()
	defer v.volTbl.ReleaseSnapshot(snap)
	iter := v.volTbl.NewIterator(snap)
	defer iter.Close()

	if afterVid > 0 {
		vidKey := EncodeVid(afterVid)
		iter.Seek(vidKey)
		if iter.Valid() {
			iter.Next()
		}
	} else {
		iter.SeekToFirst()
	}

	for i := 0; i < count; i++ {
		if iter.Valid() {
			if iter.Err() != nil {
				return nil, errors.Info(iter.Err(), "volume table iterate failed")
			}
			ret = append(ret, DecodeVid(iter.Key().Data()))
			iter.Key().Free()
			iter.Value().Free()
			iter.Next()
		}
	}

	return
}

func decodeVolumeRecord(volByte []byte) (ret *VolumeRecord, err error) {
	dec := gob.NewDecoder(bytes.NewReader(volByte))
	err = dec.Decode(&ret)

	return
}

func encodeVolumeRecord(Record *VolumeRecord) (ret []byte, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(*Record)
	if err != nil {
		return nil, err
	}
	ret = buf.Bytes()
	return
}

func (t *VolumeTable) PutToken(vid proto.Vid, token *TokenRecord) (err error) {
	key := EncodeVid(vid)
	value, err := encodeTokenRecord(token)
	if err != nil {
		return err
	}

	return t.tokenTbl.Put(kvstore.KV{Key: key, Value: value})
}

func (t *VolumeTable) GetToken(vid proto.Vid) (ret *TokenRecord, err error) {
	key := EncodeVid(vid)
	tokenByte, err := t.tokenTbl.Get(key)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(bytes.NewReader(tokenByte))
	err = dec.Decode(&ret)
	if err != nil {
		return nil, errors.Info(err, "decode token error").Detail(err)
	}
	return
}

func (t *VolumeTable) PutTokens(tokens []*TokenRecord) (err error) {
	kvs := make([]kvstore.KV, len(tokens))
	for i, token := range tokens {
		kvs[i].Key = EncodeVid(token.Vid)
		kvs[i].Value, err = encodeTokenRecord(token)
		if err != nil {
			return err
		}
	}
	err = t.tokenTbl.WriteBatch(kvs, false)
	return
}

func (t *VolumeTable) GetAllTokens() (ret []*TokenRecord, err error) {
	snap := t.tokenTbl.NewSnapshot()
	defer t.tokenTbl.ReleaseSnapshot(snap)

	iter := t.tokenTbl.NewIterator(snap)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); {
		tokenRec, err := decodeTokenRecord(iter.Value().Data())
		iter.Key().Free()
		iter.Value().Free()
		if err != nil {
			return nil, err
		}
		ret = append(ret, tokenRec)
		iter.Next()
	}

	return
}

func (t *VolumeTable) DeleteToken(vid proto.Vid) (err error) {
	key := EncodeVid(vid)
	err = t.tokenTbl.Delete(key)
	return
}

func encodeTokenRecord(token *TokenRecord) (ret []byte, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(*token)
	if err != nil {
		return nil, err
	}
	ret = buf.Bytes()
	return
}

func decodeTokenRecord(buf []byte) (token *TokenRecord, err error) {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	err = dec.Decode(&token)
	return
}

func (v *VolumeTable) GetVolumeUnit(vuidPrefix proto.VuidPrefix) (ret *VolumeUnitRecord, err error) {
	key := encodeVuidPrefix(vuidPrefix)
	volUnit, err := v.unitTbl.Get(key)
	if err != nil {
		return nil, errors.Info(err, "get volumeUnit from volumeUnit table error").Detail(err)
	}

	return decodeVolumeUnitRecord(volUnit)
}

func (v *VolumeTable) PutVolumeUnit(vuidPrefix proto.VuidPrefix, unitRecord *VolumeUnitRecord) (err error) {
	key := encodeVuidPrefix(vuidPrefix)
	value, err := encodeVolumeUnitRecord(unitRecord)
	if err != nil {
		return err
	}

	batch := v.unitTbl.NewWriteBatch()
	defer batch.Destroy()

	indexKey := ""
	indexName := v.indexes[volumeUintDiskIDIndex].indexName
	indexKey += fmt.Sprintf(indexName+"-%d-%d", unitRecord.DiskID, vuidPrefix)
	batch.PutCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), key)
	batch.PutCF(v.unitTbl.GetCf(), key, value)
	return v.unitTbl.DoBatch(batch)
}

func (v *VolumeTable) PutVolumeUnits(units []*VolumeUnitRecord) (err error) {
	batch := v.unitTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range units {
		key := encodeVuidPrefix(unit.VuidPrefix)
		value, err := encodeVolumeUnitRecord(unit)
		if err != nil {
			return err
		}

		indexKey := ""
		indexKey += fmt.Sprintf(v.indexes["diskID"].indexName+"-%d-%d", unit.DiskID, unit.VuidPrefix)
		batch.PutCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), key)
		batch.PutCF(v.unitTbl.GetCf(), key, value)
	}

	return v.unitTbl.DoBatch(batch)
}

func (v *VolumeTable) RangeVolumeUnits(f func(unitRecord *VolumeUnitRecord)) (err error) {
	snap := v.unitTbl.NewSnapshot()
	defer v.unitTbl.ReleaseSnapshot(snap)
	iter := v.unitTbl.NewIterator(snap)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); {
		if iter.Err() != nil {
			return errors.Info(iter.Err(), "range volume units err")
		}
		info, err := decodeVolumeUnitRecord(iter.Value().Data())
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

func (v *VolumeTable) ListVolumeUnit(diskID proto.DiskID) (ret []proto.VuidPrefix, err error) {
	indexTbl := v.indexes[volumeUintDiskIDIndex].indexTbl
	indexKey := fmt.Sprintf(v.indexes[volumeUintDiskIDIndex].indexName+"-%d-", diskID)

	snap := indexTbl.NewSnapshot()
	defer indexTbl.ReleaseSnapshot(snap)
	iter := indexTbl.NewIterator(snap)
	defer iter.Close()

	for iter.Seek([]byte(indexKey)); iter.ValidForPrefix([]byte(indexKey)); iter.Next() {
		if iter.Err() != nil {
			return nil, errors.Info(iter.Err(), "volumeUnit index table iterate failed")
		}
		value := iter.Value().Data()
		vuidPrefix := decodeVuidPrefix(value)
		ret = append(ret, vuidPrefix)
		iter.Key().Free()
		iter.Value().Free()
	}
	return
}

func (v *VolumeTable) UpdateVolumeUnit(vuidPrefix proto.VuidPrefix, unitRecord *VolumeUnitRecord) (err error) {
	keyVuidPrefix := encodeVuidPrefix(vuidPrefix)
	value, err := encodeVolumeUnitRecord(unitRecord)
	if err != nil {
		return err
	}

	batch := v.unitTbl.NewWriteBatch()
	defer batch.Destroy()

	indexKey := ""
	indexName := v.indexes[volumeUintDiskIDIndex].indexName

	// remove old diskID index
	volUnit, err := v.unitTbl.Get(keyVuidPrefix)
	if err != nil {
		return errors.Info(err, "get volumeUnit from volumeUnit table error").Detail(err)
	}
	uRec, err := decodeVolumeUnitRecord(volUnit)
	if err != nil {
		return err
	}
	oldDiskID := uRec.DiskID
	oldIndexKey := ""
	oldIndexKey += fmt.Sprintf(indexName+"-%d-%d", oldDiskID, vuidPrefix)
	batch.DeleteCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(oldIndexKey))

	indexKey += fmt.Sprintf(indexName+"-%d-%d", unitRecord.DiskID, vuidPrefix)
	batch.PutCF(v.indexes[volumeUintDiskIDIndex].indexTbl.GetCf(), []byte(indexKey), keyVuidPrefix)
	batch.PutCF(v.unitTbl.GetCf(), keyVuidPrefix, value)

	return v.unitTbl.DoBatch(batch)
}

func encodeVolumeUnitRecord(info *VolumeUnitRecord) (ret []byte, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(*info)
	if err != nil {
		return nil, err
	}
	ret = buf.Bytes()
	return
}

func decodeVolumeUnitRecord(buf []byte) (ret *VolumeUnitRecord, err error) {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	err = dec.Decode(&ret)

	return
}

func encodeVuidPrefix(vuidPrefix proto.VuidPrefix) (ret []byte) {
	ret = make([]byte, 8)
	binary.BigEndian.PutUint64(ret, uint64(vuidPrefix))
	return
}

func decodeVuidPrefix(buf []byte) proto.VuidPrefix {
	key := binary.BigEndian.Uint64(buf)
	return proto.VuidPrefix(key)
}

func encodeTaskRecord(taskRec *VolumeTaskRecord) (ret []byte, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(*taskRec)
	if err != nil {
		return nil, err
	}
	ret = buf.Bytes()
	return
}

func decodeTaskRecord(buf []byte) (ret *VolumeTaskRecord, err error) {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	err = dec.Decode(&ret)
	return
}
