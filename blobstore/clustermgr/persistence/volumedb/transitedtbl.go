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
	"errors"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type TransitedTable struct {
	volTbl  kvstore.KVTable
	unitTbl kvstore.KVTable
}

func OpenTransitedTable(db kvstore.KVStore) (*TransitedTable, error) {
	if db == nil {
		return nil, errors.New("OpenVolumeTable failed: db is nil")
	}
	return &TransitedTable{
		volTbl:  db.Table(transitedVolumeCF),
		unitTbl: db.Table(transitedVolumeUnitCF),
	}, nil
}

func (t *TransitedTable) RangeVolume(f func(Record *VolumeRecord) error) (err error) {
	iter := t.volTbl.NewIterator(nil)
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

func (t *TransitedTable) PutVolumeAndVolumeUnit(vol *VolumeRecord, volumeUnitsRecs []*VolumeUnitRecord) (err error) {
	batch := t.volTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range volumeUnitsRecs {
		unitKey := encodeVuidPrefix(unit.VuidPrefix)
		uRec, err := encodeVolumeUnitRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(t.unitTbl.GetCf(), unitKey, uRec)
	}
	vid := EncodeVid(vol.Vid)
	valueVol, err := encodeVolumeRecord(vol)
	if err != nil {
		return err
	}
	batch.PutCF(t.volTbl.GetCf(), vid, valueVol)

	return t.volTbl.DoBatch(batch)
}

func (t *TransitedTable) GetVolumeUnit(vuidPrefix proto.VuidPrefix) (ret *VolumeUnitRecord, err error) {
	key := encodeVuidPrefix(vuidPrefix)
	volUnit, err := t.unitTbl.Get(key)
	if err != nil {
		return nil, err
	}

	return decodeVolumeUnitRecord(volUnit)
}

func (t *TransitedTable) PutVolumeUnits(units []*VolumeUnitRecord) (err error) {
	batch := t.unitTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range units {
		key := encodeVuidPrefix(unit.VuidPrefix)
		value, err := encodeVolumeUnitRecord(unit)
		if err != nil {
			return err
		}
		batch.PutCF(t.unitTbl.GetCf(), key, value)
	}

	return t.unitTbl.DoBatch(batch)
}

func (t *TransitedTable) DeleteVolumeAndUnits(vol *VolumeRecord, volumeUnitsRecs []*VolumeUnitRecord) error {
	batch := t.volTbl.NewWriteBatch()
	defer batch.Destroy()

	for _, unit := range volumeUnitsRecs {
		unitKey := encodeVuidPrefix(unit.VuidPrefix)
		batch.DeleteCF(t.unitTbl.GetCf(), unitKey)
	}
	vid := EncodeVid(vol.Vid)
	batch.DeleteCF(t.volTbl.GetCf(), vid)

	return t.volTbl.DoBatch(batch)
}
