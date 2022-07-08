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
	"testing"

	"github.com/stretchr/testify/assert"
)

var transitedTable *TransitedTable

func TestTransitedTable_PutVolumeAndVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}
}

func TestTransitedTable_RangeVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}

	i := 0
	transitedTable.RangeVolume(func(record *VolumeRecord) error {
		i++
		return nil
	})
	assert.Equal(t, i, len(volumes))
}

func TestTransitedTable_GetVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}

	volumeUnit, err := transitedTable.GetVolumeUnit(volumeUnit1.VuidPrefix)
	assert.NoError(t, err)
	assert.Equal(t, volumeUnit1.VuidPrefix, volumeUnit.VuidPrefix)
	assert.Equal(t, volumeUnit1.Epoch, volumeUnit.Epoch)
	assert.Equal(t, volumeUnit1.NextEpoch, volumeUnit.NextEpoch)
	assert.Equal(t, volumeUnit1.DiskID, volumeUnit.DiskID)
}

func TestTransitedTable_UpdateVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}
	for _, volumeUnit := range []*VolumeUnitRecord{volumeUnit1, volumeUnit2, volumeUnit3} {
		newVolumeUnit1 := *volumeUnit
		newVolumeUnit1.Epoch += 10
		newVolumeUnit1.NextEpoch += 10
		err := transitedTable.PutVolumeUnits([]*VolumeUnitRecord{&newVolumeUnit1})
		assert.NoError(t, err)
	}

	for _, volumeUnit := range []*VolumeUnitRecord{volumeUnit1, volumeUnit2, volumeUnit3} {
		unit, err := transitedTable.GetVolumeUnit(volumeUnit.VuidPrefix)
		assert.NoError(t, err)
		assert.Equal(t, volumeUnit.VuidPrefix, unit.VuidPrefix)
		assert.Equal(t, volumeUnit.Epoch+10, unit.Epoch)
		assert.Equal(t, volumeUnit.Epoch+10, unit.NextEpoch)
	}
}

func TestTransitedTable_DeleteVolumeAndVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}

	for i := range volumes {
		err := transitedTable.DeleteVolumeAndUnits(volumes[i], volumeUnits[i])
		assert.NoError(t, err)
	}
}
