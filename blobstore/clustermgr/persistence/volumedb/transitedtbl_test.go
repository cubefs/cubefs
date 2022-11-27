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

	"github.com/stretchr/testify/require"
)

var transitedTable *TransitedTable

func TestTransitedTable_PutVolumeAndVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}
}

func TestTransitedTable_RangeVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}

	i := 0
	transitedTable.RangeVolume(func(record *VolumeRecord) error {
		i++
		return nil
	})
	require.Equal(t, i, len(volumes))
}

func TestTransitedTable_GetVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}

	volumeUnit, err := transitedTable.GetVolumeUnit(volumeUnit1.VuidPrefix)
	require.NoError(t, err)
	require.Equal(t, volumeUnit1.VuidPrefix, volumeUnit.VuidPrefix)
	require.Equal(t, volumeUnit1.Epoch, volumeUnit.Epoch)
	require.Equal(t, volumeUnit1.NextEpoch, volumeUnit.NextEpoch)
	require.Equal(t, volumeUnit1.DiskID, volumeUnit.DiskID)
}

func TestTransitedTable_UpdateVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}
	for _, volumeUnit := range []*VolumeUnitRecord{volumeUnit1, volumeUnit2, volumeUnit3} {
		newVolumeUnit1 := *volumeUnit
		newVolumeUnit1.Epoch += 10
		newVolumeUnit1.NextEpoch += 10
		err := transitedTable.PutVolumeUnits([]*VolumeUnitRecord{&newVolumeUnit1})
		require.NoError(t, err)
	}

	for _, volumeUnit := range []*VolumeUnitRecord{volumeUnit1, volumeUnit2, volumeUnit3} {
		unit, err := transitedTable.GetVolumeUnit(volumeUnit.VuidPrefix)
		require.NoError(t, err)
		require.Equal(t, volumeUnit.VuidPrefix, unit.VuidPrefix)
		require.Equal(t, volumeUnit.Epoch+10, unit.Epoch)
		require.Equal(t, volumeUnit.Epoch+10, unit.NextEpoch)
	}
}

func TestTransitedTable_DeleteVolumeAndVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	for i := range volumes {
		err := transitedTable.PutVolumeAndVolumeUnit(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}

	for i := range volumes {
		err := transitedTable.DeleteVolumeAndUnits(volumes[i], volumeUnits[i])
		require.NoError(t, err)
	}
}
