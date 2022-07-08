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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var volumeTable *VolumeTable

var (
	token1 = &TokenRecord{
		Vid:        1,
		TokenID:    "127.0.0.1;1",
		ExpireTime: 12345,
	}
	token2 = &TokenRecord{
		Vid:        2,
		TokenID:    "127.0.0.1;2",
		ExpireTime: 12345,
	}
	token3 = &TokenRecord{
		Vid:        3,
		TokenID:    "127.0.0.1;2",
		ExpireTime: 12345,
	}
	tokens = []*TokenRecord{token1, token2, token3}

	volumeUnit1 = &VolumeUnitRecord{
		VuidPrefix: 4294967296,
		Epoch:      1432,
		NextEpoch:  1432,
		DiskID:     20,
		Free:       1024,
		Used:       21,
		Total:      10240,
	}
	volumeUnit2 = &VolumeUnitRecord{
		VuidPrefix: 8589934592,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     32,
		Free:       1024,
		Used:       21,
		Total:      10240,
	}
	volumeUnit3 = &VolumeUnitRecord{
		VuidPrefix: 12884901888,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     32,
		Free:       1024,
		Used:       21,
		Total:      10240,
	}
	volumeUnit4 = &VolumeUnitRecord{
		VuidPrefix: 4,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     99,
		Free:       1024,
		Used:       21,
		Total:      10240,
	}

	units = []*VolumeUnitRecord{volumeUnit1, volumeUnit2, volumeUnit3, volumeUnit4}

	volume1 = &VolumeRecord{
		Vid:            1,
		VuidPrefixs:    []proto.VuidPrefix{4294967296, 4311744512, 4328521728, 4345298944, 4362076160, 4378853376, 4395630592, 4412407808, 4429185024, 4445962240, 4462739456, 4479516672, 4496293888, 4513071104, 4529848320, 4546625536, 4563402752, 4580179968, 4596957184, 4613734400, 4630511616, 4647288832, 4664066048, 4680843264, 4697620480, 4714397696, 4731174912},
		CodeMode:       1,
		Status:         1,
		HealthScore:    0,
		Free:           1024,
		Used:           21,
		Total:          10240,
		CreateByNodeID: 1,
	}
	volume2 = &VolumeRecord{
		Vid:            2,
		VuidPrefixs:    []proto.VuidPrefix{8589934592, 8606711808, 8623489024, 8640266240, 8657043456, 8673820672, 8690597888, 8707375104, 8724152320, 8740929536, 8757706752, 8774483968, 8791261184, 8808038400, 8824815616, 8841592832, 8858370048, 8875147264, 8891924480, 8908701696, 8925478912, 8942256128, 8959033344, 8975810560, 8992587776, 9009364992, 9026142208},
		CodeMode:       1,
		Status:         1,
		HealthScore:    0,
		Free:           1024,
		Used:           21,
		Total:          10240,
		CreateByNodeID: 1,
	}
	volume3 = &VolumeRecord{
		Vid:            3,
		VuidPrefixs:    []proto.VuidPrefix{12884901888, 12901679104, 12918456320, 12935233536, 12952010752, 12968787968, 12985565184, 13002342400, 13019119616, 13035896832, 13052674048, 13069451264, 13086228480, 13103005696, 13119782912, 13136560128, 13153337344, 13170114560, 13186891776, 13203668992, 13220446208, 13237223424, 13254000640, 13270777856, 13287555072, 13304332288, 13321109504},
		CodeMode:       1,
		Status:         1,
		HealthScore:    0,
		Free:           1024,
		Used:           21,
		Total:          10240,
		CreateByNodeID: 1,
	}

	volumes = []*VolumeRecord{volume1, volume2, volume3}

	taskRecord1 = &VolumeTaskRecord{
		Vid:      1,
		TaskType: base.VolumeTaskTypeLock,
		TaskId:   uuid.New().String(),
	}

	taskRecord2 = &VolumeTaskRecord{
		Vid:      2,
		TaskType: base.VolumeTaskTypeUnlock,
		TaskId:   uuid.New().String(),
	}
)

func TestVolumeTable_PutVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeRecord(volume1)
	assert.NoError(t, err)

	vol, err := volumeTable.GetVolume(proto.Vid(1))
	assert.NoError(t, err)
	assert.Equal(t, vol.Vid, proto.Vid(1))
}

func TestVolumeTable_PutVolumes(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	recs := []*VolumeRecord{volume1, volume2, volume3}
	err := volumeTable.PutVolumeRecords(recs)
	assert.NoError(t, err)

	vol, err := volumeTable.GetVolume(proto.Vid(1))
	assert.NoError(t, err)
	assert.Equal(t, vol.Vid, proto.Vid(1))

	vol2, err := volumeTable.GetVolume(proto.Vid(2))
	assert.NoError(t, err)
	assert.Equal(t, vol2.Vid, proto.Vid(2))

	vol3, err := volumeTable.GetVolume(proto.Vid(3))
	assert.NoError(t, err)
	assert.Equal(t, vol3.Vid, proto.Vid(3))
}

func TestVolumeTable_GetVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeRecord(volume1)
	assert.NoError(t, err)
	vol, err := volumeTable.GetVolume(1)
	assert.NoError(t, err)
	assert.Equal(t, volume1, vol)

	vol, err = volumeTable.GetVolume(55)
	assert.Error(t, err)
	assert.Nil(t, vol)
}

func TestVolumeTable_PutVolumeAndVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	err := volumeTable.PutVolumeAndVolumeUnit(volumes, volumeUnits)
	assert.NoError(t, err)
}

func TestVolumeTable_PutBatch(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	tokens := []*TokenRecord{token1, token2, token3}
	err := volumeTable.PutVolumes(volumes, volumeUnits, tokens)
	assert.NoError(t, err)

	ret, err := volumeTable.ListVolumeUnit(32)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ret))

	volTokens, err := volumeTable.GetAllTokens()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(volTokens))
}

func TestVolumeTable_RangeVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	tokens := []*TokenRecord{token1, token2, token3}
	err := volumeTable.PutVolumes(volumes, volumeUnits, tokens)
	assert.NoError(t, err)

	i := 0
	volumeTable.RangeVolumeRecord(func(Record *VolumeRecord) error {
		i++
		return nil
	})
	assert.Equal(t, i, len(volumes))
}

func TestVolumeTable_ListVolumeRecord(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUnits := [][]*VolumeUnitRecord{{volumeUnit1}, {volumeUnit2}, {volumeUnit3}}
	tokens := []*TokenRecord{token1, token2, token3}
	err := volumeTable.PutVolumes(volumes, volumeUnits, tokens)
	assert.NoError(t, err)

	ret, err := volumeTable.ListVolume(5, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 3)

	ret, err = volumeTable.ListVolume(5, 2)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 1)

	ret, err = volumeTable.ListVolume(2, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 2)

	ret, err = volumeTable.ListVolume(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 1)

	ret, err = volumeTable.ListVolume(12, 44)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 0)
}

func TestTokenTable_PutToken(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutToken(1, token1)
	assert.NoError(t, err)
}

func TestTokenTable_GetToken(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutToken(1, token1)
	assert.NoError(t, err)
	token, err := volumeTable.GetToken(1)
	assert.NoError(t, err)
	assert.Equal(t, token1, token)
}

func TestTokenTable_PutBatch(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()
	err := volumeTable.PutTokens(tokens)
	assert.NoError(t, err)
}

func TestTokenTable_DeleteToken(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutTokens(tokens)
	assert.NoError(t, err)

	err = volumeTable.DeleteToken(1)
	assert.NoError(t, err)
	ret, err := volumeTable.GetAllTokens()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ret))

	err = volumeTable.PutToken(1, token1)
	assert.NoError(t, err)

	ret, err = volumeTable.GetAllTokens()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(ret))
}

func TestTokenTable_ListTokens(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutTokens(tokens)
	assert.NoError(t, err)

	ret, err := volumeTable.GetAllTokens()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(ret))
	assert.Equal(t, token1, ret[0])
}

func TestVolumeUnitTable_PutVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUintInfo := volumeUnit1
	err := volumeTable.PutVolumeUnit(volumeUintInfo.VuidPrefix, volumeUintInfo)
	assert.NoError(t, err)
}

func TestVolumeUnitTable_GetVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUintInfo := volumeUnit1
	err := volumeTable.PutVolumeUnit(volumeUintInfo.VuidPrefix, volumeUintInfo)
	assert.NoError(t, err)

	volumeUnit, err := volumeTable.GetVolumeUnit(4294967296)
	assert.NoError(t, err)
	assert.Equal(t, proto.VuidPrefix(4294967296), volumeUnit.VuidPrefix)
	assert.Equal(t, uint32(1432), volumeUnit.Epoch)
	assert.Equal(t, uint32(1432), volumeUnit.NextEpoch)
	assert.Equal(t, proto.DiskID(20), volumeUnit.DiskID)
}

func TestVolumeTable_UpdateVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	volumeUintInfo := volumeUnit1
	err := volumeTable.PutVolumeUnit(volumeUintInfo.VuidPrefix, volumeUintInfo)
	assert.NoError(t, err)
	volumeUnit2 = &VolumeUnitRecord{
		VuidPrefix: 8589934592,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     20,
		Free:       1024,
		Used:       21,
		Total:      10240,
	}
	volumeTable.PutVolumeUnit(volumeUnit2.VuidPrefix, volumeUnit2)

	volumeUnitInfo2 := volumeUnit2
	volumeUnitInfo2.DiskID = 20
	err = volumeTable.PutVolumeUnit(volumeUnitInfo2.VuidPrefix, volumeUnitInfo2)
	assert.NoError(t, err)

	ret, err := volumeTable.ListVolumeUnit(20)
	assert.NoError(t, err)
	assert.Equal(t, len(ret), 2)

	// repeat update volume unit
	err = volumeTable.UpdateVolumeUnit(volumeUnitInfo2.VuidPrefix, volumeUnitInfo2)
	assert.NoError(t, err)
	ret, err = volumeTable.ListVolumeUnit(20)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ret))

	volumeUnit2.DiskID = 45
	err = volumeTable.UpdateVolumeUnit(volumeUnit2.VuidPrefix, volumeUnit2)
	assert.NoError(t, err)

	ret, err = volumeTable.ListVolumeUnit(20)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ret))

	ret, err = volumeTable.ListVolumeUnit(45)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ret))
}

func TestVolumeUnitTable_PutBatch(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeUnits(units)
	assert.NoError(t, err)
}

func TestVolumeUnitTable_ListVolumeUnit(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeUnits(units)
	assert.NoError(t, err)

	ret, err := volumeTable.ListVolumeUnit(32)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ret))
}

func TestVolumeUnitTable_RangeVolumeUints(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeUnits(units)
	assert.NoError(t, err)

	i := 0
	volumeTable.RangeVolumeUnits(func(unitRecord *VolumeUnitRecord) {
		i++
	})
	assert.Equal(t, len(units), i)
}

func TestVolumeTable_PutVolumeAndTask(t *testing.T) {
	initVolumeDB()
	defer closeVolumeDB()

	err := volumeTable.PutVolumeAndTask(volume1, taskRecord1)
	assert.NoError(t, err)

	err = volumeTable.PutVolumeAndTask(volume2, taskRecord2)
	assert.NoError(t, err)

	var taskRecords []*VolumeTaskRecord
	volumeTable.ListTaskRecords(func(rec *VolumeTaskRecord) bool {
		taskRecords = append(taskRecords, rec)
		return true
	})
	assert.Equal(t, 2, len(taskRecords))
	assert.Equal(t, taskRecord1, taskRecords[0])
	assert.Equal(t, taskRecord2, taskRecords[1])

	err = volumeTable.PutVolumeAndTask(nil, nil)
	assert.NoError(t, err)
	err = volumeTable.PutVolumeAndTask(volume1, nil)
	assert.NoError(t, err)
	err = volumeTable.PutVolumeAndTask(nil, taskRecord1)
	assert.NoError(t, err)
}
