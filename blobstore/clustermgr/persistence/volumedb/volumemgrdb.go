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
	"encoding/binary"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// column family name definition
var (
	volumeCF                = "volume"
	volumeUnitCF            = "volume_unit"
	volumeTokenCF           = "volume_token"
	volumeTaskCF            = "volume_task"
	transitedVolumeCF       = "transited_volume"
	transitedVolumeUnitCF   = "transited_volume_unit"
	volumeUnitDiskIDIndexCF = "volumeUnit_DiskID"

	volumeCfs = []string{
		volumeCF,
		volumeUnitCF,
		volumeTokenCF,
		volumeTaskCF,
		transitedVolumeCF,
		transitedVolumeUnitCF,
		volumeUnitDiskIDIndexCF,
	}
)

type VolumeDB struct {
	kvstore.KVStore
}

func Open(path string, isSync bool, dbOpts ...kvstore.DbOptions) (*VolumeDB, error) {
	db, err := kvstore.OpenDBWithCF(path, isSync, volumeCfs, dbOpts...)
	if err != nil {
		log.Errorf("create volumeTbl error:%v", err)
		return nil, err
	}

	return &VolumeDB{db}, nil
}

func EncodeVid(vid proto.Vid) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(vid))
	return key
}

func DecodeVid(b []byte) proto.Vid {
	key := binary.BigEndian.Uint32(b)
	return proto.Vid(key)
}

func (v *VolumeDB) GetAllCfNames() []string {
	return volumeCfs
}
