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
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

var (
	volumeDB     *VolumeDB
	volumeDBPath = "/tmp/volumedb"
)

func initVolumeDB() {
	volumeDBPath += strconv.Itoa(rand.Intn(20000))

	var err error
	volumeDB, err = Open(volumeDBPath, false)
	if err != nil {
		log.Error("open db error")
	}

	volumeTable, err = OpenVolumeTable(volumeDB)
	if err != nil {
		log.Error("open volume table error")
	}

	transitedTable, err = OpenTransitedTable(volumeDB)
	if err != nil {
		log.Error("open transited table error")
	}
}

func closeVolumeDB() {
	volumeDB.Close()
	os.RemoveAll(volumeDBPath)
}

func TestVolumeDB(t *testing.T) {
	volumeDBPath = ""
	_, err := Open(volumeDBPath, false)
	require.Error(t, err)
}

func TestVolumeDB_GetAllCfNames(t *testing.T) {
	volumeDBPath = ""
	db, err := Open(volumeDBPath, false)
	require.Error(t, err)
	db.GetAllCfNames()

	_, err = OpenVolumeTable(nil)
	require.Error(t, err)
}
