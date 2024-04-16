// Copyright 2023 The CubeFS Authors.
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

package storage_test

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func getTestPathExtentStore() (string, func(), error) {
	dir, err := os.MkdirTemp(os.TempDir(), "cfs_storage_extentstore_")
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s/extents", dir), func() { os.RemoveAll(dir) }, nil
}

func extentStoreNormalRwTest(t *testing.T, s *storage.ExtentStore, id uint64) {
	data := []byte(dataStr)
	crc := crc32.ChecksumIEEE(data)
	// append write
	_, err := s.Write(id, 0, int64(len(data)), data, crc, storage.AppendWriteType, true, false)
	require.NoError(t, err)
	actualCrc, err := s.Read(id, 0, int64(len(data)), data, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// random write
	_, err = s.Write(id, 0, int64(len(data)), data, crc, storage.RandomWriteType, true, false)
	require.NoError(t, err)
	actualCrc, err = s.Read(id, 0, int64(len(data)), data, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	// TODO: append random write
	require.NotEqualValues(t, s.GetStoreUsedSize(), 0)
}

func extentStoreMarkDeleteTiny(t *testing.T, s *storage.ExtentStore, id uint64, offset int64, size int64) {
	err := s.MarkDelete(id, offset, int64(size))
	require.NoError(t, err)
	eds, err := s.GetHasDeleteTinyRecords()
	require.NoError(t, err)
	found := false
	for _, ed := range eds {
		if ed.ExtentID == id && ed.Offset == uint64(offset) {
			found = true
		}
	}
	require.Equal(t, found, true)
}

func extentMarkDeleteNormalTest(t *testing.T, s *storage.ExtentStore, id uint64) {
	ei, err := s.Watermark(id)
	require.NoError(t, err)
	err = s.MarkDelete(id, 0, int64(ei.Size))
	require.NoError(t, err)
	require.Equal(t, s.IsDeletedNormalExtent(id), true)
	eds, err := s.GetHasDeleteExtent()
	require.NoError(t, err)
	found := false
	for _, ed := range eds {
		if ed.ExtentID == id && ed.Offset == 0 {
			found = true
		}
	}
	require.Equal(t, found, true)
}

func extentMarkDeleteTinyTest(t *testing.T, s *storage.ExtentStore, id uint64) {
	size, err := s.GetTinyExtentOffset(id)
	require.NoError(t, err)
	data := []byte(dataStr)
	require.NotEqualValues(t, size, 0)
	// write second file to extent
	crc := crc32.ChecksumIEEE(data)
	_, err = s.Write(id, size, int64(len(data)), data, crc, storage.AppendWriteType, true, false)
	require.NoError(t, err)
	// mark delete first file
	extentStoreMarkDeleteTiny(t, s, id, 0, size)
	newSize, err := s.GetTinyExtentOffset(id)
	require.NoError(t, err)
	// mark delete second file
	extentStoreMarkDeleteTiny(t, s, id, size, newSize-size)
}

func extentStoreMarkDeleteTest(t *testing.T, s *storage.ExtentStore, id uint64) {
	if !s.HasExtent(id) {
		t.Errorf("target extent doesn't exits")
		return
	}
	if storage.IsTinyExtent(id) {
		extentMarkDeleteTinyTest(t, s, id)
		return
	}
	extentMarkDeleteNormalTest(t, s, id)
}

func extentStoreSizeTest(t *testing.T, s *storage.ExtentStore) {
	maxId, size := s.GetMaxExtentIDAndPartitionSize()
	total := s.StoreSizeExtentID(maxId)
	require.EqualValues(t, total, size)
}

func extentStoreLogicalTest(t *testing.T, s *storage.ExtentStore) {
	normalId, err := s.NextExtentID()
	require.NoError(t, err)
	err = s.Create(normalId)
	require.NoError(t, err)
	s.SendToAvailableTinyExtentC(testTinyExtentID)
	tinyId, err := s.GetAvailableTinyExtent()
	require.NoError(t, err)
	ids := []uint64{
		normalId,
		tinyId,
	}
	for _, id := range ids {
		extentStoreNormalRwTest(t, s, id)
		extentStoreSizeTest(t, s)
		extentStoreMarkDeleteTest(t, s, id)
		extentStoreSizeTest(t, s)
	}
}

func reopenExtentStoreTest(t *testing.T, dpType int) {
	path, clean, err := getTestPathExtentStore()
	require.NoError(t, err)
	defer clean()
	s, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, true)
	require.NoError(t, err)
	defer s.Close()
	id, err := s.NextExtentID()
	require.NoError(t, err)
	err = s.Create(id)
	require.NoError(t, err)
	data := []byte(dataStr)
	crc := crc32.ChecksumIEEE(data)
	// write some data
	_, err = s.Write(id, 0, int64(len(data)), data, crc, storage.AppendWriteType, true, false)
	require.NoError(t, err)
	firstSnap, err := s.SnapShot()
	require.NoError(t, err)
	s.Close()
	newStor, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, false)
	require.NoError(t, err)
	defer newStor.Close()
	// read data
	actualCrc, err := newStor.Read(id, 0, int64(len(data)), data, false)
	require.NoError(t, err)
	require.EqualValues(t, crc, actualCrc)
	secondSnap, err := newStor.SnapShot()
	require.NoError(t, err)
	require.EqualValues(t, len(firstSnap), len(secondSnap))
	// check snapshot
	firstSnapNames := make(map[string]interface{})
	for _, file := range firstSnap {
		firstSnapNames[file.Name] = 1
	}
	secondSnapNames := make(map[string]interface{})
	for _, file := range secondSnap {
		secondSnapNames[file.Name] = 1
	}
	for k := range firstSnapNames {
		require.NotNil(t, secondSnapNames[k])
	}
	for k := range secondSnapNames {
		require.NotNil(t, firstSnapNames[k])
	}
}

func staleExtentStoreTest(t *testing.T, dpType int) {
	path, clean, err := getTestPathExtentStore()
	extDirName := filepath.Base(path)
	require.NoError(t, err)
	defer clean()
	s, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, true)
	require.NoError(t, err)
	id, err := s.NextExtentID()
	require.NoError(t, err)
	err = s.Create(id)
	require.NoError(t, err)
	s.Close()

	// reopen1
	newS1, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, true)
	require.NoError(t, err)
	fileList, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)

	staleCount := 0
	for _, f := range fileList {
		fname := filepath.Base(f.Name())
		if strings.HasPrefix(fname, extDirName) && filepath.Ext(fname) == storage.StaleExtStoreBackupSuffix {
			staleCount++
		}
	}
	require.Equal(t, 1, staleCount)
	newS1.Close()

	// reopen2
	newS2, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, true)
	require.NoError(t, err)
	fileList, err = os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)

	staleCount = 0
	for _, f := range fileList {
		fname := filepath.Base(f.Name())
		if strings.HasPrefix(fname, extDirName) && filepath.Ext(f.Name()) == storage.StaleExtStoreBackupSuffix {
			staleCount++
		}
	}
	require.Equal(t, 2, staleCount)

	defer newS2.Close()
}

func ExtentStoreTest(t *testing.T, dpType int) {
	path, clean, err := getTestPathExtentStore()
	require.NoError(t, err)
	defer clean()
	s, err := storage.NewExtentStore(path, 0, 1*util.GB, dpType, true)
	require.NoError(t, err)
	defer s.Close()
	extentStoreLogicalTest(t, s)
	reopenExtentStoreTest(t, dpType)
	staleExtentStoreTest(t, dpType)
}

func TestExtentStores(t *testing.T) {
	dpTypes := []int{
		proto.PartitionTypeNormal,
		proto.PartitionTypePreLoad,
		proto.PartitionTypeCache,
	}
	for _, ty := range dpTypes {
		ExtentStoreTest(t, ty)
	}
}
