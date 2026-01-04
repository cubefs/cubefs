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

package core

import (
	"context"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

/*
 * ${diskRoot}/
 * 		- .sys/
 * 		- .trash/
 * 		- data/
 * 		- meta/
			- superblock/
*/

const (
	formatConfigFile    = ".format.json"
	formatConfigFileTmp = ".format.json.tmp"

	_systemMeta      = ".sys"
	_trashPrefix     = ".trash"
	_dataSpacePrefix = "data"
	_metaSpacePrefix = "meta"

	FormatMetaTypeV1       = "fs"
	formatInfoCheckSumPoly = uint32(0xebf0ace5)
)

var (
	ErrFormatInfoCheckSum = errors.New("format info check sum error")
	ErrInvalidPathPrefix  = errors.New("invalid path prefix")
	ErrFormatV2CrcIsEmpty = errors.New("format info version 2 crc is empty")
)

// FormatInfoProtectedFieldInitVersion (not contains NodeID and NodeCtime)
type FormatInfoProtectedField struct {
	DiskID  proto.DiskID `json:"diskid"`
	Version uint8        `json:"version"`
	Ctime   int64        `json:"ctime"`
	Format  string       `json:"format"`
}

type FormatInfoV1 struct {
	FormatInfoProtectedField
	CheckSum uint32 `json:"check_sum"`
}

// version 2: with v1 version and NodeID, NodeCtime
type FormatInfoProtectedFieldV2 struct {
	FormatInfoV1
	NodeID    proto.NodeID `json:"nodeid"`
	NodeCtime int64        `json:"node_ctime"`
}

type FormatInfo struct {
	FormatInfoProtectedFieldV2
	CheckSumV2 uint32 `json:"check_sum_v2"`
}

func sysRootPath(diskRoot string) (path string) {
	return filepath.Join(diskRoot, _systemMeta)
}

func metaRootPath(diskRoot string) (path string) {
	return filepath.Join(diskRoot, _metaSpacePrefix)
}

func dataRootPath(diskRoot string) (path string) {
	return filepath.Join(diskRoot, _dataSpacePrefix)
}

func SysTrashPath(diskRoot string) (path string) {
	return filepath.Join(diskRoot, _trashPrefix)
}

func GetMetaPath(diskRoot string, metaRootPrefix string) (path string) {
	path = filepath.Join(metaRootPath(diskRoot), "superblock")
	// Metadata can be put in a unified location
	path = filepath.Join(metaRootPrefix, path)
	return path
}

func GetDataPath(diskRoot string) (path string) {
	return dataRootPath(diskRoot)
}

func EnsureDiskArea(diskpath string, rootPrefix string) (err error) {
	if _, err = os.Stat(diskpath); err != nil {
		return err
	}

	// ensure system(dir)
	err = os.MkdirAll(sysRootPath(diskpath), 0o755)
	if err != nil {
		return err
	}

	// ensure meta area(dir)
	err = os.MkdirAll(GetMetaPath(diskpath, rootPrefix), 0o755)
	if err != nil {
		return err
	}

	// ensure data area(dir)
	err = os.MkdirAll(GetDataPath(diskpath), 0o755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(SysTrashPath(diskpath), 0o755)
	if err != nil {
		return err
	}

	return nil
}

func SaveDiskFormatInfo(ctx context.Context, diskPath string, formatInfo *FormatInfo) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("save format info, path:%v, info:%v", diskPath, formatInfo)

	configFile := filepath.Join(sysRootPath(diskPath), formatConfigFile)
	configFileTemp := filepath.Join(sysRootPath(diskPath), formatConfigFileTmp)

	file, err := OpenFile(configFileTemp, true)
	if err != nil {
		span.Errorf("Failed create file:%s, err:%v", configFileTemp, err)
		return err
	}

	// Marshal and write to disk.
	formatBytes, err := json.Marshal(formatInfo)
	if err != nil {
		span.Errorf("Failed marshal, err:%v", err)
		return err
	}

	// write
	_, err = file.Write(formatBytes)
	if err != nil {
		span.Errorf("Failed write. err:%v", err)
		return nil
	}
	file.Close()

	// rename, replace save with new file
	err = os.Rename(configFileTemp, configFile)
	if err != nil {
		span.Errorf("Failed rename, err:%v", err)
		return err
	}

	span.Infof("save format info success")
	return nil
}

func ReadFormatInfo(ctx context.Context, diskRootPath string) (info *FormatInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)

	configFile := filepath.Join(sysRootPath(diskRootPath), formatConfigFile)
	buf, err := os.ReadFile(configFile)
	if err != nil {
		span.Errorf("Failed read file:%v, err:%v", configFile, err)
		return nil, err
	}

	info = &FormatInfo{}
	if err = json.Unmarshal(buf, info); err != nil {
		span.Errorf("Failed unmarshal, err:%v", err)
		return nil, err
	}

	if info.CheckSumV2 == 0 {
		return info, ErrFormatV2CrcIsEmpty
	}

	if err = info.Verify(); err != nil {
		span.Errorf("Failed check format info crc, err:%v", err)
		return nil, err
	}

	return info, nil
}

func IsFormatConfigExist(diskRootPath string) (bool, error) {
	configFile := filepath.Join(sysRootPath(diskRootPath), formatConfigFile)
	return base.IsFileExists(configFile)
}

func (fi *FormatInfo) Verify() error {
	if fi.CheckSumV2 == 0 {
		return ErrFormatV2CrcIsEmpty
	}

	checkSum, err := fi.calCheckSumV2()
	if err != nil {
		return err
	}

	if checkSum != fi.CheckSumV2 {
		return ErrFormatInfoCheckSum
	}
	return nil
}

func (fi *FormatInfo) VerifyV1() error {
	checkSum, err := fi.calCheckSumV1()
	if err != nil {
		return err
	}

	if checkSum != fi.CheckSum {
		return ErrFormatInfoCheckSum
	}
	return nil
}

func (fi *FormatInfo) CalCheckSum() error {
	checkSum, err := fi.calCheckSumV2()
	if err != nil {
		return err
	}

	fi.CheckSumV2 = checkSum
	return nil
}

func (fi *FormatInfo) calCheckSumV2() (uint32, error) {
	crc := crc32.New(crc32.MakeTable(formatInfoCheckSumPoly))

	b, err := json.Marshal(fi.FormatInfoProtectedFieldV2)
	if err != nil {
		return proto.InvalidCrc32, err
	}

	_, err = crc.Write(b)
	if err != nil {
		return proto.InvalidCrc32, err
	}

	return crc.Sum32(), nil
}

func (fi *FormatInfo) calCheckSumV1() (uint32, error) {
	crc := crc32.New(crc32.MakeTable(formatInfoCheckSumPoly))

	b, err := json.Marshal(fi.FormatInfoProtectedField)
	if err != nil {
		return proto.InvalidCrc32, err
	}

	_, err = crc.Write(b)
	if err != nil {
		return proto.InvalidCrc32, err
	}

	return crc.Sum32(), nil
}
