// Copyright 2018 The Chubao Authors.
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

package metanode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	snapshotDir     = "snapshot"
	snapshotDirTmp  = ".snapshot"
	snapshotBackup  = ".snapshot_backup"
	inodeFile       = "inode"
	dentryFile      = "dentry"
	extendFile      = "extend"
	multipartFile   = "multipart"
	applyIDFile     = "apply"
	SnapshotSign    = ".sign"
	metadataFile    = "meta"
	metadataFileTmp = ".meta"
)

func checksumFileName(originName string) string {
	return ".checksum." + originName
}

func (mp *metaPartition) loadChecksum(rootDir string, fileName string, mustExist bool) (sum []byte, err error) {
	checksumFile := path.Join(rootDir, fileName)
	sum, err = readChecksumFromFile(checksumFile)
	if err != nil {
		if os.IsNotExist(err) && !mustExist {
			log.LogInfof("LoadChecksumFile: checksum file %s not found", fileName)
			err = nil
			return
		} else {
			log.LogErrorf("LoadChecksumFile: failed to load checksum file %s, %s", fileName, err)
			return
		}
	} else {
		log.LogInfof("loadChecksumFile: read checksum success, checksum value %v", sum)
		return
	}
}

func (mp *metaPartition) loadMetadata() (err error) {
	metaFile := path.Join(mp.config.RootDir, metadataFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadMetadata] OpenFile: %s", err.Error())
		return
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadMetadata: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	//io proc
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.NewErrorf("[loadMetadata]: ReadFile %v, data: %s", err, string(data))
		return
	}

	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.NewErrorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}

	if mConf.checkMeta() != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.VolName = mConf.VolName
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	mp.config.Cursor = mp.config.Start

	log.LogInfof("loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) loadInode(rootDir string) (err error) {
	var numInodes uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numInodes)
		}
	}()
	filename := path.Join(rootDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadInode: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	reader := io.Reader(fp)
	//loadChecksum and doSumCheck (if exist)
	sumLoaded, err := mp.loadChecksum(rootDir, checksumFileName(inodeFile), false)
	if err != nil {
		return errors.NewErrorf("[loadInode] loadChecksum: %s", err.Error())
	}
	if sumLoaded != nil {
		doSumCheck := func(want []byte, hashFn hash.Hash) {
			if err == nil {
				if actual := hashFn.Sum(nil); !bytes.Equal(actual, want) {
					log.LogErrorf("loadInode: checksum not equal, want(%v), actual(%v)", want, actual)
					err = errors.NewErrorf("[loadInode] Checksum failed")
				}
			}
		}
		crc32Sign := crc32.NewIEEE()
		reader = io.TeeReader(reader, crc32Sign)
		defer doSumCheck(sumLoaded, crc32Sign)
	}

	//io proc
	reader = io.Reader(bufio.NewReaderSize(reader, 4*1024*1024))
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)
		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadInode] ReadBody: %s", err.Error())
			return
		}
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(inoBuf); err != nil {
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return
		}
		mp.fsmCreateInode(ino)
		mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		numInodes += 1
	}
}

// Load dentry from the dentry snapshot.
func (mp *metaPartition) loadDentry(rootDir string) (err error) {
	var numDentries uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadDentry: load complete: partitionID(%v) volume(%v) numDentries(%v)",
				mp.config.PartitionId, mp.config.VolName, numDentries)
		}
	}()
	filename := path.Join(rootDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		} else {
			err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
			return
		}
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadDentry: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	fpStat, err := fp.Stat()
	if err != nil {
		err = errors.NewErrorf("[loadDentry] Stat: %s", err.Error())
		return
	}

	reader := io.Reader(fp)

	//loadChecksum and doSumCheck (if exist)
	sumLoaded, err := mp.loadChecksum(rootDir, checksumFileName(dentryFile), false)
	if err != nil {
		return errors.NewErrorf("[loadDentry] loadChecksum: %s", err.Error())
	}
	if sumLoaded != nil {
		doSumCheck := func(want []byte, hashFn hash.Hash) {
			if err == nil {
				if actual := hashFn.Sum(nil); !bytes.Equal(actual, want) {
					log.LogErrorf("loadDentry: checksum not equal, want(%v), actual(%v)", want, actual)
					err = errors.NewErrorf("[loadDentry] Checksum failed")
				}
			}
		}
		crc32Sign := crc32.NewIEEE()
		reader = io.TeeReader(reader, crc32Sign)
		defer doSumCheck(sumLoaded, crc32Sign)
	}

	//io proc
	reader = bufio.NewReaderSize(reader, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(dentryBuf)
		if int64(length) > fpStat.Size() {
			log.LogErrorf("loadDentry: length too large")
			return errors.NewErrorf("[loadDentry] unexpected length, length(%d), filename(%s)",
				length, filename)
		}
		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDentry] ReadBody: %s", err.Error())
			return
		}
		dentry := &Dentry{}
		if err = dentry.Unmarshal(dentryBuf); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if status := mp.fsmCreateDentry(dentry, true); status != proto.OpOk {
			err = errors.NewErrorf("[loadDentry] createDentry dentry: %v, resp code: %d", dentry, status)
			return
		}
		numDentries += 1
	}
}

func (mp *metaPartition) loadExtend(rootDir string) (err error) {
	filename := path.Join(rootDir, extendFile)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadExtend: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	fpStat, err := fp.Stat()
	if err != nil {
		return err
	}

	reader := io.Reader(fp)

	//loadChecksum and doSumCheck (if exist)
	sumLoaded, err := mp.loadChecksum(rootDir, checksumFileName(extendFile), false)
	if err != nil {
		return errors.NewErrorf("[loadExtend] loadChecksum: %s", err.Error())
	}
	if sumLoaded != nil {
		doSumCheck := func(want []byte, hashFn hash.Hash) {
			if err == nil {
				if actual := hashFn.Sum(nil); !bytes.Equal(actual, want) {
					log.LogErrorf("loadExtend: checksum not equal, want(%v), actual(%v)", want, actual)
					err = errors.NewErrorf("[loadExtend] Checksum failed")
				}
			}
		}
		crc32Sign := crc32.NewIEEE()
		reader = io.TeeReader(reader, crc32Sign)
		defer doSumCheck(sumLoaded, crc32Sign)
	}

	//io proc
	bufReader := bufio.NewReader(reader)
	// read number of extends
	var numExtends uint64
	numExtends, err = binary.ReadUvarint(bufReader)
	if err != nil {
		log.LogErrorf("loadExtend: failed to load numExtends: %s", err.Error())
		return errors.NewErrorf("[loadDentry] ReadUvarint: %s", err.Error())
	}
	xattrBuf := make([]byte, 4096)
	for i := uint64(0); i < numExtends; i++ {
		// read length
		var numBytes uint64
		if numBytes, err = binary.ReadUvarint(bufReader); err != nil {
			log.LogErrorf("loadExtend: failed to load numBytes: %s", err.Error())
			return errors.NewErrorf("[loadDentry] ReadUvarint: %s", err.Error())
		}
		if numBytes > uint64(fpStat.Size()) {
			log.LogErrorf("loadExtend: numBytes too large")
			return errors.NewErrorf("[loadExtend] unexpected numBytes, numBytes(%d), filename(%s)",
				numBytes, filename)
		}
		if cap(xattrBuf) >= int(numBytes) {
			xattrBuf = xattrBuf[:numBytes]
		} else {
			xattrBuf = make([]byte, numBytes)
		}
		if _, err = io.ReadFull(bufReader, xattrBuf); err != nil {
			log.LogErrorf("loadExtend: failed to load extent: %s", err.Error())
			return errors.NewErrorf("[loadExtend]  ReadFull: %s", err.Error())
		}
		var extend *Extend
		if extend, err = NewExtendFromBytes(xattrBuf); err != nil {
			return err
		}
		log.LogDebugf("loadExtend: new extend from bytes: partitionID（%v) volume(%v) inode(%v)",
			mp.config.PartitionId, mp.config.VolName, extend.inode)
		_ = mp.fsmSetXAttr(extend)
	}
	log.LogInfof("loadExtend: load complete: partitionID(%v) volume(%v) numExtends(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, numExtends, filename)
	return nil
}

func (mp *metaPartition) loadMultipart(rootDir string) (err error) {
	filename := path.Join(rootDir, multipartFile)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadMultipart: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	fpStat, err := fp.Stat()
	if err != nil {
		return err
	}

	//loadChecksum and doSumCheck (if exist)
	sumLoaded, err := mp.loadChecksum(rootDir, checksumFileName(multipartFile), false)
	if err != nil {
		return errors.NewErrorf("[loadMultipart] loadChecksum: %s", err.Error())
	}
	reader := io.Reader(fp)
	if sumLoaded != nil {
		doSumCheck := func(want []byte, hashFn hash.Hash) {
			if err == nil {
				if actual := hashFn.Sum(nil); !bytes.Equal(actual, want) {
					log.LogErrorf("loadMultipart: checksum not equal, want(%v), actual(%v)", want, actual)
					err = errors.NewErrorf("[loadMultipart] Checksum failed")
				}
			}
		}
		crc32Sign := crc32.NewIEEE()
		reader = io.TeeReader(reader, crc32Sign)
		defer doSumCheck(sumLoaded, crc32Sign)
	}

	//io proc
	bufReader := bufio.NewReader(reader)
	// read number of extends
	var numMultiparts uint64
	numMultiparts, err = binary.ReadUvarint(bufReader)
	if err != nil {
		log.LogErrorf("loadMultipart: failed to load numMultiparts: %s", err.Error())
		return errors.NewErrorf("[loadMultipart] ReadUvarint: %s", err.Error())
	}
	multipartBuf := make([]byte, 4096)
	for i := uint64(0); i < numMultiparts; i++ {
		// read length
		var numBytes uint64
		if numBytes, err = binary.ReadUvarint(bufReader); err != nil {
			log.LogErrorf("loadMultipart: failed to load numBytes: %s", err.Error())
			return errors.NewErrorf("[loadMultipart]  ReadUvarint: %s", err.Error())
		}

		if numBytes > uint64(fpStat.Size()) {
			log.LogErrorf("loadMultipart: numBytes too large")
			return errors.NewErrorf("[loadMultipart] unexpected numBytes, numBytes(%d), filename(%s)",
				numBytes, filename)
		}
		if cap(multipartBuf) >= int(numBytes) {
			multipartBuf = multipartBuf[:numBytes]
		} else {
			multipartBuf = make([]byte, numBytes)
		}
		if _, err = io.ReadFull(bufReader, multipartBuf); err != nil {
			log.LogErrorf("loadMultipart: failed to load multipart: %s", err.Error())
			return errors.NewErrorf("[loadMultipart]  ReadFull: %s", err.Error())
		}
		var multipart *Multipart
		multipart = MultipartFromBytes(multipartBuf)
		log.LogDebugf("loadMultipart: create multipart from bytes: partitionID（%v) multipartID(%v)", mp.config.PartitionId, multipart.id)
		mp.fsmCreateMultipart(multipart)
	}
	log.LogInfof("loadMultipart: load complete: partitionID(%v) numMultiparts(%v) filename(%v)",
		mp.config.PartitionId, numMultiparts, filename)
	return nil
}

func (mp *metaPartition) loadApplyID(rootDir string) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	//fp close safety
	closeFp := func(fp *os.File) {
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("loadApplyID: fp.Close %s", closeErr.Error())
		}
	}
	defer closeFp(fp)

	//loadChecksum and doSumCheck (if exist)
	sumLoaded, err := mp.loadChecksum(rootDir, checksumFileName(applyIDFile), false)
	if err != nil {
		return errors.NewErrorf("[loadApplyID] loadChecksum: %s", err.Error())
	}
	reader := io.Reader(fp)
	if sumLoaded != nil {
		doSumCheck := func(want []byte, hashFn hash.Hash) {
			if err == nil {
				if actual := hashFn.Sum(nil); !bytes.Equal(actual, want) {
					log.LogErrorf("loadApplyID: checksum not equal, want(%v), actual(%v)", want, actual)
					err = errors.NewErrorf("[loadApplyID] Checksum failed")
				}
			}
		}
		crc32Sign := crc32.NewIEEE()
		reader = io.TeeReader(reader, crc32Sign)
		defer doSumCheck(sumLoaded, crc32Sign)
	}

	//io proc
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadApplyID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyID]: ApplyID is empty")
		return
	}
	var cursor uint64
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%d", &mp.applyID, &cursor)
	} else {
		_, err = fmt.Sscanf(string(data), "%d", &mp.applyID)
	}
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	if cursor > atomic.LoadUint64(&mp.config.Cursor) {
		atomic.StoreUint64(&mp.config.Cursor, cursor)
	}
	log.LogInfof("loadApplyID: load complete: partitionID(%v) volume(%v) applyID(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, filename)
	return
}

func (mp *metaPartition) persistChecksum(sum []byte, root, fileName string) (err error) {
	tmpName := ".tmp" + fileName
	tmpFile := path.Join(root, tmpName)
	if err = writeChecksumToFile(tmpFile, sum); err != nil {
		log.LogErrorf("persistChecksum: failed to persist checksum to tmpFile %s: %s", tmpFile, err)
		return
	}
	defer func() {
		if rmErr := os.Remove(tmpFile); rmErr != nil && !os.IsNotExist(rmErr) {
			log.LogErrorf("persistChecksum: failed to delete checksum tmpFile %s: %s", tmpFile, rmErr)
		}
	}()
	checksumFile := path.Join(root, fileName)
	err = os.Rename(tmpFile, checksumFile)
	if err != nil {
		log.LogErrorf("persistChecksum: failed to rename tmpFile %s to checksumFile %s", tmpFile, checksumFile)
	} else {
		log.LogInfof("persistChecksum: create checksumFile %s success", checksumFile)
	}
	return err
}

func (mp *metaPartition) persistMetadata() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[persistMetadata]->%s", err.Error())
		return
	}

	if err = os.MkdirAll(mp.config.RootDir, 0755); err != nil {
		err = errors.NewErrorf("[persistMetadata] MkdirAll: %s", err.Error())
		return err
	}

	filename := path.Join(mp.config.RootDir, metadataFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("persistMetadata: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[persistMetadata] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("persistMetadata: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[persistMetadata] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)
	// TODO Unhandled rm errors
	defer os.Remove(filename)

	data, err := json.Marshal(mp.config)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	if err = os.Rename(filename, path.Join(mp.config.RootDir, metadataFile)); err != nil {
		return
	}
	log.LogInfof("persistMetata: persist complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) storeApplyID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("storeApplyID: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[storeApplyID] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("storeApplyID: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[storeApplyID] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)

	crc32Sign := crc32.NewIEEE()
	writer := io.MultiWriter(fp, crc32Sign)
	//io proc
	data := []byte(fmt.Sprintf("%d|%d", sm.applyIndex, atomic.LoadUint64(&mp.config.Cursor)))
	if _, err = writer.Write(data); err != nil {
		return
	}

	//persist checksum file if success
	err = mp.persistChecksum(crc32Sign.Sum(nil), rootDir, checksumFileName(applyIDFile))
	if err != nil {
		log.LogErrorf("storeApplyID: failed to persist applyId checksum: %s", err.Error())
		return
	}

	crc := crc32Sign.Sum32()
	log.LogInfof("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex, crc)
	return
}

func (mp *metaPartition) storeInode(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("storeInode: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[storeInode] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("storeInode: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[storeInode] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)

	crc32Sign := crc32.NewIEEE()
	writer := io.MultiWriter(fp, crc32Sign)

	//io proc
	var data []byte
	lenBuf := make([]byte, 4)
	sm.inodeTree.Ascend(func(i BtreeItem) bool {
		ino := i.(*Inode)
		if data, err = ino.Marshal(); err != nil {
			return false
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = writer.Write(lenBuf); err != nil {
			return false
		}
		if _, err = writer.Write(data); err != nil {
			return false
		}
		return true
	})

	//persist checksum file if success
	err = mp.persistChecksum(crc32Sign.Sum(nil), rootDir, checksumFileName(inodeFile))
	if err != nil {
		log.LogErrorf("storeInode: failed to persist inode checksum: %s", err.Error())
		return
	}

	crc = crc32Sign.Sum32()
	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), crc)
	return
}

func (mp *metaPartition) storeDentry(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("storeDentry: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[storeDentry] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("storeDentry: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[storeDentry] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)

	crc32Sign := crc32.NewIEEE()
	writer := io.MultiWriter(fp, crc32Sign)

	//io proc
	var data []byte
	lenBuf := make([]byte, 4)
	sm.dentryTree.Ascend(func(i BtreeItem) bool {
		dentry := i.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = writer.Write(lenBuf); err != nil {
			return false
		}
		if _, err = writer.Write(data); err != nil {
			return false
		}
		return true
	})

	//persist checksum file if success
	err = mp.persistChecksum(crc32Sign.Sum(nil), rootDir, checksumFileName(dentryFile))
	if err != nil {
		log.LogErrorf("storeDentry: failed to persist dentry checksum: %s", err.Error())
		return
	}

	crc = crc32Sign.Sum32()
	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), crc)
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var extendTree = sm.extendTree
	var filename = path.Join(rootDir, extendFile)
	var fp *os.File
	fp, err = os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("storeExtend: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[storeExtend] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("storeExtend: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[storeExtend] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)

	buffer := bufio.NewWriterSize(fp, 4*1024*1024)
	crc32Sign := crc32.NewIEEE()
	writer := io.MultiWriter(buffer, crc32Sign)

	//io proc
	var (
		varintTmp = make([]byte, binary.MaxVarintLen64)
		n         int
	)
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(extendTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	extendTree.Ascend(func(i BtreeItem) bool {
		e := i.(*Extend)
		var raw []byte
		if raw, err = e.Bytes(); err != nil {
			return false
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}

	if err = buffer.Flush(); err != nil {
		return
	}

	//persist checksum file if success
	err = mp.persistChecksum(crc32Sign.Sum(nil), rootDir, checksumFileName(extendFile))
	if err != nil {
		log.LogErrorf("storeExtend: failed to persist extend checksum: %s", err.Error())
	}

	crc = crc32Sign.Sum32()
	log.LogInfof("storeExtend: store complete: partitoinID(%v) volume(%v) numExtends(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, extendTree.Len(), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var multipartTree = sm.multipartTree
	var filename = path.Join(rootDir, multipartFile)
	var fp *os.File
	fp, err = os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	//fp safety
	syncCloseFp := func(fp *os.File) {
		if err == nil {
			syncErr := fp.Sync()
			if syncErr != nil {
				log.LogErrorf("storeMultipart: fp.Sync %s", syncErr.Error())
				err = errors.NewErrorf("[storeMultipart] fp.Sync: %s", syncErr)
			}
		}
		closeErr := fp.Close()
		if closeErr != nil {
			log.LogErrorf("storeMultipart: fp.Close %s", closeErr.Error())
			if err == nil {
				err = errors.NewErrorf("[storeMultipart] fp.Close: %s", closeErr)
			}
		}
	}
	defer syncCloseFp(fp)

	buffer := bufio.NewWriterSize(fp, 4*1024*1024)
	crc32Sign := crc32.NewIEEE()
	writer := io.MultiWriter(buffer, crc32Sign)

	//io proc
	var (
		varintTmp = make([]byte, binary.MaxVarintLen64)
		n         int
	)
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(multipartTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	multipartTree.Ascend(func(i BtreeItem) bool {
		m := i.(*Multipart)
		var raw []byte
		if raw, err = m.Bytes(); err != nil {
			return false
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}

	if err = buffer.Flush(); err != nil {
		return
	}

	//persist checksum file if success
	err = mp.persistChecksum(crc32Sign.Sum(nil), rootDir, checksumFileName(multipartFile))
	if err != nil {
		log.LogErrorf("storeMultipart: failed to persist multipart checksum: %s", err.Error())
	}

	crc = crc32Sign.Sum32()
	log.LogInfof("storeMultipart: store complete: partitoinID(%v) volume(%v) numMultiparts(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, multipartTree.Len(), crc)
	return
}
