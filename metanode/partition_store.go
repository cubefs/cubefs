// Copyright 2018 The CubeFS Authors.
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
	"hash/crc32"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	mmap "github.com/edsrzf/mmap-go"
)

var (
	ErrInvalidSnapshotRoot = errors.NewErrorf("invalid snapshot root")
)

const (
	snapshotDir             = "snapshot"
	snapshotDirTmp          = ".snapshot"
	snapshotBackup          = ".snapshot_backup"
	inodeFile               = "inode"
	dentryFile              = "dentry"
	extendFile              = "extend"
	multipartFile           = "multipart"
	txInfoFile              = "tx_info"
	txRbInodeFile           = "tx_rb_inode"
	txRbDentryFile          = "tx_rb_dentry"
	applyIDFile             = "apply"
	TxIDFile                = "transactionID"
	SnapshotSign            = ".sign"
	metadataFile            = "meta"
	metadataFileTmp         = ".meta"
	uniqIDFile              = "uniqID"
	uniqCheckerFile         = "uniqChecker"
	verdataFile             = "multiVer"
	StaleMetadataSuffix     = ".old"
	StaleMetadataTimeFormat = "20060102150405.000000000"
	verdataInitFile         = "multiVerInitFile"
	deletedExtentsIdFile    = "deletedExtentsId"
	deletedExtentsFile      = "deletedExtents"
)

func (mp *metaPartition) loadMetadataFromFile() (mConf *MetaPartitionConfig, err error) {
	metaFile := path.Join(mp.config.RootDir, metadataFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadMetadata]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := io.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.NewErrorf("[loadMetadata]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf = &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.NewErrorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}

	if mConf.checkMeta() != nil {
		return
	}

	return
}

func (mp *metaPartition) loadMetadata() (err error) {
	mConf, err := mp.loadMetadataFromFile()
	if err != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.VolName = mConf.VolName
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	mp.config.Cursor = mp.config.Start
	mp.config.UniqId = 0

	mp.config.StoreMode = mConf.StoreMode
	mp.config.RocksDBDir = mConf.RocksDBDir
	mp.config.RocksWalFileSize = mConf.RocksWalFileSize
	mp.config.RocksWalMemSize = mConf.RocksWalMemSize
	mp.config.RocksLogFileSize = mConf.RocksLogFileSize
	mp.config.RocksLogReversedTime = mConf.RocksLogReversedTime
	mp.config.RocksLogReVersedCnt = mConf.RocksLogReVersedCnt
	mp.config.RocksWalTTL = mConf.RocksWalTTL

	if mp.config.StoreMode < proto.StoreModeMem || mp.config.StoreMode > proto.StoreModeRocksDb {
		mp.config.StoreMode = proto.StoreModeMem
	}
	if mp.config.RocksDBDir == "" {
		// new version but old config; need select one dir
		err = mp.selectRocksDBDir()
		if err != nil {
			return
		}
	}

	log.LogInfof("loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) loadInode(rootDir string, crc uint32) (err error) {
	var numInodes uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numInodes)
		}
	}()

	handler, _ := mp.inodeTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadInode] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	inoBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				if res := crcCheck.Sum32(); res != crc {
					log.LogErrorf("[loadInode]: check crc mismatch, expected[%d], actual[%d]", crc, res)
					return ErrSnapshotCrcMismatch
				}
				return
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		// length crc
		if _, err = crcCheck.Write(inoBuf); err != nil {
			return err
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
		mp.acucumUidSizeByLoad(ino)
		// data crc
		if _, err = crcCheck.Write(inoBuf); err != nil {
			return err
		}

		mp.size += ino.Size

		_, err = mp.fsmCreateInode(handler, ino)
		// mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
			mp.inodeTree.SetCursor(ino.Inode)
		}
		numInodes += 1
	}
}

// Load dentry from the dentry snapshot.
func (mp *metaPartition) loadDentry(rootDir string, crc uint32) (err error) {
	var numDentries uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
				mp.config.PartitionId, mp.config.VolName, numDentries)
		}
	}()

	handler, _ := mp.inodeTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadDentry] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				if res := crcCheck.Sum32(); res != crc {
					log.LogErrorf("[loadDentry]: check crc mismatch, expected[%d], actual[%d]", crc, res)
					return ErrSnapshotCrcMismatch
				}
				return
			}
			err = errors.NewErrorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}
		if _, err = crcCheck.Write(dentryBuf); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(dentryBuf)

		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDentry]: ReadBody: %s", err.Error())
			return
		}
		dentry := &Dentry{}
		if err = dentry.Unmarshal(dentryBuf); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if status, _ := mp.fsmCreateDentry(handler, dentry, true); status != proto.OpOk {
			err = errors.NewErrorf("[loadDentry] createDentry dentry: %v, resp code: %d", dentry, status)
			return
		}
		if _, err = crcCheck.Write(dentryBuf); err != nil {
			return err
		}
		numDentries += 1
	}
}

func (mp *metaPartition) loadExtend(rootDir string, crc uint32) (err error) {
	filename := path.Join(rootDir, extendFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadExtend] Stat: %s", err.Error())
		return err
	}
	handle, _ := mp.inodeTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	}()
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadExtend] OpenFile: %s", err.Error())
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of extends
	var numExtends uint64
	numExtends, n = binary.Uvarint(mem)
	offset += n

	varintTmp := make([]byte, binary.MaxVarintLen64)
	// write number of extends
	n = binary.PutUvarint(varintTmp, numExtends)

	crcCheck := crc32.NewIEEE()
	if _, err = crcCheck.Write(varintTmp[:n]); err != nil {
		return
	}
	for i := uint64(0); i < numExtends; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var extend *Extend
		if extend, err = NewExtendFromBytes(mem[offset : offset+int(numBytes)]); err != nil {
			return err
		}

		if _, err = crcCheck.Write(mem[offset-n : offset]); err != nil {
			return err
		}
		// log.LogDebugf("loadExtend: new extend from bytes: partitionID (%v) volume(%v) inode[%v]",
		//	mp.config.PartitionId, mp.config.VolName, extend.inode)
		_, _ = mp.fsmSetXAttr(handle, extend)

		if _, err = crcCheck.Write(mem[offset : offset+int(numBytes)]); err != nil {
			return
		}
		offset += int(numBytes)
		mp.statisticExtendByLoad(extend)
	}

	log.LogInfof("loadExtend: load complete: partitionID(%v) volume(%v) numExtends(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, numExtends, filename)
	if res := crcCheck.Sum32(); res != crc {
		log.LogErrorf("loadExtend: check crc mismatch, expected[%d], actual[%d]", crc, res)
		return ErrSnapshotCrcMismatch
	}
	return nil
}

func (mp *metaPartition) loadMultipart(rootDir string, crc uint32) (err error) {
	filename := path.Join(rootDir, multipartFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadMultipart] Stat: %s", err.Error())
		return err
	}
	handler, _ := mp.inodeTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadMultipart] OpenFile: %s", err.Error())
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of multipart
	var numMultiparts uint64
	numMultiparts, n = binary.Uvarint(mem)
	varintTmp := make([]byte, binary.MaxVarintLen64)
	// write number of multipart
	n = binary.PutUvarint(varintTmp, numMultiparts)
	crcCheck := crc32.NewIEEE()
	if _, err = crcCheck.Write(varintTmp[:n]); err != nil {
		return
	}
	offset += n
	for i := uint64(0); i < numMultiparts; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		if _, err = crcCheck.Write(mem[offset-n : offset]); err != nil {
			return err
		}
		var multipart *Multipart
		multipart = MultipartFromBytes(mem[offset : offset+int(numBytes)])
		log.LogDebugf("loadMultipart: create multipart from bytes: partitionID（%v) multipartID(%v)", mp.config.PartitionId, multipart.id)
		mp.fsmCreateMultipart(handler, multipart)
		offset += int(numBytes)
		if _, err = crcCheck.Write(mem[offset-int(numBytes) : offset]); err != nil {
			return err
		}
	}
	log.LogInfof("loadMultipart: load complete: partitionID(%v) numMultiparts(%v) filename(%v)",
		mp.config.PartitionId, numMultiparts, filename)
	if res := crcCheck.Sum32(); res != crc {
		log.LogErrorf("[loadMultipart] check crc mismatch, expected[%d], actual[%d]", crc, res)
		return ErrSnapshotCrcMismatch
	}
	return nil
}

func (mp *metaPartition) loadApplyIDFromSnapshot(rootDir string) (applyID, cursor uint64, err error) {
	filename := path.Join(rootDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadApplyID]: Stat %s", err.Error())
		return
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyID]: ApplyID is empty")
		return
	}
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%d", &applyID, &cursor)

	} else {
		_, err = fmt.Sscanf(string(data), "%d", &applyID)
	}
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}

	log.LogInfof("loadApplyID: load complete: partitionID(%v) volume(%v) applyID(%v) cursor(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, mp.config.Cursor, filename)
	return
}

func (mp *metaPartition) loadApplyID(rootDir string) (err error) {
	var (
		applyIDInSnapshot uint64 = ^uint64(0)
		applyIDInRocksDB  uint64 = ^uint64(0)
		cursorInSnapshot  uint64
		cursorInRocksDB   uint64
		maxInode          uint64
	)
	if mp.HasMemStore() {
		if rootDir == "" {
			return ErrInvalidSnapshotRoot
		}
		if applyIDInSnapshot, cursorInSnapshot, err = mp.loadApplyIDFromSnapshot(rootDir); err != nil {
			return
		}

		atomic.StoreUint64(&mp.applyID, applyIDInSnapshot)

		if cursorInSnapshot > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, cursorInSnapshot)
		}
	}

	if mp.HasRocksDBStore() {
		applyIDInRocksDB = mp.inodeTree.GetApplyID()
		atomic.StoreUint64(&mp.applyID, applyIDInRocksDB)

		cursorInRocksDB = mp.inodeTree.GetCursor()
		if maxInode, err = mp.inodeTree.GetMaxInode(); err != nil {
			return
		}

		if maxInode > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, maxInode)
		}
		if cursorInRocksDB > atomic.LoadUint64(&mp.config.Cursor) {
			atomic.StoreUint64(&mp.config.Cursor, cursorInRocksDB)
		}
	}
	log.LogInfof("mp[%v] applyID:%v, cursor:%v", mp.config.PartitionId, mp.applyID, mp.config.Cursor)

	return
}

func (mp *metaPartition) loadTxRbDentry(rootDir string, crc uint32) (err error) {
	var numTxRbDentry uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadTxRbDentry: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numTxRbDentry)
		}
	}()
	filename := path.Join(rootDir, txRbDentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadTxRbDentry] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadTxRbDentry] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()
	handle, err := mp.txProcessor.txResource.txRbDentryTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[loadTxRbDentry] cannot open write batch, err(%v)", err)
		return err
	}
	defer mp.txProcessor.txResource.txRbDentryTree.ReleaseBatchWriteHandle(handle)

	for {
		txBuf = txBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				if res := crcCheck.Sum32(); res != crc {
					log.LogErrorf("[loadTxRbDentry]: check crc mismatch, expected[%d], actual[%d]", crc, res)
					return ErrSnapshotCrcMismatch
				}
				break
			}
			err = errors.NewErrorf("[loadTxRbDentry] ReadHeader: %s", err.Error())
			return
		}
		// length crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(txBuf)

		// next read body
		if uint32(cap(txBuf)) >= length {
			txBuf = txBuf[:length]
		} else {
			txBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			err = errors.NewErrorf("[loadTxRbDentry] ReadBody: %s", err.Error())
			return
		}

		txRbDentry := NewTxRollbackDentry(nil, nil, 0)
		if err = txRbDentry.Unmarshal(txBuf); err != nil {
			err = errors.NewErrorf("[loadTxRbDentry] Unmarshal: %s", err.Error())
			return
		}

		// data crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		// mp.txProcessor.txResource.txRollbackDentries[txRbDentry.txDentryInfo.GetKey()] = txRbDentry
		err = mp.txProcessor.txResource.txRbDentryTree.Put(handle, txRbDentry)
		if err != nil {
			return
		}
		numTxRbDentry++
	}

	err = mp.txProcessor.txResource.txRbDentryTree.CommitBatchWrite(handle, false)
	if err != nil {
		log.LogErrorf("[loadTxRbDentry] failed to commit write batch, err(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) loadTxRbInode(rootDir string, crc uint32) (err error) {
	var numTxRbInode uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadTxRbInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numTxRbInode)
		}
	}()
	filename := path.Join(rootDir, txRbInodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadTxRbInode] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadTxRbInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()

	handle, err := mp.txProcessor.txResource.txRbInodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[loadTxRbInode] cannot open write batch, err(%v)", err)
		return
	}
	defer mp.txProcessor.txResource.txRbInodeTree.ReleaseBatchWriteHandle(handle)

	for {
		txBuf = txBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			err = errors.NewErrorf("[loadTxRbInode] ReadHeader: %s", err.Error())
			return
		}
		// length crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(txBuf)

		// next read body
		if uint32(cap(txBuf)) >= length {
			txBuf = txBuf[:length]
		} else {
			txBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			err = errors.NewErrorf("[loadTxRbInode] ReadBody: %s", err.Error())
			return
		}

		txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
		if err = txRbInode.Unmarshal(txBuf); err != nil {
			err = errors.NewErrorf("[loadTxRbInode] Unmarshal: %s", err.Error())
			return
		}
		// data crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		err = mp.txProcessor.txResource.txRbInodeTree.Put(handle, txRbInode)
		if err != nil {
			return
		}
		numTxRbInode++
	}
	err = mp.txProcessor.txResource.txRbInodeTree.CommitBatchWrite(handle, false)
	if err != nil {
		log.LogErrorf("[loadTxRbInode] failed to commit write batch, err(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) loadTxInfo(rootDir string, crc uint32) (err error) {
	var numTxInfos uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadTxInfo: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numTxInfos)
		}
	}()
	filename := path.Join(rootDir, txInfoFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadTxInfo] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadTxInfo] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()
	handle, err := mp.txProcessor.txManager.txTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[loadTxInfo] cannot open write batch, err(%v)", err)
		return
	}
	defer mp.txProcessor.txManager.txTree.ReleaseBatchWriteHandle(handle)

	for {
		txBuf = txBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				if res := crcCheck.Sum32(); res != crc {
					log.LogErrorf("[loadTxInfo]: check crc mismatch, expected[%d], actual[%d]", crc, res)
					return ErrSnapshotCrcMismatch
				}
				break
			}
			err = errors.NewErrorf("[loadTxInfo] ReadHeader: %s", err.Error())
			return
		}
		// length crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(txBuf)

		// next read body
		if uint32(cap(txBuf)) >= length {
			txBuf = txBuf[:length]
		} else {
			txBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			err = errors.NewErrorf("[loadTxInfo] ReadBody: %s", err.Error())
			return
		}

		txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
		if err = txInfo.Unmarshal(txBuf); err != nil {
			err = errors.NewErrorf("[loadTxInfo] Unmarshal: %s", err.Error())
			return
		}

		// data crc
		if _, err = crcCheck.Write(txBuf); err != nil {
			return err
		}

		err = mp.txProcessor.txManager.addTxInfo(handle, txInfo)
		// NOTE: should never happens in memory store
		if err != nil {
			log.LogErrorf("[loadTxInfo]: failed to add tx to tx tree, err(%v)", err)
			panic(err)
		}
		numTxInfos++
	}
	err = mp.txProcessor.txManager.txTree.CommitBatchWrite(handle, false)
	if err != nil {
		log.LogErrorf("[loadTxInfo] failed to commit write batch, err(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) loadTxID(rootDir string) (err error) {
	filename := path.Join(rootDir, TxIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		err = errors.NewErrorf("[loadTxID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadTxID]: TxID is empty")
		return
	}
	var txId uint64
	_, err = fmt.Sscanf(string(data), "%d", &txId)
	if err != nil {
		err = errors.NewErrorf("[loadTxID] ReadTxID: %s", err.Error())
		return
	}

	if txId > mp.txProcessor.txManager.txIdAlloc.getTransactionID() {
		mp.txProcessor.txManager.txIdAlloc.setTransactionID(txId)
		mp.txProcessor.txManager.txTree.SetTxId(txId)
	}
	log.LogInfof("loadTxID: load complete: partitionID(%v) volume(%v) txId(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.txProcessor.txManager.txIdAlloc.getTransactionID(), filename)
	return
}

func (mp *metaPartition) loadUniqID(rootDir string) (err error) {
	filename := path.Join(rootDir, uniqIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		err = errors.NewErrorf("[loadUniqID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadUniqID]: uniqID is empty")
		return
	}
	var uniqId uint64
	_, err = fmt.Sscanf(string(data), "%d", &uniqId)
	if err != nil {
		err = errors.NewErrorf("[loadUniqID] Read uniqID: %s", err.Error())
		return
	}

	if uniqId > mp.GetUniqId() {
		atomic.StoreUint64(&mp.config.UniqId, uniqId)
	}

	log.LogInfof("loadUniqID: load complete: partitionID(%v) volume(%v) uniqID(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.GetUniqId(), filename)
	return
}

func (mp *metaPartition) loadUniqChecker(rootDir string, crc uint32) (err error) {
	log.LogInfof("loadUniqChecker partition(%v) begin", mp.config.PartitionId)
	filename := path.Join(rootDir, uniqCheckerFile)
	if _, err = os.Stat(filename); err != nil {
		log.LogErrorf("loadUniqChecker get file %s err(%s)", filename, err)
		err = nil
		return
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		log.LogErrorf("loadUniqChecker read file %s err(%s)", filename, err)
		err = errors.NewErrorf("[loadUniqChecker] OpenFile: %v", err.Error())
		return
	}
	if err = mp.uniqChecker.UnMarshal(data); err != nil {
		log.LogErrorf("loadUniqChecker UnMarshal err(%s)", err)
		err = errors.NewErrorf("[loadUniqChecker] Unmarshal: %v", err.Error())
		return
	}

	crcCheck := crc32.NewIEEE()
	if _, err = crcCheck.Write(data); err != nil {
		log.LogErrorf("loadUniqChecker write to  crcCheck failed: %s", err)
		return err
	}
	if res := crcCheck.Sum32(); res != crc {
		log.LogErrorf("[loadUniqChecker]: check crc mismatch, expected[%d], actual[%d]", crc, res)
		return ErrSnapshotCrcMismatch
	}

	log.LogInfof("loadUniqChecker partition(%v) complete", mp.config.PartitionId)
	return
}

func (mp *metaPartition) loadMultiVer(rootDir string, crc uint32) (err error) {
	filename := path.Join(rootDir, verdataFile)
	if _, err = os.Stat(filename); err != nil {

		err = nil
		return
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}

		err = errors.NewErrorf("[loadMultiVer] OpenFile: %s", err.Error())
		return
	}

	if len(data) == 0 {
		err = errors.NewErrorf("[loadMultiVer]: ApplyID is empty")
		return
	}

	var (
		verData string
		applyId uint64
	)
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%s", &applyId, &verData)
	} else {
		_, err = fmt.Sscanf(string(data), "%d", &applyId)
	}

	if err != nil {
		err = errors.NewErrorf("[loadMultiVer] ReadVerList: %s", err.Error())
		return
	}

	var verList []*proto.VolVersionInfo
	if err = json.Unmarshal([]byte(verData), &verList); err != nil {
		err = errors.NewErrorf("[loadMultiVer] ReadVerList: %s verData(%v) applyId %v", verList, verData, applyId)
		return
	}

	var byteData []byte
	if byteData, err = json.Marshal(verList); err != nil {
		return
	}
	sign := crc32.NewIEEE()
	if _, err = sign.Write(byteData); err != nil {
		return
	}

	if crc != sign.Sum32() {
		return fmt.Errorf("partitionID(%v) volume(%v) calc crc %v not equal with disk %v", mp.config.PartitionId, mp.config.VolName, sign.Sum32(), crc)
	}

	mp.multiVersionList.VerList = verList
	mp.verSeq = mp.multiVersionList.GetLastVer()

	log.LogInfof("loadMultiVer: updateVerList load complete: partitionID(%v) volume(%v) applyID(%v) filename(%v) verlist (%v) crc (%v) mp Ver(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, filename, mp.multiVersionList.VerList, crc, mp.verSeq)
	return
}

func (mp *metaPartition) storeMultiVersion(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, verdataFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	var verData []byte
	if verData, err = json.Marshal(sm.multiVerList); err != nil {
		return
	}
	sign := crc32.NewIEEE()
	if _, err = sign.Write(verData); err != nil {
		return
	}
	crc = sign.Sum32()

	if _, err = fp.WriteString(fmt.Sprintf("%d|%s", sm.snap.ApplyID(), string(verData))); err != nil {
		return
	}
	log.LogInfof("storeMultiVersion: store complete: partitionID(%v) volume(%v) applyID(%v) verData(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.ApplyID(), string(verData), crc)
	return
}

func (mp *metaPartition) renameStaleMetadata() (err error) {
	if _, err = os.Stat(mp.config.RootDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
	}

	curTime := time.Now().Format(StaleMetadataTimeFormat)
	staleMetaDirName := mp.config.RootDir + "_" + curTime + StaleMetadataSuffix
	if err = os.Rename(mp.config.RootDir, staleMetaDirName); err != nil {
		return err
	}
	return nil
}

func (mp *metaPartition) persistMetadataToFile(conf *MetaPartitionConfig) (err error) {
	if err = conf.checkMeta(); err != nil {
		return
	}

	// TODO Unhandled errors
	os.MkdirAll(conf.RootDir, 0o755)
	filename := path.Join(conf.RootDir, metadataFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		// TODO Unhandled errors
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()

	data, err := json.Marshal(conf)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	if err = os.Rename(filename, path.Join(conf.RootDir, metadataFile)); err != nil {
		return
	}
	log.LogInfof("persistMetata: persist complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		conf.PartitionId, conf.VolName, conf.Start, conf.End, conf.Cursor)
	return
}

func (mp *metaPartition) persistMetadata() (err error) {
	err = mp.persistMetadataToFile(mp.config)
	return
}

func (mp *metaPartition) persistSelectedRocksdbDir() (err error) {
	conf, err := mp.loadMetadataFromFile()
	if err != nil {
		return
	}
	if conf.RocksDBDir != "" {
		log.LogDebugf("[persistSelectedRocksdbDir] mp(%v) rocksdb dir(%v)", mp.config.PartitionId, conf.RocksDBDir)
		return
	}
	conf.RocksDBDir = mp.config.RocksDBDir
	log.LogInfof("[persistSelectedRocksdbDir] mp(%v) persist rocksdb dir(%v)", mp.config.PartitionId, mp.config.RocksDBDir)
	err = mp.persistMetadata()
	if err != nil {
		log.LogErrorf("[persistSelectedRocksdbDir] mp(%v) failed to persist rocksdb dir", mp.config.PartitionId)
		return
	}
	return
}

func (mp *metaPartition) storeApplyID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()

	cursor := mp.GetCursor()
	if _, err = fp.WriteString(fmt.Sprintf("%d|%d", sm.snap.ApplyID(), cursor)); err != nil {
		return
	}
	log.LogWarnf("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.ApplyID(), cursor)
	return
}

func (mp *metaPartition) storeTxID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, TxIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", sm.snap.TxID())); err != nil {
		return
	}
	log.LogInfof("storeTxID: store complete: partitionID(%v) volume(%v) txId(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.TxID())
	return
}

func (mp *metaPartition) storeTxRbDentry(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txRbDentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

	err = sm.snap.Range(TransactionRollbackDentryType, func(item interface{}) (bool, error) {
		rbDentry := item.(*TxRollbackDentry)
		if data, err = rbDentry.Marshal(); err != nil {
			return false, err
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}

		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("[storeTxRbDentry] failed to store rb dentry, err(%v)", err)
		return
	}

	crc = sign.Sum32()
	log.LogInfof("storeTxRbDentry: store complete: partitoinID(%v) volume(%v) numRbDentry(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(TransactionRollbackDentryType), crc)
	return
}

func (mp *metaPartition) storeTxRbInode(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txRbInodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

	err = sm.snap.Range(TransactionRollbackInodeType, func(item interface{}) (bool, error) {
		rbInode := item.(*TxRollbackInode)
		if data, err = rbInode.Marshal(); err != nil {
			return false, err
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}

		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("[storeTxRbInode] failed to store rb inode, err(%v)", err)
		return
	}

	crc = sign.Sum32()
	log.LogInfof("storeTxRbInode: store complete: partitoinID(%v) volume(%v) numRbinode[%v] crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(TransactionRollbackInodeType), crc)
	return
}

func (mp *metaPartition) storeTxInfo(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txInfoFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	err = sm.snap.Range(TransactionType, func(item interface{}) (bool, error) {
		tx := item.(*proto.TransactionInfo)
		if data, err = tx.Marshal(); err != nil {
			return false, err
		}

		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}

		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, err
	})
	if err != nil {
		log.LogErrorf("[storeTxInfo] failed to store tx info, err(%v)", err)
		return
	}

	crc = sign.Sum32()
	log.LogInfof("storeTxInfo: store complete: partitoinID(%v) volume(%v) numTxs(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(TransactionType), crc)
	return
}

func (mp *metaPartition) storeInode(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	size := uint64(0)

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.snap.Range(InodeType, func(item interface{}) (bool, error) {

		ino := item.(*Inode)
		if sm.uidRebuild {
			mp.acucumUidSizeByStore(ino)
		}

		if data, err = ino.Marshal(); err != nil {
			return false, nil
		}

		size += ino.Size
		mp.fileStats(ino)

		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, nil
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, nil
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false, nil
		}
		if _, err = sign.Write(data); err != nil {
			return false, nil
		}
		return true, nil
	})
	mp.acucumRebuildFin(sm.uidRebuild)
	crc = sign.Sum32()
	mp.size = size

	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v), size (%d)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(InodeType), crc, size)

	return
}

func (mp *metaPartition) storeDentry(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.snap.Range(DentryType, func(item interface{}) (bool, error) {
		dentry := item.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false, nil
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, nil
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, nil
		}
		if _, err = fp.Write(data); err != nil {
			return false, nil
		}
		if _, err = sign.Write(data); err != nil {
			return false, nil
		}
		return true, nil
	})
	crc = sign.Sum32()
	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(DentryType), crc)
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc uint32, err error) {
	fp := path.Join(rootDir, extendFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	log.LogDebugf("storeExtend: store start: partitoinID(%v) volume(%v) numInodes(%v) extends(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(InodeType), sm.snap.Count(ExtendType))
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	writer := bufio.NewWriterSize(f, 4*1024*1024)
	crc32 := crc32.NewIEEE()
	varintTmp := make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(sm.snap.Count(ExtendType)))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}

	sm.snap.Range(ExtendType, func(i interface{}) (bool, error) {
		e := i.(*Extend)
		var raw []byte
		if sm.quotaRebuild {
			mp.statisticExtendByStore(e)
		}
		if raw, err = e.Bytes(); err != nil {
			return false, nil
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false, nil
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false, nil
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false, nil
		}
		if _, err = crc32.Write(raw); err != nil {
			return false, nil
		}
		return true, nil
	})
	log.LogInfof("storeExtend: write data ok: partitoinID(%v) volume(%v) numInodes(%v) extends(%v) quotaRebuild(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(InodeType), sm.snap.Count(ExtendType), sm.quotaRebuild)
	mp.mqMgr.statisticRebuildFin(sm.quotaRebuild)
	if err != nil {
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeExtend: store complete: partitoinID(%v) volume(%v) numExtends(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(ExtendType), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	fp := path.Join(rootDir, multipartFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	writer := bufio.NewWriterSize(f, 4*1024*1024)
	crc32 := crc32.NewIEEE()
	varintTmp := make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(sm.snap.Count(MultipartType)))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	sm.snap.Range(MultipartType, func(i interface{}) (bool, error) {
		m := i.(*Multipart)
		var raw []byte
		if raw, err = m.Bytes(); err != nil {
			return false, nil
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false, nil
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false, nil
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false, nil
		}
		if _, err = crc32.Write(raw); err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeMultipart: store complete: partitoinID(%v) volume(%v) numMultiparts(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(MultipartType), crc)
	return
}

func (mp *metaPartition) storeUniqID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, uniqIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", sm.uniqId)); err != nil {
		return
	}
	log.LogInfof("storeUniqID: store complete: partitionID(%v) volume(%v) uniqID(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.uniqId)
	return
}

func (mp *metaPartition) storeUniqChecker(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, uniqCheckerFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()

	var data []byte
	if data, crc, err = sm.uniqChecker.Marshal(); err != nil {
		return
	}

	if _, err = fp.Write(data); err != nil {
		return
	}

	log.LogInfof("storeUniqChecker: store complete: PartitionID(%v) volume(%v) crc(%v)",
		mp.config.UniqId, mp.config.VolName, crc)
	return
}

func (mp *metaPartition) loadDeletedExtentId(rootDir string) (err error) {
	if mp.HasMemStore() {
		if rootDir == "" {
			return ErrInvalidSnapshotRoot
		}
		filename := path.Join(rootDir, deletedExtentsIdFile)
		if _, err = os.Stat(filename); err != nil {
			if os.IsNotExist(err) {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDeletedExtentID]: Stat %s", err.Error())
			return
		}
		var data []byte
		data, err = os.ReadFile(filename)
		if err != nil {
			return
		}
		mp.deletedExtentId = binary.BigEndian.Uint64(data)
		return
	}

	if mp.HasRocksDBStore() {
		mp.deletedExtentId = mp.deletedExtentsTree.GetDeletedExtentId()
	}
	return
}

func (mp *metaPartition) storeDeletedExtentId(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, deletedExtentsIdFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()

	buff := bytes.NewBuffer([]byte{})
	err = binary.Write(buff, binary.BigEndian, sm.snap.DeletedExtentId())
	if err != nil {
		return
	}
	if _, err = buff.WriteTo(fp); err != nil {
		return
	}
	return
}

func (mp *metaPartition) loadDeletedExtents(rootDir string, crc uint32) (err error) {

	handler, _ := mp.deletedExtentsTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, deletedExtentsFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadDeletedExtents] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadDeletedExtents] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	lenBuf := make([]byte, 4)
	buff := make([]byte, 0)
	crcCheck := crc32.NewIEEE()
	for {
		// first read length
		_, err = io.ReadFull(reader, lenBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				if res := crcCheck.Sum32(); res != crc {
					log.LogErrorf("[loadDeletedExtents]: check crc mismatch, expected[%d], actual[%d]", crc, res)
					err = ErrSnapshotCrcMismatch
				}
				return
			}
			err = errors.NewErrorf("[loadDeletedExtents] ReadHeader: %s", err.Error())
			return
		}
		// length crc
		if _, err = crcCheck.Write(lenBuf); err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(lenBuf)

		// next read body
		if len(buff) < int(length) {
			buff = make([]byte, length)
		}
		_, err = io.ReadFull(reader, buff)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedExtents] ReadBody: %s ", err.Error())
			return
		}
		if _, err = crcCheck.Write(buff[:length]); err != nil {
			return
		}
		dek := &DeletedExtentKey{}
		err = dek.Unmarshal(buff[:length])
		if err != nil {
			err = errors.NewErrorf("[loadDeletedExtents] Unmarshal: %s", err.Error())
			return
		}
		err = mp.deletedExtentsTree.Put(handler, dek)
		if err != nil {
			return
		}
	}
}

func (mp *metaPartition) storeDeletedExtents(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, deletedExtentsFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.snap.Range(DeletedExtentsType, func(item interface{}) (bool, error) {
		dek := item.(*DeletedExtentKey)

		if data, err = dek.Marshal(); err != nil {
			return false, nil
		}

		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, nil
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, nil
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false, nil
		}
		if _, err = sign.Write(data); err != nil {
			return false, nil
		}
		return true, nil
	})
	crc = sign.Sum32()
	return
}
