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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	mmap "github.com/edsrzf/mmap-go"
)

const (
	snapshotDir     = "snapshot"
	snapshotDirTmp  = ".snapshot"
	snapshotBackup  = ".snapshot_backup"
	inodeFile       = "inode"
	dentryFile      = "dentry"
	extendFile      = "extend"
	multipartFile   = "multipart"
	txInfoFile      = "tx_info"
	txRbInodeFile   = "tx_rb_inode"
	txRbDentryFile  = "tx_rb_dentry"
	applyIDFile     = "apply"
	TxIDFile        = "transactionID"
	SnapshotSign    = ".sign"
	metadataFile    = "meta"
	metadataFileTmp = ".meta"
)

func (mp *metaPartition) loadMetadata() (err error) {
	metaFile := path.Join(mp.config.RootDir, metadataFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadMetadata]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.NewErrorf("[loadMetadata]: ReadFile %s, data: %s", err.Error(),
			string(data))
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

	mp.uidManager = NewUidMgr(mp.config.VolName, mp.config.PartitionId)
	mp.mqMgr = NewQuotaManager(mp.config.VolName, mp.config.PartitionId)

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
	filename := path.Join(rootDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadInode] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
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

		mp.fsmCreateInode(ino)
		mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
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
	filename := path.Join(rootDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadDentry] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
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
		if status := mp.fsmCreateDentry(dentry, true); status != proto.OpOk {
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
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

	var varintTmp = make([]byte, binary.MaxVarintLen64)
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
		log.LogDebugf("loadExtend: new extend from bytes: partitionID（%v) volume(%v) inode(%v)",
			mp.config.PartitionId, mp.config.VolName, extend.inode)
		_ = mp.fsmSetXAttr(extend)

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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
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
	var varintTmp = make([]byte, binary.MaxVarintLen64)
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
		mp.fsmCreateMultipart(multipart)
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

func (mp *metaPartition) loadApplyID(rootDir string) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[loadApplyID]: Stat %s", err.Error())
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadFile: %s", err.Error())
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadTxRbDentry] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()

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
				return
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

		//mp.txProcessor.txResource.txRollbackDentries[txRbDentry.txDentryInfo.GetKey()] = txRbDentry
		mp.txProcessor.txResource.txRbDentryTree.ReplaceOrInsert(txRbDentry, true)
		numTxRbDentry++
	}
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadTxRbInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()

	for {
		txBuf = txBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, txBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
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

		//mp.txProcessor.txResource.txRollbackInodes[txRbInode.inode.Inode] = txRbInode
		mp.txProcessor.txResource.txRbInodeTree.ReplaceOrInsert(txRbInode, true)
		numTxRbInode++
	}
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadTxInfo] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	txBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()

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
				return
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

		//mp.txProcessor.txManager.transactions[txInfo.TxID] = txInfo
		//mp.txProcessor.txManager.txTree.ReplaceOrInsert(txInfo, true)
		mp.txProcessor.txManager.addTxInfo(txInfo)
		numTxInfos++
	}
}

func (mp *metaPartition) loadTxID(rootDir string) (err error) {
	filename := path.Join(rootDir, TxIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
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
	}
	log.LogInfof("loadTxID: load complete: partitionID(%v) volume(%v) txId(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.txProcessor.txManager.txIdAlloc.getTransactionID(), filename)
	return
}

func (mp *metaPartition) persistMetadata() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[persistMetadata]->%s", err.Error())
		return
	}

	// TODO Unhandled errors
	os.MkdirAll(mp.config.RootDir, 0755)
	filename := path.Join(mp.config.RootDir, metadataFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		// TODO Unhandled errors
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()

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
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d|%d", sm.applyIndex, atomic.LoadUint64(&mp.config.Cursor))); err != nil {
		return
	}
	log.LogWarnf("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex)
	return
}

func (mp *metaPartition) storeTxID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, TxIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", sm.txId)); err != nil {
		return
	}
	log.LogInfof("storeTxID: store complete: partitionID(%v) volume(%v) txId(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.txId)
	return
}

func (mp *metaPartition) storeTxRbDentry(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txRbDentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
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

	//for _, rbDentry := range sm.txRollbackDentries {
	//	if data, err = rbDentry.Marshal(); err != nil {
	//		break
	//	}
	//	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	//	if _, err = fp.Write(lenBuf); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(lenBuf); err != nil {
	//		break
	//	}
	//
	//	if _, err = fp.Write(data); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(data); err != nil {
	//		break
	//	}
	//}

	sm.txRbDentryTree.Ascend(func(i BtreeItem) bool {
		rbDentry := i.(*TxRollbackDentry)
		if data, err = rbDentry.Marshal(); err != nil {
			return false
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}

		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})

	crc = sign.Sum32()
	log.LogInfof("storeTxRbDentry: store complete: partitoinID(%v) volume(%v) numRbDentry(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.txRbDentryTree.Len(), crc)
	return
}

func (mp *metaPartition) storeTxRbInode(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txRbInodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
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

	//for _, rbInode := range sm.txRollbackInodes {
	//	if data, err = rbInode.Marshal(); err != nil {
	//		break
	//	}
	//	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	//	if _, err = fp.Write(lenBuf); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(lenBuf); err != nil {
	//		break
	//	}
	//
	//	if _, err = fp.Write(data); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(data); err != nil {
	//		break
	//	}
	//}

	sm.txRbInodeTree.Ascend(func(i BtreeItem) bool {
		rbInode := i.(*TxRollbackInode)
		if data, err = rbInode.Marshal(); err != nil {
			return false
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}

		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})

	crc = sign.Sum32()
	log.LogInfof("storeTxRbInode: store complete: partitoinID(%v) volume(%v) numRbInode(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.txRbInodeTree.Len(), crc)
	return
}

func (mp *metaPartition) storeTxInfo(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txInfoFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
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

	//for _, tx := range sm.transactions {
	//	if data, err = tx.Marshal(); err != nil {
	//		break
	//	}
	//
	//	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	//	if _, err = fp.Write(lenBuf); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(lenBuf); err != nil {
	//		break
	//	}
	//
	//	if _, err = fp.Write(data); err != nil {
	//		break
	//	}
	//	if _, err = sign.Write(data); err != nil {
	//		break
	//	}
	//}

	sm.txTree.Ascend(func(i BtreeItem) bool {
		tx := i.(*proto.TransactionInfo)
		if data, err = tx.Marshal(); err != nil {
			return false
		}

		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}

		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})

	crc = sign.Sum32()
	log.LogInfof("storeTxInfo: store complete: partitoinID(%v) volume(%v) numTxs(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.txTree.Len(), crc)
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
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	size := uint64(0)

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	mp.fileRange = make([]int64, MaxRangeType)
	mp.acucumRebuildStart()
	sm.inodeTree.Ascend(func(i BtreeItem) bool {
		ino := i.(*Inode)
		mp.acucumUidSizeByStore(ino)

		if data, err = ino.Marshal(); err != nil {
			return false
		}

		size += ino.Size
		mp.fileStats(ino)

		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})
	mp.acucumRebuildFin()
	crc = sign.Sum32()
	mp.size = size

	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v), size (%d)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), crc, size)

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
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.dentryTree.Ascend(func(i BtreeItem) bool {
		dentry := i.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}
		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})
	crc = sign.Sum32()
	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), crc)
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var extendTree = sm.extendTree
	var fp = path.Join(rootDir, extendFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	log.LogDebugf("hytemp storeExtend: store start: partitoinID(%v) volume(%v) numInodes(%v) extends(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), sm.extendTree.Len())
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(extendTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	mp.mqMgr.statisticRebuildStart()
	extendTree.Ascend(func(i BtreeItem) bool {
		e := i.(*Extend)
		var raw []byte
		mp.statisticExtendByStore(e, sm.inodeTree)
		if raw, err = e.Bytes(); err != nil {
			return false
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		if _, err = crc32.Write(raw); err != nil {
			return false
		}
		return true
	})
	log.LogInfof("hytemp storeExtend: write data ok: partitoinID(%v) volume(%v) numInodes(%v) extends(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), sm.extendTree.Len())
	mp.mqMgr.statisticRebuildFin()
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
		mp.config.PartitionId, mp.config.VolName, extendTree.Len(), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var multipartTree = sm.multipartTree
	var fp = path.Join(rootDir, multipartFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(multipartTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
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
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		if _, err = crc32.Write(raw); err != nil {
			return false
		}
		return true
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
		mp.config.PartitionId, mp.config.VolName, multipartTree.Len(), crc)
	return
}
