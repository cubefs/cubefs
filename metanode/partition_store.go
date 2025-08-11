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
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	mmap "github.com/edsrzf/mmap-go"
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
	writeBuffSize           = 1024 * 1024
)

type bufFile struct {
	*os.File
	bf *bufio.Writer
}

func newBufFile(name string, flag int, mode os.FileMode) (*bufFile, error) {
	file, err := os.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}

	bfile := &bufFile{
		File: file,
		bf:   bufio.NewWriterSize(file, writeBuffSize),
	}

	return bfile, nil
}

func (bf *bufFile) Sync() error {
	err := bf.bf.Flush()
	if err != nil {
		return err
	}
	return bf.File.Sync()
}

func (bf *bufFile) Close() error {
	err := bf.bf.Flush()
	if err != nil {
		return err
	}
	return bf.File.Close()
}

func (bf *bufFile) Write(data []byte) (nn int, err error) {
	return bf.bf.Write(data)
}

func (mp *metaPartition) loadMetadata() (err error) {
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
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.NewErrorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}

	// compat with old persisted meta partitions, add raft port info
	lackRaftPort := false
	if mp.manager.metaNode.raftPartitionCanUsingDifferentPort {
		for i, peer := range mConf.Peers {
			if len(peer.ReplicaPort) == 0 || len(peer.HeartbeatPort) == 0 {
				peer.ReplicaPort = mp.manager.metaNode.raftReplicatePort
				peer.HeartbeatPort = mp.manager.metaNode.raftHeartbeatPort
				mConf.Peers[i] = peer
				lackRaftPort = true
			}
		}
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
	mp.config.UniqId = 0

	mp.uidManager = NewUidMgr(mp.config.VolName, mp.config.PartitionId)
	mp.mqMgr = NewQuotaManager(mp.config.VolName, mp.config.PartitionId)

	if mp.manager.metaNode.raftPartitionCanUsingDifferentPort && lackRaftPort {
		if err = mp.persistMetadata(); err != nil {
			log.LogErrorf("persist mp(%v) meta with raft port error(%v)", mp.config.PartitionId, err)
			return
		}
	}

	log.LogWarnf("loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) loadInode(rootDir string, crc uint32) (err error) {
	var (
		numInodes uint64
		fileRange []int64
	)
	defer func() {
		if err == nil {
			log.LogInfof("loadInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numInodes)
			mp.fileRange = fileRange
		}
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
	limitReader := &io.LimitedReader{R: reader, N: 0}

	buff := GetInodeBuf()
	defer PutInodeBuf(buff)

	inoBuf := make([]byte, 4)
	crcCheck := crc32.NewIEEE()

	thresholds, _, enable := mp.manager.GetFileStatsConfig()
	fileRange = make([]int64, len(thresholds)+1)

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
		buff.Reset()
		limitReader.N = int64(length)
		n := int64(0)
		n, err = io.Copy(buff, limitReader)
		if err != nil || n != int64(length) {
			err = errors.NewErrorf("[loadInode] ReadBody: %s, n %d, length %d", err, n, length)
			return
		}

		data := buff.Bytes()
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(data); err != nil {
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return
		}
		if ino.LeaseExpireTime == 0 {
			ino.LeaseExpireTime = uint64(ino.ModifyTime) + proto.ForbiddenMigrationRenewalSeonds
		}
		mp.acucumUidSizeByLoad(ino)
		// data crc
		if _, err = crcCheck.Write(data); err != nil {
			return err
		}

		if enable && proto.IsRegular(ino.Type) && ino.NLink > 0 {
			index := calculateFileRangeIndex(ino.Size, thresholds)
			if index >= 0 && index < len(fileRange) {
				fileRange[index]++
			}
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

func calculateFileRangeIndex(size uint64, thresholds []uint64) int {
	if len(thresholds) == 0 {
		return -1
	}

	maxSize := thresholds[len(thresholds)-1]
	if size >= maxSize {
		return len(thresholds)
	}

	return sort.Search(len(thresholds), func(i int) bool {
		return size < thresholds[i]
	})
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()

	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	limitReader := &io.LimitedReader{R: reader, N: 0}

	dentryBuf := make([]byte, 4)
	buff := GetDentryBuf()
	defer PutDentryBuf(buff)

	crcCheck := crc32.NewIEEE()
	for {
		// dentryBuf = dentryBuf[:4]
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

		buff.Reset()

		limitReader.N = int64(length)
		n := int64(0)

		n, err = io.Copy(buff, limitReader)
		if err != nil || n != int64(length) {
			err = errors.NewErrorf("[loadDentry]: ReadBody: %v, n %d, len %d", err, n, length)
			return
		}
		data := buff.Bytes()

		dentry := &Dentry{}
		if err = dentry.Unmarshal(data); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if status := mp.fsmCreateDentry(dentry, true); status != proto.OpOk {
			err = errors.NewErrorf("[loadDentry] createDentry dentry: %v, resp code: %d", dentry, status)
			return
		}
		if _, err = crcCheck.Write(data); err != nil {
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
	ino := NewSimpleInode(0)
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
		_ = mp.fsmSetXAttr(extend)

		if _, err = crcCheck.Write(mem[offset : offset+int(numBytes)]); err != nil {
			return
		}
		offset += int(numBytes)
		mp.statisticExtendByLoad(extend, ino)
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
	numMultiparts, _ = binary.Uvarint(mem)
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
		multipart := MultipartFromBytes(mem[offset : offset+int(numBytes)])
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
	data, err := os.ReadFile(filename)
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

	mp.storedApplyId = mp.applyID

	if cursor > mp.GetCursor() {
		atomic.StoreUint64(&mp.config.Cursor, cursor)
	}

	log.LogInfof("loadApplyID: load complete: partitionID(%v) volume(%v) applyID(%v) cursor(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, mp.config.Cursor, filename)
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

		// mp.txProcessor.txResource.txRollbackDentries[txRbDentry.txDentryInfo.GetKey()] = txRbDentry
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
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
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
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

	if uniqId == 0 {
		// to avoid the issue of UniqID being read as 0 after a restart due to the lack of persistence in version v3.3.1,
		// set it to a very large value.
		uniqId = 200 * 10000 * 10000
		mp.doStoreUniqID(rootDir, uniqId)
		log.LogInfof("loadUniqID: vol(%v) loaded uniqId is 0, force set it as %v and persist",
			mp.config.VolName, uniqId)
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

	if _, err = fp.WriteString(fmt.Sprintf("%d|%s", sm.applyIndex, string(verData))); err != nil {
		return
	}
	log.LogInfof("storeMultiVersion: store complete: partitionID(%v) volume(%v) applyID(%v) verData(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex, string(verData), crc)
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

func (mp *metaPartition) persistMetadata() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[persistMetadata]->%s", err.Error())
		return
	}

	// TODO Unhandled errors
	os.MkdirAll(mp.config.RootDir, 0o755)
	filename := path.Join(mp.config.RootDir, metadataFileTmp)
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
	log.LogWarnf("persistMetata: persist complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
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
		if err == nil {
			err = fp.Sync()
		}
		fp.Close()
	}()

	cursor := mp.GetCursor()
	if _, err = fp.WriteString(fmt.Sprintf("%d|%d", sm.applyIndex, cursor)); err != nil {
		return
	}

	log.LogWarnf("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex, cursor)
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
		if err == nil {
			err = fp.Sync()
		}
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
	fp, err := newBufFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

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
	fp, err := newBufFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

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
	log.LogInfof("storeTxRbInode: store complete: partitoinID(%v) volume(%v) numRbinode[%v] crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.txRbInodeTree.Len(), crc)
	return
}

func (mp *metaPartition) storeTxInfo(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, txInfoFile)
	fp, err := newBufFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		// TODO Unhandled errors
		fp.Close()
	}()

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

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
	sm *storeMsg,
) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeFile)
	fp, err := newBufFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		// TODO Unhandled errors
		fp.Close()
	}()

	size := uint64(0)

	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()

	thresholds, _, enable := mp.manager.GetFileStatsConfig()
	fileRange := make([]int64, len(thresholds)+1)

	sm.inodeTree.Ascend(func(i BtreeItem) bool {
		ino := i.(*Inode)
		if sm.uidRebuild {
			mp.acucumUidSizeByStore(ino)
		}

		buf := GetInodeBuf()
		defer PutInodeBuf(buf)

		err = ino.MarshalV2(buf)
		if err != nil {
			return false
		}
		data = buf.Bytes()

		size += ino.Size
		if enable && proto.IsRegular(ino.Type) && ino.NLink > 0 {
			index := calculateFileRangeIndex(ino.Size, thresholds)
			if index >= 0 && index < len(fileRange) {
				fileRange[index]++
			}
		}

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

	mp.fileRange = fileRange

	mp.acucumRebuildFin(sm.uidRebuild)
	crc = sign.Sum32()
	mp.size = size

	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v), size (%d)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), crc, size)

	return
}

func (mp *metaPartition) storeDentry(rootDir string,
	sm *storeMsg,
) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryFile)
	fp, err := newBufFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.dentryTree.Ascend(func(i BtreeItem) bool {
		dentry := i.(*Dentry)

		tmpBuf := GetDentryBuf()
		defer PutDentryBuf(tmpBuf)

		err = dentry.MarshalV2(tmpBuf)
		if err != nil {
			return false
		}
		data = tmpBuf.Bytes()

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
	extendTree := sm.extendTree
	fp := path.Join(rootDir, extendFile)
	f, err := newBufFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return 0, err
	}
	log.LogDebugf("storeExtend: store start: partitoinID(%v) volume(%v) numInodes(%v) extends(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), sm.extendTree.Len())
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
	n = binary.PutUvarint(varintTmp, uint64(extendTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	sIno := NewSimpleInode(0)
	extendTree.Ascend(func(i BtreeItem) bool {
		e := i.(*Extend)
		var raw []byte
		if sm.quotaRebuild {
			sIno.Inode = e.GetInode()
			mp.statisticExtendByStore(e, sIno)
		}
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
	log.LogInfof("storeExtend: write data ok: partitoinID(%v) volume(%v) numInodes(%v) extends(%v) quotaRebuild(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), sm.extendTree.Len(), sm.quotaRebuild)
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
		mp.config.PartitionId, mp.config.VolName, extendTree.Len(), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	multipartTree := sm.multipartTree
	fp := path.Join(rootDir, multipartFile)
	f, err := newBufFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0o755)
	if err != nil {
		return 0, err
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

func (mp *metaPartition) doStoreUniqID(rootDir string, uniqId uint64) (err error) {
	filename := path.Join(rootDir, uniqIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", uniqId)); err != nil {
		return
	}
	log.LogInfof("storeUniqID: store complete: partitionID(%v) volume(%v) uniqID(%v)",
		mp.config.PartitionId, mp.config.VolName, uniqId)
	return
}

func (mp *metaPartition) storeUniqID(rootDir string, sm *storeMsg) (err error) {
	return mp.doStoreUniqID(rootDir, sm.uniqId)
}

func (mp *metaPartition) storeUniqChecker(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, uniqCheckerFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0o755)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = fp.Sync()
		}
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
