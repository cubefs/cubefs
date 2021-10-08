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

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	mmap "github.com/edsrzf/mmap-go"
)

const (
	snapshotDir        = "snapshot"
	snapshotDirTmp     = ".snapshot"
	snapshotBackup     = ".snapshot_backup"
	inodeFileLarge     = "inode"
	inodeFileSmall     = "inode_small"
	dentryFileLarge    = "dentry"
	dentryFileSmall    = "dentry_small"
	extendFileLarge    = "extend"
	extendFileSmall    = "extend_small"
	multipartFileLarge = "multipart"
	multipartFileSmall = "multipart_small"
	applyIDFile        = "apply"
	SnapshotSign       = ".sign"
	metadataFile       = "meta"
	metadataFileTmp    = ".meta"

	smallFileFormatVersion = 1
	smallFileHeaderSize    = 32
	snapshotBlockSize      = 64 * 1024 * 1024
	snapshotLoadRoutineMax = 32
)

type SmallFileHeader struct {
	version   uint32
	blockSize uint32
	reserved1 uint64
	reserved2 uint64
	reserved3 uint64
}

func (hdr *SmallFileHeader) Marshal() (data []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, smallFileHeaderSize))
	if err = binary.Write(buff, binary.BigEndian, hdr.version); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, hdr.blockSize); err != nil {
		return
	}
	switch hdr.version {
	case 1:
		if err = binary.Write(buff, binary.BigEndian, hdr.reserved1); err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, hdr.reserved2); err != nil {
			return
		}
		if err = binary.Write(buff, binary.BigEndian, hdr.reserved3); err != nil {
			return
		}
	default:
		err = errors.NewErrorf("Invalid format version %v", hdr.version)
	}
	data = buff.Bytes()
	return
}

func (hdr *SmallFileHeader) Unmarshal(data []byte) (err error) {
	if len(data) != smallFileHeaderSize {
		err = errors.NewErrorf("Invalid Small File Header")
		return
	}
	buff := bytes.NewBuffer(data)
	if err = binary.Read(buff, binary.BigEndian, &hdr.version); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &hdr.blockSize); err != nil {
		return
	}
	return nil
}

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
	filename := path.Join(rootDir, inodeFileLarge)
	if _, err = os.Stat(filename); err != nil {
		err = nil
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
			log.LogInfof("loadDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
				mp.config.PartitionId, mp.config.VolName, numDentries)
		}
	}()
	filename := path.Join(rootDir, dentryFileLarge)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
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
		numDentries += 1
	}
}

func (mp *metaPartition) loadExtend(rootDir string) error {
	var err error
	filename := path.Join(rootDir, extendFileLarge)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
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
	for i := uint64(0); i < numExtends; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var extend *Extend
		if extend, err = NewExtendFromBytes(mem[offset : offset+int(numBytes)]); err != nil {
			return err
		}
		log.LogDebugf("loadExtend: new extend from bytes: partitionID（%v) volume(%v) inode(%v)",
			mp.config.PartitionId, mp.config.VolName, extend.inode)
		_ = mp.fsmSetXAttr(extend)
		offset += int(numBytes)
	}
	log.LogInfof("loadExtend: load complete: partitionID(%v) volume(%v) numExtends(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, numExtends, filename)
	return nil
}

func (mp *metaPartition) loadMultipart(rootDir string) error {
	var err error
	filename := path.Join(rootDir, multipartFileLarge)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
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
	var numMultiparts uint64
	numMultiparts, n = binary.Uvarint(mem)
	offset += n
	for i := uint64(0); i < numMultiparts; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var multipart *Multipart
		multipart = MultipartFromBytes(mem[offset : offset+int(numBytes)])
		log.LogDebugf("loadMultipart: create multipart from bytes: partitionID（%v) multipartID(%v)", mp.config.PartitionId, multipart.id)
		mp.fsmCreateMultipart(multipart)
		offset += int(numBytes)
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
	data, err := ioutil.ReadFile(filename)
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
	log.LogInfof("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex)
	return
}

func prepareSnapshotFiles(dir, small, large string) (fpSmall, fpLarge *os.File, err error) {
	fileLarge := path.Join(dir, large)
	fpLarge, err = os.OpenFile(fileLarge, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		log.LogErrorf("[prepareSnapshotFiles]: file %s OpenFile: %v", fileLarge, err)
		return
	}

	fileSmall := path.Join(dir, small)
	fpSmall, err = os.OpenFile(fileSmall, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		log.LogErrorf("[prepareSnapshotFiles]: file %s OpenFile: %v", fileSmall, err)
		return
	}

	return
}

func closeSnapshotFiles(fp1, fp2 *os.File) {
	if fp1 != nil {
		_ = fp1.Sync()
		// TODO Unhandled errors
		fp1.Close()
	}

	if fp2 != nil {
		_ = fp2.Sync()
		// TODO Unhandled errors
		fp2.Close()
	}
}

func storeSmallFileHeader(fp *os.File, crc hash.Hash32) (hdr *SmallFileHeader, err error) {
	var data []byte

	defer func() {
		if err != nil {
			log.LogErrorf("[storeSmallFileHeader]: file %v err %v", fp.Name(), err)
		}
	}()

	hdr = &SmallFileHeader{version: smallFileFormatVersion, blockSize: snapshotBlockSize}
	if data, err = hdr.Marshal(); err != nil {
		return
	}
	if len(data) != smallFileHeaderSize {
		err = errors.NewErrorf("Invalid header size %v", len(data))
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	if _, err = crc.Write(data); err != nil {
		return
	}
	return
}

func storeToSnapshot(fp *os.File, crc hash.Hash32, data []byte) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[storeToSnapshot]: file %s store: %v", fp.Name(), err)
		}
	}()

	lenBuf := make([]byte, 4)
	// set length
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err = fp.Write(lenBuf); err != nil {
		return
	}
	if _, err = crc.Write(lenBuf); err != nil {
		return
	}
	// set body
	if _, err = fp.Write(data); err != nil {
		return
	}
	if _, err = crc.Write(data); err != nil {
		return
	}
	return
}

func (mp *metaPartition) storeInode(rootDir string, sm *storeMsg) (crc [2]uint32, err error) {
	var (
		fpLarge  *os.File
		fpSmall  *os.File
		posSmall int64
		hdr      *SmallFileHeader
	)

	defer closeSnapshotFiles(fpSmall, fpLarge)

	if fpSmall, fpLarge, err = prepareSnapshotFiles(rootDir, inodeFileSmall, inodeFileLarge); err != nil {
		return
	}

	var data []byte
	signLarge := crc32.NewIEEE()
	signSmall := crc32.NewIEEE()
	if hdr, err = storeSmallFileHeader(fpSmall, signSmall); err != nil {
		return
	}
	posSmall += smallFileHeaderSize

	sm.inodeTree.Ascend(func(i BtreeItem) bool {
		ino := i.(*Inode)
		if data, err = ino.Marshal(); err != nil {
			return false
		}

		if 4+int64(len(data)) > int64(hdr.blockSize) {
			if err = storeToSnapshot(fpLarge, signLarge, data); err != nil {
				return false
			}
			return true
		}

		if (posSmall%int64(hdr.blockSize))+4+int64(len(data)) > int64(hdr.blockSize) {
			// round up to blockSize alignment
			posSmall = (posSmall + int64(hdr.blockSize) - 1) / int64(hdr.blockSize) * int64(hdr.blockSize)
			fpSmall.Seek(posSmall, os.SEEK_SET)
		}
		if err = storeToSnapshot(fpSmall, signSmall, data); err != nil {
			return false
		}

		posSmall += (4 + int64(len(data)))
		return true
	})
	crc[0] = signLarge.Sum32()
	crc[1] = signSmall.Sum32()

	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), crc)

	return
}

func (mp *metaPartition) storeDentry(rootDir string, sm *storeMsg) (crc [2]uint32, err error) {
	var (
		fpLarge  *os.File
		fpSmall  *os.File
		posSmall int64
		hdr      *SmallFileHeader
	)

	defer closeSnapshotFiles(fpSmall, fpLarge)

	if fpSmall, fpLarge, err = prepareSnapshotFiles(rootDir, dentryFileSmall, dentryFileLarge); err != nil {
		return
	}

	var data []byte
	signLarge := crc32.NewIEEE()
	signSmall := crc32.NewIEEE()
	if hdr, err = storeSmallFileHeader(fpSmall, signSmall); err != nil {
		return
	}
	posSmall += smallFileHeaderSize

	sm.dentryTree.Ascend(func(i BtreeItem) bool {
		dentry := i.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false
		}

		if 4+int64(len(data)) > int64(hdr.blockSize) {
			if err = storeToSnapshot(fpLarge, signLarge, data); err != nil {
				return false
			}
			return true
		}

		if (posSmall%int64(hdr.blockSize))+4+int64(len(data)) > int64(hdr.blockSize) {
			// round up to blockSize alignment
			posSmall = (posSmall + int64(hdr.blockSize) - 1) / int64(hdr.blockSize) * int64(hdr.blockSize)
			fpSmall.Seek(posSmall, os.SEEK_SET)
		}
		if err = storeToSnapshot(fpSmall, signSmall, data); err != nil {
			return false
		}

		posSmall += (4 + int64(len(data)))
		return true
	})
	crc[0] = signLarge.Sum32()
	crc[1] = signSmall.Sum32()

	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), crc)
	return
}

func storeToSnapshot2(fp *os.File, crc hash.Hash32, data []byte, length []byte) (err error) {
	// set length
	if _, err = fp.Write(length); err != nil {
		return
	}
	if _, err = crc.Write(length); err != nil {
		return
	}
	// set body
	if _, err = fp.Write(data); err != nil {
		return
	}
	if _, err = crc.Write(data); err != nil {
		return
	}
	return
}

func storeDummyNum(fp *os.File, crc hash.Hash32) (err error) {
	// write a dummy extend number to the fileLarge
	varintTmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintTmp, 0)
	if _, err = fp.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc.Write(varintTmp[:n]); err != nil {
		return
	}
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc [2]uint32, err error) {
	var (
		fpLarge  *os.File
		fpSmall  *os.File
		posSmall int64
		hdr      *SmallFileHeader
	)

	defer closeSnapshotFiles(fpSmall, fpLarge)

	if fpSmall, fpLarge, err = prepareSnapshotFiles(rootDir, extendFileSmall, extendFileLarge); err != nil {
		return
	}

	var data []byte
	signLarge := crc32.NewIEEE()
	signSmall := crc32.NewIEEE()

	if err = storeDummyNum(fpLarge, signLarge); err != nil {
		return
	}
	if hdr, err = storeSmallFileHeader(fpSmall, signSmall); err != nil {
		return
	}
	posSmall += smallFileHeaderSize

	varintTmp := make([]byte, binary.MaxVarintLen64)
	sm.extendTree.Ascend(func(i BtreeItem) bool {
		e := i.(*Extend)
		if data, err = e.Bytes(); err != nil {
			return false
		}

		n := binary.PutUvarint(varintTmp, uint64(len(data)))
		if int64(n)+int64(len(data)) > int64(hdr.blockSize) {
			if err = storeToSnapshot2(fpLarge, signLarge, data, varintTmp[:n]); err != nil {
				return false
			}
			return true
		}

		if (posSmall%int64(hdr.blockSize))+int64(n)+int64(len(data)) > int64(hdr.blockSize) {
			// round up to blockSize alignment
			posSmall = (posSmall + int64(hdr.blockSize) - 1) / int64(hdr.blockSize) * int64(hdr.blockSize)
			fpSmall.Seek(posSmall, os.SEEK_SET)
		}
		if err = storeToSnapshot2(fpSmall, signSmall, data, varintTmp[:n]); err != nil {
			return false
		}

		posSmall += (int64(n) + int64(len(data)))
		return true
	})
	if err != nil {
		return
	}

	crc[0] = signLarge.Sum32()
	crc[1] = signSmall.Sum32()

	log.LogInfof("storeExtend: store complete: partitoinID(%v) volume(%v) numExtends(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.extendTree.Len(), crc)

	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var multipartTree = sm.multipartTree
	var fp = path.Join(rootDir, multipartFileLarge)
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
