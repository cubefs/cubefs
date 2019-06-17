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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
)

const (
	snapshotDir     = "snapshot"
	snapshotDirTmp  = ".snapshot"
	snapshotBackup  = ".snapshot_backup"
	inodeFile       = "inode"
	dentryFile      = "dentry"
	applyIDFile     = "apply"
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
	return
}

func (mp *metaPartition) loadInode(rootDir string) (err error) {
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
	}
}

// Load dentry from the dentry snapshot.
func (mp *metaPartition) loadDentry(rootDir string) (err error) {
	filename := path.Join(rootDir, dentryFile)
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
			err = errors.NewErrorf("[loadDentry] createDentry dentry: %v, "+
				"resp code: %d", status)
			return
		}
	}
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
	err = os.Rename(filename, path.Join(mp.config.RootDir, metadataFile))
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
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.inodeTree.Ascend(func(i BtreeItem) bool {
		ino := i.(*Inode)
		if data, err = ino.Marshal(); err != nil {
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
		// set body
		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})
	crc = sign.Sum32()
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
	return
}

func (mp *metaPartition) deleteInodeFile() {
	filename := path.Join(mp.config.RootDir, inodeFile)
	// TODO Unhandled errors
	os.Remove(filename)
}
func (mp *metaPartition) deleteDentryFile() {
	filename := path.Join(mp.config.RootDir, dentryFile)
	// TODO Unhandled errors
	os.Remove(filename)

}
func (mp *metaPartition) deleteApplyFile() {
	filename := path.Join(mp.config.RootDir, applyIDFile)
	// TODO Unhandled errors
	os.Remove(filename)
}
