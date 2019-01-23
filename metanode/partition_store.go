// Copyright 2018 The Container File System Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"hash/crc32"
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
		err = errors.Errorf("[loadMetadata]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.Errorf("[loadMetadata]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.Errorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
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
		err = errors.Errorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	lenBuf := make([]byte, 4)
	for {
		// first read length
		_, err = io.ReadFull(fp, lenBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.Errorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(lenBuf)

		// next read body
		buf := make([]byte, length)
		_, err = io.ReadFull(fp, buf)
		if err != nil {
			err = errors.Errorf("[loadInode] ReadBody: %s", err.Error())
			return
		}
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(buf); err != nil {
			err = errors.Errorf("[loadInode] Unmarshal: %s", err.Error())
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
		err = errors.Errorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	// TODO Unhandled errors
	defer fp.Close()
	lenBuf := make([]byte, 4)
	for {
		// First Read 4byte header length
		_, err = io.ReadFull(fp, lenBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.Errorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(lenBuf)

		// next read body
		buf := make([]byte, length)
		_, err = io.ReadFull(fp, buf)
		if err != nil {
			err = errors.Errorf("[loadDentry]: ReadBody: %s", err.Error())
			return
		}
		dentry := &Dentry{}
		if err = dentry.Unmarshal(buf); err != nil {
			err = errors.Errorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if mp.fsmCreateDentry(dentry) != proto.OpOk {
			err = errors.Errorf("[loadDentry]->%s", err.Error())
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
		err = errors.Errorf("[loadApplyID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.Errorf("[loadApplyID]: ApplyID is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &mp.applyID); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) persistMetadata() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.Errorf("[persistMetadata]->%s", err.Error())
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
		// TODO Unhandled errors
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", sm.applyIndex)); err != nil {
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
