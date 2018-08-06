// Copyright 2018 The ChuBao Authors.
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

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/btree"
	"github.com/juju/errors"
)

const (
	inodeFile      = "inode"
	inodeFileTmp   = ".inode"
	dentryFile     = "dentry"
	dentryFileTmp  = ".dentry"
	metaFile       = "meta"
	metaFileTmp    = ".meta"
	applyIDFile    = "apply"
	applyIDFileTmp = ".apply"
)

// Load struct from meta
func (mp *metaPartition) loadMeta() (err error) {
	metaFile := path.Join(mp.config.RootDir, metaFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.Errorf("[loadMeta]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.Errorf("[loadMeta]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.Errorf("[loadMeta]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}
	// TODO: Valid PartitionConfig
	if mConf.checkMeta() != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.VolName = mConf.VolName
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	return
}

// Load inode info from inode snapshot file
func (mp *metaPartition) loadInode() (err error) {
	filename := path.Join(mp.config.RootDir, inodeFile)
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
		// First read length
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
		// Now Read Body
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
		mp.createInode(ino)
		mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
	}
}

// Load dentry from dentry snapshot file
func (mp *metaPartition) loadDentry() (err error) {
	filename := path.Join(mp.config.RootDir, dentryFile)
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
		// Now Read Body Data
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
		if mp.createDentry(dentry) != proto.OpOk {
			err = errors.Errorf("[loadDentry]->%s", err.Error())
			return
		}
	}
}

func (mp *metaPartition) loadApplyID() (err error) {
	filename := path.Join(mp.config.RootDir, applyIDFile)
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

// Store Meta to file
func (mp *metaPartition) storeMeta() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.Errorf("[storeMeta]->%s", err.Error())
		return
	}
	os.MkdirAll(mp.config.RootDir, 0755)
	filename := path.Join(mp.config.RootDir, metaFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
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
	err = os.Rename(filename, path.Join(mp.config.RootDir, metaFile))
	return
}

func (mp *metaPartition) storeApplyID(sm *storeMsg) (err error) {
	filename := path.Join(mp.config.RootDir, applyIDFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", sm.applyIndex)); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.RootDir, applyIDFile))
	return
}

func (mp *metaPartition) storeInode(sm *storeMsg) (err error) {
	filename := path.Join(mp.config.RootDir, inodeFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		if err != nil {
			os.RemoveAll(filename)
		}
	}()
	sm.inodeTree.Ascend(func(i btree.Item) bool {
		var data []byte
		lenBuf := make([]byte, 4)
		ino := i.(*Inode)
		if data, err = ino.Marshal(); err != nil {
			return false
		}
		// Set Length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		// Set Body Data
		if _, err = fp.Write(data); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.RootDir, inodeFile))
	return
}

func (mp *metaPartition) storeDentry(sm *storeMsg) (err error) {
	filename := path.Join(mp.config.RootDir, dentryFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	sm.dentryTree.Ascend(func(i btree.Item) bool {
		var data []byte
		lenBuf := make([]byte, 4)
		dentry := i.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false
		}
		// Set Length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = fp.Write(data); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}
	err = os.Rename(filename, path.Join(mp.config.RootDir, dentryFile))
	return
}

func (mp *metaPartition) deleteInodeFile() {
	filename := path.Join(mp.config.RootDir, inodeFile)
	os.Remove(filename)
}
func (mp *metaPartition) deleteDentryFile() {
	filename := path.Join(mp.config.RootDir, dentryFile)
	os.Remove(filename)

}
func (mp *metaPartition) deleteApplyFile() {
	filename := path.Join(mp.config.RootDir, applyIDFile)
	os.Remove(filename)
}
