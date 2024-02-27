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

package metanode

import (
	"os"
	"path"

	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) fsmDeleteExtentsFromTree(dbHandle interface{}, req *DeleteExtentsFromTreeRequest) (err error) {
	for _, dek := range req.DeletedKeys {
		log.LogDebugf("[fsmDeleteExtentsFromTree] mp(%v) delete ek(%v)", mp.config.PartitionId, dek)
		var ok bool
		ok, err = mp.deletedExtentsTree.Delete(dbHandle, dek)
		if log.EnableDebug() && !ok {
			log.LogDebugf("[fsmDeleteExtentsFromTree] mp(%v) not found ek(%v)", mp.config.PartitionId, dek)
		}
	}
	return
}

func (mp *metaPartition) fsmDeletedExtentMoveToTree(dbHandle interface{}) (err error) {
	files, err := mp.moveDelExtentToTree(dbHandle)
	if err != nil {
		log.LogErrorf("[fsmDeletedExtentMoveToTree] mp(%v) failed to move deleted extent to tree, err(%v)", mp.config.PartitionId, err)
		return
	}
	if log.EnableDebug() {
		for _, file := range files {
			log.LogDebugf("[fsmDeleteExtentFromTree] mp(%v) move file(%v) to tree", mp.config.PartitionId, file)
		}
	}
	log.LogDebugf("[fsmDeleteExtentFromTree] mp(%v) dek tree count(%v)", mp.config.PartitionId, mp.deletedExtentsTree.Count())
	// NOTE: we need to commit handle
	err = mp.deletedExtentsTree.CommitBatchWrite(dbHandle, true)
	if err != nil {
		return
	}
	err = mp.deletedExtentsTree.ClearBatchWriteHandle(dbHandle)
	if err != nil {
		return
	}
	// NOTE: remove all files
	for _, file := range files {
		filePath := path.Join(mp.config.RootDir, file)
		log.LogDebugf("[fsmDeletedExtentMoveToTree] mp(%v removing file(%v), path(%v)", mp.config.PartitionId, file, filePath)
		err = os.Remove(filePath)
		if err != nil && !os.IsNotExist(err) {
			log.LogErrorf("[fsmDeletedExtentMoveToTree] mp(%v) failed to rmove file(%v), path(%v), err(%v)", mp.config.PartitionId, file, filePath, err)
			return
		}
	}
	return
}

func (mp *metaPartition) fsmDeleteObjExtentsFromTree(dbHandle interface{}, req *DeleteObjExtentsFromTreeRequest) (err error) {
	for _, doek := range req.DeletedObjKeys {
		log.LogDebugf("[fsmDeleteObjExtentsFromTree] mp(%v) delete ek(%v)", mp.config.PartitionId, doek)
		var ok bool
		ok, err = mp.deletedObjExtentsTree.Delete(dbHandle, doek)
		if log.EnableDebug() && !ok {
			log.LogDebugf("[fsmDeleteObjExtentsFromTree] mp(%v) not found ek(%v)", mp.config.PartitionId, doek)
		}
	}
	return
}
