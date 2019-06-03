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
	"os"
	"strings"
	"time"

	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"path"
)

func (mp *metaPartition) initInode(ino *Inode) {
	for {
		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			// check first root inode
			if mp.hasInode(ino) {
				return
			}
			if !mp.raftPartition.IsRaftLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}
			// put first root inode
			resp, err := mp.Put(opFSMCreateInode, data)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := &Packet{}
			p.ResultCode = resp.(uint8)
			log.LogDebugf("[initInode] raft sync: response status = %v.",
				p.GetResultMsg())
			return
		}
	}
}

// Not implemented.
func (mp *metaPartition) decommissionPartition() (err error) {
	return
}

func (mp *metaPartition) fsmUpdatePartition(end uint64) (status uint8,
	err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
			status = proto.OpDiskErr
		}
	}()
	err = mp.PersistMetadata()
	return
}

func (mp *metaPartition) fsmDeletePartition() (status uint8) {
	mp.Stop()
	// TODO Unhandled errors
	os.RemoveAll(mp.config.RootDir)
	return
}

func (mp *metaPartition) confAddNode(req *proto.
	MetaPartitionDecommissionRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			addPeer = true
			break
		}
	}
	updated = !addPeer
	if !updated {
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.MetaPartitionDecommissionRequest,
	index uint64) (updated bool, err error) {
	peerIndex := -1
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		return
	}
	if req.RemovePeer.ID == mp.config.NodeId {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if mp.raftPartition != nil {
					if mp.raftPartition.AppliedIndex() < index {
						continue
					}
					mp.raftPartition.Delete()
				}
				mp.Stop()
				os.RemoveAll(mp.config.RootDir)
				log.LogDebugf("[confRemoveNode]: remove self end.")
				return
			}
		}(index)
		updated = false
		log.LogDebugf("[confRemoveNode]: begin remove self.")
		return
	}
	mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	log.LogDebugf("[confRemoveNode]: remove peer.")
	return
}

func (mp *metaPartition) confUpdateNode(req *proto.MetaPartitionDecommissionRequest,
	index uint64) (updated bool, err error) {
	return
}

func (mp *metaPartition) delOldExtentFile(buf []byte) (err error) {
	fileName := string(buf)
	infos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		return
	}
	for _, f := range infos {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), prefixDelExtent) {
			continue
		}
		if f.Name() <= fileName {
			// TODO Unhandled errors
			os.Remove(path.Join(mp.config.RootDir, f.Name()))
			continue
		}
		break
	}
	return
}

func (mp *metaPartition) setExtentDeleteFileCursor(buf []byte) (err error) {
	str := string(buf)
	var (
		fileName string
		cursor   int64
	)
	_, err = fmt.Sscanf(str, "%s %d", &fileName, &cursor)
	if err != nil {
		return
	}
	fp, err := os.OpenFile(path.Join(mp.config.RootDir, fileName), os.O_RDWR,
		0644)
	if err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] openFile %s failed: %s",
			fileName, err.Error())
		return
	}
	if err = binary.Write(fp, binary.BigEndian, cursor); err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] write file %s cursor"+
			" failed: %s", fileName, err.Error())
	}
	// TODO Unhandled errors
	fp.Close()
	return
}
