package metanode

import (
	"os"
	"strings"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
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
			if !mp.raftPartition.IsLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}
			// put first root inode
			resp, err := mp.Put(opCreateInode, data)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := &Packet{}
			p.ResultCode = resp.(uint8)
			log.LogDebugf("[initInode] raft sync: response status = %v.",
				p.GetResultMesg())
			return
		}
	}
}

func (mp *metaPartition) openFile(ino *Inode) (status uint8) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	item.(*Inode).AccessTime = ino.AccessTime
	status = proto.OpOk
	return
}

func (mp *metaPartition) offlinePartition() (err error) {
	return
}

func (mp *metaPartition) updatePartition(end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
			status = proto.OpDiskErr
		}
	}()
	err = mp.StoreMeta()
	return
}

func (mp *metaPartition) deletePartition() (status uint8) {
	mp.Stop()
	os.RemoveAll(mp.config.RootDir)
	return
}

func (mp *metaPartition) confAddNode(req *proto.
	MetaPartitionOfflineRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicatePort int
	)
	if heartbeatPort, replicatePort, err = mp.getRaftPort(); err != nil {
		return
	}

	findAddPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	updated = !findAddPeer
	if !updated {
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicatePort)
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.MetaPartitionOfflineRequest,
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
				if mp.raftPartition.AppliedIndex() < index {
					continue
				}
				if mp.raftPartition != nil {
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

func (mp *metaPartition) confUpdateNode(req *proto.MetaPartitionOfflineRequest,
	index uint64) (updated bool, err error) {
	return
}
