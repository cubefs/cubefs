package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"time"
)

const (
	AsyncDeleteInterval = 10 * time.Second
	UpdateVolTicket     = 5 * time.Minute
	BatchCounts         = 100
)

func (mp *metaPartition) startFreeList() {
	// start vol update ticket
	go mp.updateVolWorker()

	go mp.deleteWorker()
	go mp.checkFreelistWorker()
}

func (mp *metaPartition) updateVolWorker() {
	t := time.NewTimer(UpdateVolTicket)
	reqURL := fmt.Sprintf("%s?name=%s", DataPartitionViewUrl, mp.config.VolName)
	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
		}
		// Get dataPartitionView
		respBody, err := postToMaster("GET", reqURL, nil)
		if err != nil {
			log.LogErrorf("[updateVol] %s", err.Error())
			continue
		}
		dataView := new(DataPartitionsView)
		if err = json.Unmarshal(respBody, dataView); err != nil {
			log.LogErrorf("[updateVol] %s", err.Error())
			continue
		}
		mp.vol.UpdatePartitions(dataView)
		log.LogDebugf("[updateVol] %v", dataView)
	}
}

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
Begin:
	time.Sleep(AsyncDeleteInterval)
	for {
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); !isLeader {
			goto Begin
		}
		for idx = 0; idx < BatchCounts; idx++ {
			// batch get free inode from freeList
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		if len(buffSlice) == 0 {
			goto Begin
		}
		mp.deleteDataPartitionMark(buffSlice)
		if len(buffSlice) < BatchCounts {
			goto Begin
		}
	}
}

func (mp *metaPartition) checkFreelistWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
	for {
		time.Sleep(time.Second)
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); isLeader {
			continue
		}
		for idx = 0; idx < BatchCounts; idx++ {
			// get the first item of list
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			buffSlice = append(buffSlice, ino)

		}
		if len(buffSlice) == 0 {
			continue
		}
		for _, ino := range buffSlice {
			if mp.internalHasInode(ino) {
				mp.freeList.Push(ino)
			}
		}

	}
}

func (mp *metaPartition) deleteDataPartitionMark(inoSlice []*Inode) {
	stepFunc := func(ext proto.ExtentKey) (err error) {
		// get dataNode View
		dp := mp.vol.GetPartition(ext.PartitionId)
		if dp == nil {
			err = errors.Errorf("unknown dataPartitionID=%d in vol",
				ext.PartitionId)
			return
		}
		// delete dataNode
		conn, err := mp.config.ConnPool.Get(dp.Hosts[0])
		if err != nil {
			return
		}
		p := NewExtentDeletePacket(dp, ext.ExtentId)
		if err = p.WriteToConn(conn); err != nil {
			return
		}
		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			return
		}
		return
	}
	shouldCommit := make([]*Inode, 0, BatchCounts)
	for _, ino := range inoSlice {
		ino.Extents.Range(func(i int, v proto.ExtentKey) bool {
			if err := stepFunc(v); err != nil {
				mp.freeList.Push(ino)
				log.LogWarnf("[deleteDataPartitionMark]: %s", err.Error())
			}
			shouldCommit = append(shouldCommit, ino)
			return true
		})
	}
	if len(shouldCommit) > 0 {
		bufSlice := make([]byte, 0, 8*len(shouldCommit))
		for _, ino := range shouldCommit {
			bufSlice = append(bufSlice, ino.MarshalKey()...)
		}
		// raft Commit
		_, err := mp.Put(opFSMInternalDeleteInode, bufSlice)
		if err != nil {
			for _, ino := range shouldCommit {
				mp.freeList.Push(ino)
			}
		}
	}

}
