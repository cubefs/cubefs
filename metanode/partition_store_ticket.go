package metanode

import (
	"time"

	"github.com/juju/errors"
	"github.com/chubaoio/cbfs/util/btree"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/chubaoio/cbfs/util/ump"
)

type storeMsg struct {
	command    uint32
	applyIndex uint64
	inodeTree  *btree.BTree
	dentryTree *btree.BTree
}

func (mp *metaPartition) startSchedule(curIndex uint64) {
	timer := time.NewTimer(time.Hour * 24 * 365)
	timer.Stop()
	scheduleState := StateStopped
	dumpFunc := func(msg *storeMsg) {
		log.LogDebugf("[startSchedule] partitionId=%d: nowAppID"+
			"=%d, applyID=%d", mp.config.PartitionId, curIndex,
			msg.applyIndex)
		if err := mp.store(msg); err != nil {
			// retry
			mp.storeChan <- msg
			err = errors.Errorf(
				"[startSchedule]: dump partition id=%d: %v",
				mp.config.PartitionId, err.Error())
			log.LogErrorf(err.Error())
			ump.Alarm(UMPKey, err.Error())
		} else {
			curIndex = msg.applyIndex
		}
		// Truncate raft log
		mp.raftPartition.Truncate(curIndex)
		if _, ok := mp.IsLeader(); ok {
			timer.Reset(storeTimeTicker)
		}
		scheduleState = StateStopped
	}
	go func(stopC chan bool) {
		var msgs []*storeMsg
		readyChan := make(chan struct{}, 1)
		for {
			if len(msgs) > 0 {
				if scheduleState == StateStopped {
					scheduleState = StateRunning
					readyChan <- struct{}{}
				}
			}
			select {
			case <-stopC:
				timer.Stop()
				return

			case <-readyChan:
				idx := -1
				for i, msg := range msgs {
					if curIndex >= msg.applyIndex {
						continue
					}
					idx = i
				}
				if idx >= 0 {
					go dumpFunc(msgs[idx])
				}
				msgs = nil
			case msg := <-mp.storeChan:
				switch msg.command {
				case startStoreTick:
					timer.Reset(storeTimeTicker)
				case stopStoreTick:
					timer.Stop()
				case opStoreTick:
					msgs = append(msgs, msg)
				}
			case <-timer.C:
				if mp.applyID <= curIndex {
					timer.Reset(storeTimeTicker)
					continue
				}
				if _, err := mp.Put(opStoreTick, nil); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s",
						err.Error())
					if _, ok := mp.IsLeader(); ok {
						timer.Reset(storeTimeTicker)
					}
				}
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) stopSchedule() {
	if mp.stopC != nil {
		close(mp.stopC)
	}
}
